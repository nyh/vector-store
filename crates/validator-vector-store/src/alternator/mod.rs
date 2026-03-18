/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

pub mod create_table;
pub mod delete_table;
pub mod query;
pub mod update_table;

use async_backtrace::framed;
use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::config::Region;
use aws_smithy_runtime_api::box_error::BoxError;
use aws_smithy_runtime_api::client::interceptors::Intercept;
use aws_smithy_runtime_api::client::interceptors::context::BeforeTransmitInterceptorContextMut;
use aws_smithy_runtime_api::client::runtime_components::RuntimeComponents;
use aws_smithy_types::body::SdkBody;
use aws_smithy_types::config_bag::ConfigBag;
use http::HeaderValue;
use http::header::CONTENT_LENGTH;
use httpclient::HttpClient;
use serde_json::Map;
use serde_json::Value;
use std::net::Ipv4Addr;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tracing::info;
use vector_search_validator_tests::TestActors;
use vector_search_validator_tests::common;
use vector_store::IndexInfo;
use vector_store::KeyspaceName;

static TABLE_COUNTER: AtomicUsize = AtomicUsize::new(0);

pub(super) fn unique_alternator_table_name() -> String {
    format!("AltTbl{}", TABLE_COUNTER.fetch_add(1, Ordering::Relaxed))
}

pub const ALTERNATOR_PORT: u16 = 8000;

/// In ScyllaDB Alternator, a DynamoDB table named `T` is stored under the CQL
/// keyspace `alternator_T`. Vector Store discovers indexes by scanning
/// `system_schema.indexes`, so the keyspace name is what VS uses to identify
/// an Alternator-backed index.
pub fn alternator_keyspace(table_name: &str) -> KeyspaceName {
    format!("alternator_{table_name}").into()
}

/// A DynamoDB SDK interceptor that injects arbitrary key/value pairs into the
/// JSON request body before SigV4 signing.
///
/// The standard `aws-sdk-dynamodb` crate serialises requests without knowledge
/// of ScyllaDB Alternator extension fields such as `VectorIndexes`.  This
/// interceptor fires in [`modify_before_signing`], reads the already-serialised
/// JSON body, merges the provided fields, re-serialises, replaces the body, and
/// updates the `Content-Length` header so the SigV4 signature and HTTP transport
/// both operate on the correct byte count.
///
/// # Example
/// ```ignore
/// client
///     .create_table()
///     // ...
///     .customize()
///     .interceptor(JsonBodyInjectInterceptor::new([
///         ("VectorIndexes", vector_indexes_json),
///     ]))
///     .send()
///     .await?;
/// ```
///
/// [`modify_before_signing`]: Intercept::modify_before_signing
#[derive(Debug, Clone)]
pub struct JsonBodyInjectInterceptor {
    fields: Map<String, Value>,
}

impl JsonBodyInjectInterceptor {
    /// Creates a new interceptor that will inject the given `fields` into every
    /// outgoing request body.
    pub fn new(fields: impl IntoIterator<Item = (impl Into<String>, Value)>) -> Self {
        Self {
            fields: fields.into_iter().map(|(k, v)| (k.into(), v)).collect(),
        }
    }
}

impl Intercept for JsonBodyInjectInterceptor {
    fn name(&self) -> &'static str {
        "JsonBodyInjectInterceptor"
    }

    fn modify_before_signing(
        &self,
        context: &mut BeforeTransmitInterceptorContextMut<'_>,
        _runtime_components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let new_bytes = {
            let original = context
                .request()
                .body()
                .bytes()
                .ok_or("expected in-memory body for Alternator request")?
                .to_vec();

            let mut json: Value = serde_json::from_slice(&original)?;
            let obj = json
                .as_object_mut()
                .ok_or("expected JSON object body for Alternator request")?;
            for (key, value) in &self.fields {
                obj.insert(key.clone(), value.clone());
            }
            serde_json::to_vec(&json)?
        };

        let new_len = new_bytes.len();

        let request = context.request_mut();
        *request.body_mut() = SdkBody::from(new_bytes);
        request.headers_mut().insert(
            CONTENT_LENGTH,
            HeaderValue::from_str(&new_len.to_string()).expect("content-length value is valid"),
        );

        Ok(())
    }
}

/// Builds a DynamoDB client pointing at the ScyllaDB Alternator endpoint on
/// `db_ip`.
///
/// Dummy AWS credentials are used because authorization is disabled in tests
/// via `--alternator-enforce-authorization=false`.
pub(super) async fn make_dynamodb_client(db_ip: Ipv4Addr) -> Client {
    let creds = Credentials::new("any", "any", None, None, "test");
    let config = aws_config::defaults(BehaviorVersion::latest())
        .credentials_provider(creds)
        .endpoint_url(format!("http://{db_ip}:{ALTERNATOR_PORT}"))
        .region(Region::new("us-east-1"))
        .load()
        .await;
    Client::new(&config)
}

/// Polls the Alternator HTTP endpoint on `db_ip` until it responds successfully.
///
/// The Alternator port may become available slightly after the CQL port (which
/// is what `db.wait_for_ready()` checks), so tests should call this before
/// issuing their first DynamoDB request.
pub(super) async fn wait_for_alternator(db_ip: Ipv4Addr, client: &Client) {
    common::wait_for(
        || {
            let c = client.clone();
            async move { c.list_tables().limit(1).send().await.is_ok() }
        },
        format!("Alternator endpoint at http://{db_ip}:{ALTERNATOR_PORT} to be ready"),
        common::DEFAULT_TEST_TIMEOUT,
    )
    .await;
}

/// Creates a DynamoDB client pointing at the first DB node and VS HTTP clients.
/// Also waits for the Alternator endpoint to be ready before returning.
pub async fn make_clients(actors: &TestActors) -> (Client, Vec<HttpClient>) {
    let db_ip = actors.services_subnet.ip(common::DB_OCTET_1);
    let dynamodb_client = make_dynamodb_client(db_ip).await;
    wait_for_alternator(db_ip, &dynamodb_client).await;
    let vs_clients = common::get_default_vs_ips(actors)
        .into_iter()
        .map(|ip| HttpClient::new((ip, common::VS_PORT).into()))
        .collect();
    (dynamodb_client, vs_clients)
}

/// Polls the VS HTTP endpoint until the given index is no longer visible
/// (i.e. `index_status` returns an error).  Used to confirm that a delete
/// action (via `UpdateTable` or `DeleteTable`) has been processed and
/// propagated to the Vector Store.
pub(super) async fn wait_for_no_index(client: &HttpClient, index: &IndexInfo) {
    common::wait_for(
        || async {
            client
                .index_status(&index.keyspace, &index.index)
                .await
                .is_err()
        },
        format!(
            "index '{}/{}' to be gone at {}",
            index.keyspace,
            index.index,
            client.url()
        ),
        Duration::from_secs(60),
    )
    .await;
}

/// Creates an Alternator table with a single `pk` (HASH, STRING) key and a
/// `VectorIndex` on the `v` attribute (3 dimensions).
pub(super) async fn create_table_with_vector_index(
    client: &Client,
    table_name: &str,
    index_name: &str,
) {
    let vector_indexes = serde_json::json!([
        {
            "IndexName": index_name,
            "VectorAttribute": {
                "AttributeName": "v",
                "Dimensions": 3
            }
        }
    ]);

    client
        .create_table()
        .table_name(table_name)
        .attribute_definitions(
            aws_sdk_dynamodb::types::AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(aws_sdk_dynamodb::types::ScalarAttributeType::S)
                .build()
                .expect("failed to build AttributeDefinition"),
        )
        .key_schema(
            aws_sdk_dynamodb::types::KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(aws_sdk_dynamodb::types::KeyType::Hash)
                .build()
                .expect("failed to build KeySchemaElement"),
        )
        .billing_mode(aws_sdk_dynamodb::types::BillingMode::PayPerRequest)
        .customize()
        .interceptor(JsonBodyInjectInterceptor::new([(
            "VectorIndexes",
            vector_indexes,
        )]))
        .send()
        .await
        .expect("CreateTable with VectorIndex should succeed");
}

/// Standard test init: starts ScyllaDB with the Alternator endpoint enabled on
/// each node's own IP, alongside the Vector Store.
#[framed]
pub async fn init(actors: TestActors) {
    info!("started");

    let mut scylla_configs = common::get_default_scylla_node_configs(&actors).await;

    for config in &mut scylla_configs {
        let node_ip = config.db_ip;
        config.args.extend([
            format!("--alternator-port={ALTERNATOR_PORT}"),
            format!("--alternator-address={node_ip}"),
            "--alternator-write-isolation=only_rmw_uses_lwt".to_string(),
            "--alternator-enforce-authorization=false".to_string(),
        ]);
    }

    let vs_configs = common::get_default_vs_node_configs(&actors).await;
    common::init_with_config(actors, scylla_configs, vs_configs).await;

    info!("finished");
}
