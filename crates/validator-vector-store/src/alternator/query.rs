/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use async_backtrace::framed;
use aws_sdk_dynamodb::types::AttributeValue;
use tracing::info;
use vector_search_validator_tests::TestActors;
use vector_search_validator_tests::TestCase;
use vector_search_validator_tests::common;
use vector_search_validator_tests::common::DEFAULT_TEST_TIMEOUT;
use vector_store::IndexInfo;

use super::JsonBodyInjectInterceptor;
use super::alternator_keyspace;
use super::make_clients;
use super::unique_alternator_table_name;

/// Converts a sequence of `f32` values into a DynamoDB `L` (list of numbers)
/// `AttributeValue`.
///
/// The AWS SDK for Rust requires explicit `AttributeValue` construction for
/// each field — there is no built-in conversion from native Rust types.
/// (`serde_dynamo` is the standard third-party crate for that, but is not a
/// dependency here.)  This helper encapsulates the repetitive boilerplate for
/// float vector attributes.
fn dynamo_float_list(values: impl IntoIterator<Item = f32>) -> AttributeValue {
    AttributeValue::L(
        values
            .into_iter()
            .map(|x| AttributeValue::N(x.to_string()))
            .collect(),
    )
}

/// Inserts an item with the given `pk` (S) and `v` (L of N) into an Alternator table.
async fn put_item(client: &aws_sdk_dynamodb::Client, table_name: &str, pk: &str, v: [f32; 3]) {
    client
        .put_item()
        .table_name(table_name)
        .item("pk", AttributeValue::S(pk.to_string()))
        .item("v", dynamo_float_list(v))
        .send()
        .await
        .expect("PutItem should succeed");
}

// ---------------------------------------------------------------------------
// Test: Query with VectorSearch works and respects Limit
// ---------------------------------------------------------------------------

/// Creates an Alternator table with a vector index, inserts 5 items, waits for
/// the index to be SERVING, then issues a `Query` extended with the Alternator
/// `VectorSearch` attribute (no `KeyConditionExpression`) and `Limit=2`.
/// Asserts that at least one result is returned and that the limit is respected.
#[framed]
async fn query_with_vector_search(actors: TestActors) {
    info!("started");

    let (client, vs_clients) = make_clients(&actors).await;

    let table_name = unique_alternator_table_name();
    let index = IndexInfo::new(alternator_keyspace(&table_name).as_ref(), "v-idx");

    info!("Creating Alternator table '{table_name}' with VectorIndex");
    super::create_table_with_vector_index(&client, &table_name, index.index.as_ref()).await;

    info!("Inserting items into '{table_name}'");
    for i in 0..5u8 {
        put_item(
            &client,
            &table_name,
            &format!("pk-{i}"),
            [i as f32, (i + 1) as f32, (i + 2) as f32],
        )
        .await;
    }

    info!(
        "Waiting for Vector Store to serve index '{}/{}'",
        index.keyspace, index.index
    );
    common::wait_for_index(&vs_clients[0], &index).await;

    let vector_search = serde_json::json!({
        "QueryVector": {
            "L": [{"N": "1"}, {"N": "2"}, {"N": "3"}]
        }
    });

    info!("Issuing Query with VectorSearch Limit=2 on '{table_name}'");
    let resp = client
        .query()
        .table_name(&table_name)
        .limit(2)
        .customize()
        .interceptor(JsonBodyInjectInterceptor::new([(
            "VectorSearch",
            vector_search,
        )]))
        .send()
        .await
        .expect("Query with VectorSearch should succeed");

    let items = resp.items();
    assert!(
        !items.is_empty(),
        "Query with VectorSearch should return at least one item"
    );
    assert!(
        items.len() <= 2,
        "Query with VectorSearch Limit=2 should return at most 2 items, got {}",
        items.len()
    );
    info!("Query returned {} item(s) (limit was 2)", items.len());

    client
        .delete_table()
        .table_name(&table_name)
        .send()
        .await
        .expect("DeleteTable should succeed");

    info!("finished");
}

// ---------------------------------------------------------------------------
// Test-case registration
// ---------------------------------------------------------------------------

#[framed]
pub(crate) async fn new() -> TestCase {
    TestCase::empty()
        .with_init(DEFAULT_TEST_TIMEOUT, super::init)
        .with_cleanup(DEFAULT_TEST_TIMEOUT, common::cleanup)
        .with_test(
            "query_with_vector_search",
            DEFAULT_TEST_TIMEOUT,
            query_with_vector_search,
        )
}
