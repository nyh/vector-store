/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use async_backtrace::framed;
use tracing::info;
use vector_search_validator_tests::TestActors;
use vector_search_validator_tests::TestCase;
use vector_search_validator_tests::common;
use vector_search_validator_tests::common::DEFAULT_TEST_TIMEOUT;
use vector_store::IndexInfo;
use vector_store::IndexName;
use vector_store::KeyspaceName;

use super::JsonBodyInjectInterceptor;
use super::alternator_keyspace;
use super::make_clients;
use super::unique_alternator_table_name;

/// Builds the standard single-partition-key table without any vector index.
async fn create_plain_table(client: &aws_sdk_dynamodb::Client, table_name: &str) {
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
        .send()
        .await
        .expect("CreateTable should succeed");
}

// ---------------------------------------------------------------------------
// Test: create a vector index via UpdateTable
// ---------------------------------------------------------------------------

/// Creates a plain Alternator table (no vector index), then issues an
/// `UpdateTable` with `VectorIndexUpdates[{Create: ...}]` and waits for the
/// Vector Store to start serving the newly created index.
#[framed]
async fn create_vector_index_via_update_table(actors: TestActors) {
    info!("started");

    let (client, vs_clients) = make_clients(&actors).await;

    let table_name = unique_alternator_table_name();
    let index = IndexInfo::new(alternator_keyspace(&table_name).as_ref(), "v-idx");

    info!("Creating plain Alternator table '{table_name}' (no vector index)");
    create_plain_table(&client, &table_name).await;

    let vector_index_updates = serde_json::json!([
        {
            "Create": {
                "IndexName": index.index,
                "VectorAttribute": {
                    "AttributeName": "v",
                    "Dimensions": 3
                }
            }
        }
    ]);

    info!(
        "Issuing UpdateTable for '{table_name}' to add vector index '{}'",
        index.index
    );

    client
        .update_table()
        .table_name(&table_name)
        .customize()
        .interceptor(JsonBodyInjectInterceptor::new([(
            "VectorIndexUpdates",
            vector_index_updates,
        )]))
        .send()
        .await
        .expect("UpdateTable with VectorIndexUpdates Create should succeed");

    info!(
        "Waiting for Vector Store to serve index '{}/{}'",
        index.keyspace, index.index
    );
    common::wait_for_index(&vs_clients[0], &index).await;

    client
        .delete_table()
        .table_name(&table_name)
        .send()
        .await
        .expect("DeleteTable should succeed");

    info!("finished");
}

// ---------------------------------------------------------------------------
// Test: delete a vector index via UpdateTable
// ---------------------------------------------------------------------------

/// Creates an Alternator table that already has a vector index, waits for the
/// Vector Store to serve it, then issues `UpdateTable` with
/// `VectorIndexUpdates[{Delete: ...}]` and confirms the index disappears.
#[framed]
async fn delete_vector_index_via_update_table(actors: TestActors) {
    info!("started");

    let (client, vs_clients) = make_clients(&actors).await;

    let table_name = unique_alternator_table_name();
    let index = IndexInfo::new(alternator_keyspace(&table_name).as_ref(), "v-idx");

    let vector_indexes = serde_json::json!([
        {
            "IndexName": index.index,
            "VectorAttribute": {
                "AttributeName": "v",
                "Dimensions": 3
            }
        }
    ]);

    info!("Creating Alternator table '{table_name}' with VectorIndex");

    client
        .create_table()
        .table_name(&table_name)
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

    info!(
        "Waiting for Vector Store to serve index '{}/{}'",
        index.keyspace, index.index
    );
    common::wait_for_index(&vs_clients[0], &index).await;

    let vector_index_updates = serde_json::json!([
        {
            "Delete": {
                "IndexName": index.index
            }
        }
    ]);

    info!(
        "Issuing UpdateTable for '{table_name}' to delete vector index '{}'",
        index.index
    );

    client
        .update_table()
        .table_name(&table_name)
        .customize()
        .interceptor(JsonBodyInjectInterceptor::new([(
            "VectorIndexUpdates",
            vector_index_updates,
        )]))
        .send()
        .await
        .expect("UpdateTable with VectorIndexUpdates Delete should succeed");

    info!(
        "Waiting for Vector Store to drop index '{}/{}'",
        index.keyspace, index.index
    );
    super::wait_for_no_index(&vs_clients[0], &index).await;

    client
        .delete_table()
        .table_name(&table_name)
        .send()
        .await
        .expect("DeleteTable should succeed");

    info!("finished");
}

// ---------------------------------------------------------------------------
// Test: replace (swap) a vector index via UpdateTable
// ---------------------------------------------------------------------------

/// Creates an Alternator table with a first vector index, waits for the Vector
/// Store to serve it, then issues a single `UpdateTable` that simultaneously
/// creates a second index and deletes the first one.  Both the appearance of
/// the new index and the disappearance of the old index are verified.
#[framed]
async fn replace_vector_index_via_update_table(actors: TestActors) {
    info!("started");

    let (client, vs_clients) = make_clients(&actors).await;

    let table_name = unique_alternator_table_name();
    let keyspace: KeyspaceName = alternator_keyspace(&table_name);
    let index_a_name: IndexName = "v-idx-a".into();
    let index_b_name: IndexName = "v-idx-b".into();
    let index_a = IndexInfo::new(keyspace.as_ref(), index_a_name.as_ref());
    let index_b = IndexInfo::new(keyspace.as_ref(), index_b_name.as_ref());

    let vector_indexes = serde_json::json!([
        {
            "IndexName": index_a_name,
            "VectorAttribute": {
                "AttributeName": "v",
                "Dimensions": 3
            }
        }
    ]);

    info!("Creating Alternator table '{table_name}' with first vector index '{index_a_name}'");

    client
        .create_table()
        .table_name(&table_name)
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
        .expect("CreateTable with first VectorIndex should succeed");

    info!(
        "Waiting for Vector Store to serve first index '{}/{}'",
        index_a.keyspace, index_a.index
    );
    common::wait_for_index(&vs_clients[0], &index_a).await;

    let vector_index_updates = serde_json::json!([
        {
            "Create": {
                "IndexName": index_b_name,
                "VectorAttribute": {
                    "AttributeName": "v",
                    "Dimensions": 3
                }
            }
        },
        {
            "Delete": {
                "IndexName": index_a_name
            }
        }
    ]);

    info!(
        "Issuing UpdateTable for '{table_name}': create '{index_b_name}', delete '{index_a_name}'"
    );

    client
        .update_table()
        .table_name(&table_name)
        .customize()
        .interceptor(JsonBodyInjectInterceptor::new([(
            "VectorIndexUpdates",
            vector_index_updates,
        )]))
        .send()
        .await
        .expect("UpdateTable with simultaneous Create+Delete VectorIndexUpdates should succeed");

    info!(
        "Waiting for Vector Store to serve new index '{}/{}' and drop old index '{}/{}'",
        index_b.keyspace, index_b.index, index_a.keyspace, index_a.index
    );
    common::wait_for_index(&vs_clients[0], &index_b).await;
    super::wait_for_no_index(&vs_clients[0], &index_a).await;

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
            "create_vector_index_via_update_table",
            DEFAULT_TEST_TIMEOUT,
            create_vector_index_via_update_table,
        )
        .with_test(
            "delete_vector_index_via_update_table",
            DEFAULT_TEST_TIMEOUT,
            delete_vector_index_via_update_table,
        )
        .with_test(
            "replace_vector_index_via_update_table",
            DEFAULT_TEST_TIMEOUT,
            replace_vector_index_via_update_table,
        )
}
