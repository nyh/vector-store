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

use super::alternator_keyspace;
use super::make_clients;
use super::unique_alternator_table_name;

// ---------------------------------------------------------------------------
// Test: DeleteTable removes the vector index from Vector Store
// ---------------------------------------------------------------------------

/// Creates an Alternator table with a vector index, waits for the Vector Store
/// to serve it, then issues `DeleteTable` and confirms that the index
/// disappears from the Vector Store.
#[framed]
async fn delete_table_removes_vector_index(actors: TestActors) {
    info!("started");

    let (client, vs_clients) = make_clients(&actors).await;

    let table_name = unique_alternator_table_name();
    let index = IndexInfo::new(alternator_keyspace(&table_name).as_ref(), "v-idx");

    info!("Creating Alternator table '{table_name}' with VectorIndex");
    super::create_table_with_vector_index(&client, &table_name, index.index.as_ref()).await;

    info!(
        "Waiting for Vector Store to serve index '{}/{}'",
        index.keyspace, index.index
    );
    common::wait_for_index(&vs_clients[0], &index).await;

    info!("Deleting Alternator table '{table_name}'");
    client
        .delete_table()
        .table_name(&table_name)
        .send()
        .await
        .expect("DeleteTable should succeed");

    info!(
        "Waiting for Vector Store to drop index '{}/{}'",
        index.keyspace, index.index
    );
    super::wait_for_no_index(&vs_clients[0], &index).await;

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
            "delete_table_removes_vector_index",
            DEFAULT_TEST_TIMEOUT,
            delete_table_removes_vector_index,
        )
}
