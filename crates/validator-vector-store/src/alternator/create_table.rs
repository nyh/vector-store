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

#[framed]
async fn create_table_with_vector_index(actors: TestActors) {
    info!("started");

    let (client, vs_clients) = make_clients(&actors).await;

    let table_name = unique_alternator_table_name();
    let index = IndexInfo::new(alternator_keyspace(&table_name).as_ref(), "v-idx");

    info!("Creating Alternator table '{table_name}' with VectorIndex");
    super::create_table_with_vector_index(&client, &table_name, index.index.as_ref()).await;
    info!("Created Alternator table '{table_name}' with VectorIndex");

    common::wait_for_index(&vs_clients[0], &index).await;

    client
        .delete_table()
        .table_name(&table_name)
        .send()
        .await
        .expect("DeleteTable should succeed");

    info!("finished");
}

#[framed]
pub(crate) async fn new() -> TestCase {
    TestCase::empty()
        .with_init(DEFAULT_TEST_TIMEOUT, super::init)
        .with_cleanup(DEFAULT_TEST_TIMEOUT, common::cleanup)
        .with_test(
            "create_table_with_vector_index",
            DEFAULT_TEST_TIMEOUT,
            create_table_with_vector_index,
        )
}
