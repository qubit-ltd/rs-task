// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
#[cfg(feature = "sqlite")]
mod common;
mod support;

#[cfg(feature = "sqlite")]
use common::sqlite_paths;
use qubit_task::store::MemoryTaskStore;
use tokio::test as tokio_test;

#[tokio_test]
async fn test_memory_store_obeys_core_contract() {
    let store = MemoryTaskStore::new(8);
    support::store_contract::check_core_contract(&store).await;
}

#[cfg(feature = "sqlite")]
#[tokio_test]
async fn test_sqlite_store_obeys_core_contract() {
    use qubit_task::store::SqliteTaskStore;

    let path = std::env::temp_dir().join(format!("rs-task-contract-{}.sqlite", uuid::Uuid::new_v4()));
    let store = SqliteTaskStore::open(&path).expect("sqlite store opens");
    support::store_contract::check_core_contract(&store).await;
    drop(store);
    for file in [
        &path,
        &sqlite_paths::owner_lock_path(&path),
        &path.with_extension("sqlite-wal"),
        &path.with_extension("sqlite-shm"),
    ] {
        let _ = std::fs::remove_file(file);
    }
}
