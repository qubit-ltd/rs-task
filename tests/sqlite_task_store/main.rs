// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
#![cfg(feature = "sqlite")]

use std::num::NonZeroUsize;
use std::path::Path;
use std::path::PathBuf;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use qubit_task::store::SqliteTaskStore;
use qubit_task::store::StoreError;
use qubit_task::store::TaskStore;
use tokio::test as tokio_test;

/// Creates a unique disposable database for one SQLite store test.
fn test_database_path() -> PathBuf {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock is after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "qubit-task-sqlite-prune-{}-{timestamp}.sqlite",
        std::process::id()
    ))
}

/// Removes only database files created by this test.
fn remove_database(path: &Path) {
    let mut lock_path = path.as_os_str().to_owned();
    lock_path.push(".owner.lock");
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(PathBuf::from(lock_path));
    let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
    let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
}

#[tokio_test]
async fn test_pruning_rejects_values_outside_sqlite_integer_range() {
    let path = test_database_path();
    let store = SqliteTaskStore::open(&path).expect("SQLite store opens");
    let cutoff = store
        .prune_terminal_before(u64::MAX, NonZeroUsize::new(1).expect("positive limit"))
        .await;
    assert!(
        matches!(cutoff, Err(StoreError::InvalidRequest(_))),
        "out-of-range cutoff should be rejected as an invalid request"
    );
    let limit = store
        .prune_terminal_before(0, NonZeroUsize::new(i64::MAX as usize + 1).expect("positive limit"))
        .await;
    assert!(
        matches!(limit, Err(StoreError::InvalidRequest(_))),
        "limit outside SQLite's integer range should be rejected as an invalid request"
    );
    drop(store);
    remove_database(&path);
}
