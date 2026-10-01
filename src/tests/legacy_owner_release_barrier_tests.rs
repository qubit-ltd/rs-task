// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
#![cfg(feature = "sqlite")]

use std::sync::Arc;
use std::time::Duration;

use tokio::spawn;
use tokio::test as tokio_test;
use tokio::time;

use crate::model::AcceptOutcome;
use crate::model::TaskId;
use crate::model::TaskRequest;
use crate::store::LegacyTaskStore;
use crate::store::SqliteTaskStore;
use crate::tests::delayed_write_store_fixture::DelayedWriteStore;

#[tokio_test]
async fn test_release_owner_waits_for_previously_admitted_write() {
    let path = std::env::temp_dir().join(format!("qubit-task-owner-barrier-{}.sqlite", TaskId::generate()));
    let inner = SqliteTaskStore::open(&path).expect("SQLite provider opens");
    let (store, accept_started, release_waiting) = DelayedWriteStore::new(inner);
    let store = Arc::new(store);
    let epoch = store.acquire_owner().await.expect("owner lease acquired");

    let accept_store = Arc::clone(&store);
    let accepting = spawn(async move { accept_store.accept(TaskId::generate(), keyed_request()).await });
    accept_started.await.expect("write entered provider");

    let release_store = Arc::clone(&store);
    let releasing = spawn(async move { release_store.release_owner(epoch).await });
    assert!(
        time::timeout(Duration::from_secs(2), release_waiting)
            .await
            .expect("owner release reaches the active-write barrier")
            .is_ok(),
        "owner release signals that it is waiting for the earlier write"
    );
    assert!(
        !releasing.is_finished(),
        "release remains pending while a write is active"
    );

    store.release_accept();
    let accepted = accepting.await.expect("accept worker joins").expect("write completes");
    assert!(matches!(accepted, AcceptOutcome::Accepted(_)));
    releasing
        .await
        .expect("release worker joins")
        .expect("owner released after write");

    drop(store);
    remove_database(&path);
}

fn keyed_request() -> TaskRequest {
    let mut request = TaskRequest::new("owner-barrier-test", "1", Vec::new());
    request.idempotency_key = Some("owner-barrier-test-key".into());
    request
}

fn remove_database(path: &std::path::Path) {
    let _ = std::fs::remove_file(path);
    let mut lock_path = path.as_os_str().to_owned();
    lock_path.push(".owner.lock");
    let _ = std::fs::remove_file(std::path::PathBuf::from(lock_path));
    let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
    let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
}
