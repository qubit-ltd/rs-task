// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
#![cfg(feature = "sqlite")]

mod support;

use std::sync::Arc;
use std::time::Duration;

use qubit_task::model::TaskId;
use qubit_task::model::TaskRequest;
use qubit_task::store::SqliteTaskStore;
use qubit_task::store::TaskStore;
use support::delayed_write_store::DelayedWriteStore;
use tokio::spawn;
use tokio::sync::oneshot;
use tokio::test as tokio_test;
use tokio::time;

#[tokio_test]
async fn test_release_owner_waits_for_previously_admitted_write() {
    let path = std::env::temp_dir().join(format!("qubit-task-owner-barrier-{}.sqlite", TaskId::generate()));
    let inner = SqliteTaskStore::open(&path).expect("SQLite provider opens");
    let (store, accept_started) = DelayedWriteStore::new(inner);
    let store = Arc::new(store);
    let epoch = store.acquire_owner().await.expect("owner lease acquired");

    let accept_store = Arc::clone(&store);
    let accepting = spawn(async move { accept_store.accept(TaskId::generate(), keyed_request()).await });
    accept_started.await.expect("write entered provider");

    let (release_started_sender, release_started_receiver) = oneshot::channel();
    let release_store = Arc::clone(&store);
    let mut releasing = spawn(async move {
        let _ = release_started_sender.send(());
        release_store.release_owner(epoch).await
    });
    release_started_receiver.await.expect("release began");
    assert!(
        time::timeout(Duration::from_millis(50), &mut releasing).await.is_err(),
        "owner release must wait for the earlier write"
    );

    store.release_accept();
    accepting.await.expect("accept worker joins").expect("write completes");
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
