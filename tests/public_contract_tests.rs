// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0 (the "License");
// =============================================================================
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use qubit_task::engine::ExecutionHandle;
use qubit_task::handler::LocalTaskHandler;
use qubit_task::handler::TaskHandler;
use qubit_task::handler::TaskHandlerDescriptor;
#[cfg(feature = "sqlite")]
use qubit_task::model::TaskId;
use qubit_task::model::TaskOutput;
#[cfg(feature = "sqlite")]
use qubit_task::model::TaskRequest;

#[test]
fn execution_handle_cancellation_signal_is_a_shared_clone() {
    let cancellation = Arc::new(AtomicBool::new(false));
    let (sender, receiver) = tokio::sync::oneshot::channel::<Result<TaskOutput, _>>();
    let handle = ExecutionHandle::new(receiver, cancellation.clone());

    let signal = handle.cancellation_signal();
    assert!(!signal.load(std::sync::atomic::Ordering::SeqCst));

    cancellation.store(true, std::sync::atomic::Ordering::SeqCst);
    assert!(signal.load(std::sync::atomic::Ordering::SeqCst));

    drop(sender);
}

#[test]
fn local_task_handler_returns_its_declared_descriptor() {
    let descriptor = TaskHandlerDescriptor {
        task_type: "thumbnail".into(),
        version: "v3".into(),
    };
    let handler = LocalTaskHandler::new(descriptor.clone(), |_| Ok(TaskOutput::default()));

    assert_eq!(handler.descriptor(), descriptor);
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_find_idempotent_handles_missing_matching_and_conflicting_requests() {
    use qubit_task::store::SqliteTaskStore;
    use qubit_task::store::TaskStore;

    let path = std::env::temp_dir().join(format!(
        "qubit-task-find-idempotent-{}.sqlite",
        TaskId::generate()
    ));
    let store = SqliteTaskStore::open(&path).expect("SQLite store opens");

    let without_key = TaskRequest::new("thumbnail", "v3", b"source".to_vec());
    assert_eq!(store.find_idempotent(without_key).await.unwrap(), None);

    let mut request = TaskRequest::new("thumbnail", "v3", b"source".to_vec());
    request.idempotency_key = Some("thumbnail-source-42".into());
    assert_eq!(store.find_idempotent(request.clone()).await.unwrap(), None);

    let id = TaskId::generate();
    let accepted = store.accept(id, request.clone()).await.unwrap();
    let accepted_record = match accepted {
        qubit_task::model::AcceptOutcome::Accepted(record) => record,
        qubit_task::model::AcceptOutcome::Existing(_) => panic!("first request is newly accepted"),
    };
    assert_eq!(
        store.find_idempotent(request.clone()).await.unwrap(),
        Some(accepted_record)
    );

    let mut conflicting = request;
    conflicting.payload = b"different source".to_vec();
    assert!(matches!(
        store.find_idempotent(conflicting).await,
        Err(qubit_task::store::StoreError::IdempotencyConflict)
    ));

    drop(store);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("owner.lock"));
    let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
    let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
}
