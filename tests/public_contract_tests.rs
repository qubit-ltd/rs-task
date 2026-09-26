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
use qubit_task::engine::ExecutionOutcome;
use qubit_task::handler::LocalTaskHandler;
use qubit_task::handler::TaskHandler;
use qubit_task::handler::TaskHandlerDescriptor;
use qubit_task::handler::TaskRunOutcome;
#[cfg(feature = "sqlite")]
use qubit_task::model::TaskId;
use qubit_task::model::TaskOutput;
use qubit_task::model::TaskRequest;

#[test]
fn execution_handle_cancellation_signal_is_a_shared_clone() {
    let cancellation = Arc::new(AtomicBool::new(false));
    let (sender, receiver) = tokio::sync::oneshot::channel::<ExecutionOutcome>();
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
    let handler = LocalTaskHandler::new(descriptor.clone(), |_| {
        Ok(TaskRunOutcome::Succeeded(TaskOutput::default()))
    });

    assert_eq!(handler.descriptor(), descriptor);
}

#[tokio::test]
async fn local_task_handler_rejects_a_second_run() {
    let descriptor = TaskHandlerDescriptor {
        task_type: "one-shot".into(),
        version: "1".into(),
    };
    let handler = Arc::new(LocalTaskHandler::new(descriptor, |_| {
        Ok(TaskRunOutcome::Succeeded(TaskOutput::default()))
    }));
    let service = qubit_task::TaskExecutionServiceBuilder::in_memory()
        .register_handler(handler)
        .expect("handler registers")
        .build()
        .await
        .expect("service builds");
    let request = || TaskRequest::new("one-shot", "1", Vec::new());
    let first = service.submit(test_keyed(request())).await.expect("first task submits");
    let second = service
        .submit(test_keyed(request()))
        .await
        .expect("second task submits");
    let first = service.wait(first.id).await.expect("first task finishes");
    let second = service.wait(second.id).await.expect("second task finishes");
    let states = [first.state, second.state];

    assert_eq!(
        states
            .iter()
            .filter(|state| matches!(state, qubit_task::model::TaskState::Succeeded))
            .count(),
        1
    );
    assert!(states.iter().any(|state| matches!(
        state,
        qubit_task::model::TaskState::Failed { category, message }
            if category == "local_handler" && message.contains("ran more than once")
    )));
    service.shutdown().await.expect("service shuts down");
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_get_by_idempotency_key_returns_the_record_for_a_key() {
    use qubit_task::store::SqliteTaskStore;
    use qubit_task::store::TaskStore;

    let path = std::env::temp_dir().join(format!("qubit-task-find-idempotent-{}.sqlite", TaskId::generate()));
    let store = SqliteTaskStore::open(&path).expect("SQLite store opens");

    assert_eq!(store.get_by_idempotency_key("thumbnail-source-42").await.unwrap(), None);
    let mut request = TaskRequest::new("thumbnail", "v3", b"source".to_vec());
    request.idempotency_key = Some("thumbnail-source-42".into());
    assert_eq!(store.get_by_idempotency_key("thumbnail-source-42").await.unwrap(), None);

    let id = TaskId::generate();
    let accepted = store.accept(id, request.clone()).await.unwrap();
    let accepted_record = match accepted {
        qubit_task::model::AcceptOutcome::Accepted(record) => record,
        qubit_task::model::AcceptOutcome::Existing(_) => panic!("first request is newly accepted"),
    };
    assert_eq!(
        store.get_by_idempotency_key("thumbnail-source-42").await.unwrap(),
        Some(accepted_record)
    );

    let mut conflicting = request;
    conflicting.payload = b"different source".to_vec();
    assert!(matches!(
        store.accept(TaskId::generate(), conflicting).await,
        Err(qubit_task::store::StoreError::IdempotencyConflict)
    ));

    drop(store);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("owner.lock"));
    let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
    let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_accept_rejects_invalid_requests_before_queueing_a_write() {
    use qubit_task::store::SqliteTaskStore;
    use qubit_task::store::TaskStore;

    let path = std::env::temp_dir().join(format!("qubit-task-invalid-request-{}.sqlite", TaskId::generate()));
    let store = SqliteTaskStore::open(&path).expect("SQLite store opens");
    let request = TaskRequest::new("", "v1", Vec::new());

    assert!(matches!(
        store.accept(TaskId::generate(), request).await,
        Err(qubit_task::store::StoreError::InvalidRequest(_))
    ));
    drop(store);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("owner.lock"));
}

#[cfg(feature = "sqlite")]
#[test]
fn sqlite_open_reports_a_non_directory_parent() {
    use qubit_task::store::SqliteTaskStore;

    let parent = std::env::temp_dir().join(format!("qubit-task-not-directory-{}", TaskId::generate()));
    std::fs::write(&parent, b"not a directory").expect("parent fixture is created");
    let result = SqliteTaskStore::open(parent.join("tasks.sqlite"));

    assert!(matches!(result, Err(qubit_task::store::StoreError::Failure(_))));
    std::fs::remove_file(parent).expect("parent fixture is removed");
}

#[allow(dead_code)]
fn test_keyed(mut request: qubit_task::model::TaskRequest) -> qubit_task::model::TaskRequest {
    static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(1);
    if request.idempotency_key.is_none() {
        request.idempotency_key = Some(format!(
            "test-request-{}",
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
    }
    request
}
