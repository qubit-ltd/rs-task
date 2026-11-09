// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
#[cfg(feature = "sqlite")]
use std::env;
#[cfg(feature = "sqlite")]
use std::fs;

use qubit_id::Id;
use qubit_metadata::Metadata;
use qubit_model_id::ModelIdBuf;
use qubit_task::model::MAX_TASK_DIAGNOSTIC_MESSAGE_BYTES;
use qubit_task::model::ResourceRequest;
use qubit_task::model::StartCommand;
use qubit_task::model::StoredPayload;
use qubit_task::model::StoredTaskRequest;
use qubit_task::model::TaskId;
use qubit_task::model::TaskState;
use qubit_task::model::TransitionCommand;
use qubit_task::store::MemoryTaskStore;
#[cfg(feature = "sqlite")]
use qubit_task::store::SqliteTaskStore;
use qubit_task::store::TaskStore;
#[cfg(feature = "sqlite")]
use uuid::Uuid;

async fn check_oversized_cancel_error_is_rejected(store: &dyn TaskStore, id: TaskId) {
    let accepted = store
        .accept_encoded(
            id,
            StoredTaskRequest {
                kind_id: "cancel-error.bound".into(),
                category: None,
                payload: StoredPayload {
                    type_id: ModelIdBuf::parse("test.CancelErrorBound").expect("valid model ID"),
                    schema_version: 1,
                    codec_id: "qubit.bytes.json".into(),
                    bytes: vec![1],
                },
                metadata: Metadata::new(),
                resource_limit: ResourceRequest::default(),
                correlation_key: None,
                idempotency_key: None,
            },
        )
        .await
        .expect("task is accepted");
    let error = store
        .transition_encoded(TransitionCommand {
            id,
            expected_state_version: accepted.summary.state_version,
            expected_attempt: 0,
            retry_not_before_ms: None,
            state: TaskState::Queued,
            cancel_requested: false,
            cancel_error: Some("x".repeat(MAX_TASK_DIAGNOSTIC_MESSAGE_BYTES + 1)),
            finished_at_ms: None,
            output: None,
        })
        .await
        .expect_err("oversized cancel diagnostic is rejected");
    assert!(matches!(error, qubit_task::store::StoreError::InvalidRequest(_)));
    let current = store
        .get_encoded_task(id)
        .await
        .expect("task lookup succeeds")
        .expect("task remains retained")
        .summary;
    assert_eq!(current.state_version, accepted.summary.state_version);
    assert_eq!(current.state, TaskState::Queued);
    assert!(current.cancel_error.is_none());

    let running = store
        .start_encoded(StartCommand {
            id,
            expected_state_version: current.state_version,
            started_at_ms: 1,
        })
        .await
        .expect("task starts");
    let running_error = store
        .transition_encoded(TransitionCommand {
            id,
            expected_state_version: running.state_version,
            expected_attempt: running.attempt,
            retry_not_before_ms: None,
            state: TaskState::Running,
            cancel_requested: true,
            cancel_error: Some("x".repeat(MAX_TASK_DIAGNOSTIC_MESSAGE_BYTES + 1)),
            finished_at_ms: None,
            output: None,
        })
        .await
        .expect_err("oversized running cancellation diagnostic is rejected");
    assert!(matches!(
        running_error,
        qubit_task::store::StoreError::InvalidRequest(_)
    ));
    let current_running = store.get_encoded_task(id).await.unwrap().unwrap().summary;
    assert_eq!(current_running.state_version, running.state_version);
    assert_eq!(current_running.state, TaskState::Running);
    let cancelled = store
        .transition_encoded(TransitionCommand {
            id,
            expected_state_version: running.state_version,
            expected_attempt: running.attempt,
            retry_not_before_ms: None,
            state: TaskState::Cancelled,
            cancel_requested: false,
            cancel_error: None,
            finished_at_ms: Some(2),
            output: None,
        })
        .await
        .expect("task becomes terminal");
    let terminal_error = store
        .transition_encoded(TransitionCommand {
            id,
            expected_state_version: cancelled.state_version,
            expected_attempt: cancelled.attempt,
            retry_not_before_ms: None,
            state: TaskState::Cancelled,
            cancel_requested: true,
            cancel_error: Some("x".repeat(MAX_TASK_DIAGNOSTIC_MESSAGE_BYTES + 1)),
            finished_at_ms: None,
            output: None,
        })
        .await
        .expect_err("oversized terminal cancellation annotation is rejected");
    assert!(matches!(
        terminal_error,
        qubit_task::store::StoreError::InvalidRequest(_)
    ));
    let current_terminal = store.get_encoded_task(id).await.unwrap().unwrap().summary;
    assert_eq!(current_terminal.state_version, cancelled.state_version);
    assert_eq!(current_terminal.state, TaskState::Cancelled);
    let accepted_boundary = store
        .transition_encoded(TransitionCommand {
            id,
            expected_state_version: cancelled.state_version,
            expected_attempt: cancelled.attempt,
            retry_not_before_ms: None,
            state: TaskState::Cancelled,
            cancel_requested: true,
            cancel_error: Some("x".repeat(MAX_TASK_DIAGNOSTIC_MESSAGE_BYTES)),
            finished_at_ms: None,
            output: None,
        })
        .await
        .expect("exactly bounded terminal cancel diagnostic is accepted");
    assert_eq!(accepted_boundary.state_version, cancelled.state_version + 1);
    assert_eq!(
        accepted_boundary.cancel_error.as_deref().map(str::len),
        Some(MAX_TASK_DIAGNOSTIC_MESSAGE_BYTES)
    );
}

#[tokio::test]
async fn test_memory_store_rejects_oversized_cancel_error_without_transition() {
    let store = MemoryTaskStore::new(8);
    check_oversized_cancel_error_is_rejected(&store, TaskId::from_id(Id::new(103))).await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_sqlite_store_rejects_oversized_cancel_error_without_transition() {
    let path = env::temp_dir().join(format!("rs-task-cancel-error-{}.sqlite", Uuid::new_v4()));
    let store = SqliteTaskStore::open(&path).expect("SQLite store opens");
    check_oversized_cancel_error_is_rejected(&store, TaskId::from_id(Id::new(104))).await;
    drop(store);
    let mut owner_lock = path.as_os_str().to_owned();
    owner_lock.push(".owner.lock");
    for file in [
        path.clone(),
        owner_lock.into(),
        path.with_extension("sqlite-wal"),
        path.with_extension("sqlite-shm"),
    ] {
        let _ = fs::remove_file(file);
    }
}
