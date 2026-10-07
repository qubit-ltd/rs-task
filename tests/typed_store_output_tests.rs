// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use qubit_task::model::AcceptOutcome;
use qubit_task::model::MAX_TASK_OUTPUT_SUMMARY_BYTES;
use qubit_task::model::ResourceRequest;
use qubit_task::model::StartCommand;
use qubit_task::model::StoredPayload;
use qubit_task::model::StoredTaskRequest;
use qubit_task::model::TaskId;
use qubit_task::model::TaskOutput;
use qubit_task::model::TaskState;
use qubit_task::model::TransitionCommand;
use qubit_task::store::MemoryTaskStore;
#[cfg(feature = "sqlite")]
use qubit_task::store::SqliteTaskStore;
use qubit_task::store::StoreError;
use qubit_task::store::TaskStore;

fn request() -> StoredTaskRequest {
    StoredTaskRequest {
        kind_id: "output.test".to_owned(),
        category: None,
        payload: StoredPayload {
            type_id: qubit_model_id::ModelIdBuf::parse("qubit_task.tests.Payload")
                .expect("valid model ID"),
            schema_version: 1,
            codec_id: "qubit.bytes.json".to_owned(),
            bytes: vec![1],
        },
        metadata: qubit_metadata::Metadata::new(),
        resource_limit: ResourceRequest::default(),
        correlation_key: None,
        idempotency_key: None,
    }
}

fn task_id(value: u64) -> TaskId {
    TaskId::from_id(qubit_id::Id::new(value))
}

async fn start_task(store: &dyn TaskStore, id: TaskId) -> qubit_task::model::TaskSummary {
    let accepted = store
        .accept_encoded(id, request())
        .await
        .expect("task acceptance succeeds");
    let AcceptOutcome { summary, .. } = accepted;
    store
        .start_encoded(StartCommand {
            id,
            expected_state_version: summary.state_version,
            started_at_ms: 11,
        })
        .await
        .expect("queued task starts")
}

async fn finish_with_output(
    store: &dyn TaskStore,
    running: &qubit_task::model::TaskSummary,
    output: TaskOutput,
) -> Result<qubit_task::model::TaskSummary, StoreError> {
    store
        .transition_encoded(TransitionCommand {
            id: running.id,
            expected_state_version: running.state_version,
            expected_attempt: running.attempt,
            retry_not_before_ms: None,
            state: TaskState::Succeeded,
            cancel_requested: false,
            cancel_error: None,
            finished_at_ms: Some(12),
            output: Some(output),
        })
        .await
}

#[tokio::test]
async fn typed_memory_store_persists_success_output_in_summary_and_record() {
    let store = MemoryTaskStore::new(16);
    let running = start_task(&store, task_id(1)).await;
    let expected = TaskOutput {
        summary: vec![4, 2],
    };

    let finished = finish_with_output(&store, &running, expected.clone())
        .await
        .expect("successful transition stores output");
    let loaded = store
        .get_encoded_task(running.id)
        .await
        .expect("task lookup succeeds")
        .expect("task remains retained");

    assert_eq!(finished.output, Some(expected.clone()));
    assert_eq!(loaded.summary.output, Some(expected));
    assert_eq!(loaded.summary.state, TaskState::Succeeded);
}

async fn assert_oversized_output_is_atomic(store: &dyn TaskStore, id: TaskId) {
    let running = start_task(store, id).await;
    let rejected = finish_with_output(
        store,
        &running,
        TaskOutput {
            summary: vec![7; MAX_TASK_OUTPUT_SUMMARY_BYTES + 1],
        },
    )
    .await;

    assert!(matches!(
        rejected,
        Err(StoreError::InvalidRequest(
            "task output summary exceeds the 64 KiB limit"
        ))
    ));
    let unchanged = store
        .get_encoded_task(id)
        .await
        .expect("lookup after rejection succeeds")
        .expect("task still exists")
        .summary;
    assert_eq!(unchanged.state, TaskState::Running);
    assert_eq!(unchanged.state_version, running.state_version);
    assert_eq!(unchanged.finished_at_ms, None);
    assert_eq!(unchanged.output, None);
}

#[tokio::test]
async fn typed_memory_store_rejects_oversized_output_without_mutating_task() {
    let store = MemoryTaskStore::new(16);
    assert_oversized_output_is_atomic(&store, task_id(2)).await;
}

#[cfg(feature = "sqlite")]
fn remove_database(path: &std::path::Path) {
    let mut owner_lock = path.as_os_str().to_owned();
    owner_lock.push(".owner.lock");
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(owner_lock);
    let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
    let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn typed_sqlite_store_persists_output_and_reads_older_lifecycle_without_one() {
    use rusqlite::Connection;

    let directory =
        std::env::temp_dir().join(format!("qubit-task-output-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&directory).expect("test directory creates");
    let path = directory.join("tasks.sqlite");
    let id = task_id(3);
    let expected = TaskOutput {
        summary: vec![9, 8, 7],
    };

    let store = SqliteTaskStore::open_next(&path).expect("typed SQLite store opens");
    let running = start_task(&store, id).await;
    let finished = finish_with_output(&store, &running, expected.clone())
        .await
        .expect("successful transition stores output");
    assert_eq!(finished.output, Some(expected.clone()));
    drop(store);

    let reopened = SqliteTaskStore::open_next(&path).expect("typed SQLite store reopens");
    let loaded = reopened
        .get_encoded_task(id)
        .await
        .expect("task lookup succeeds after reopen")
        .expect("task remains retained");
    assert_eq!(loaded.summary.output, Some(expected));
    assert_eq!(loaded.summary.state, TaskState::Succeeded);
    drop(reopened);

    let connection =
        Connection::open(&path).expect("SQLite database opens for compatibility fixture");
    connection
        .execute(
            "UPDATE tasks SET lifecycle_json=json_remove(lifecycle_json, '$.output') WHERE id=?1",
            [id.to_padded_decimal()],
        )
        .expect("remove output field from prior-format lifecycle");
    drop(connection);
    let compatible =
        SqliteTaskStore::open_next(&path).expect("lifecycle without output remains readable");
    let summary = compatible
        .get_encoded_task(id)
        .await
        .expect("legacy lifecycle lookup succeeds")
        .expect("task remains available")
        .summary;
    assert_eq!(summary.state, TaskState::Succeeded);
    assert_eq!(summary.output, None);
    drop(compatible);
    remove_database(&path);
    let _ = std::fs::remove_dir(directory);
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn typed_sqlite_store_rejects_oversized_output_without_mutating_task() {
    let directory =
        std::env::temp_dir().join(format!("qubit-task-output-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&directory).expect("test directory creates");
    let path = directory.join("tasks.sqlite");
    let store = SqliteTaskStore::open_next(&path).expect("typed SQLite store opens");
    assert_oversized_output_is_atomic(&store, task_id(4)).await;
    drop(store);
    remove_database(&path);
    let _ = std::fs::remove_dir(directory);
}
