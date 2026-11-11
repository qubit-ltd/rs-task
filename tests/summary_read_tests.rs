// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
// =============================================================================
//! Summary-only reads return lifecycle state without decoding stored payloads.

use qubit_id::Id;
use qubit_metadata::Metadata;
use qubit_model_id::ModelIdBuf;
use qubit_task::model::TaskState;
use qubit_task::model::typed::StartCommand;
use qubit_task::model::typed::StoredPayload;
use qubit_task::model::typed::StoredTaskRequest;
use qubit_task::model::typed::TaskId;
use qubit_task::model::typed::TransitionCommand;
use qubit_task::store::MemoryTaskStore;
use qubit_task::store::SqliteTaskStore;
use qubit_task::store::TaskStore;
use rusqlite::Connection;

fn task_id(value: u64) -> TaskId {
    TaskId::from_id(Id::new(value))
}

fn request(payload_size: usize) -> StoredTaskRequest {
    StoredTaskRequest {
        kind_id: "summary.read.test".to_owned(),
        category: None,
        payload: StoredPayload {
            type_id: ModelIdBuf::parse("qubit_task.tests.Payload").expect("valid model ID"),
            schema_version: 1,
            codec_id: "qubit.bytes.test".to_owned(),
            bytes: vec![0x5a; payload_size],
        },
        metadata: Metadata::new(),
        resource_limit: Default::default(),
        correlation_key: None,
        idempotency_key: None,
    }
}

#[tokio::test]
async fn memory_summary_reads_track_updates_without_cloning_payload() {
    let store = MemoryTaskStore::new(4);
    let id = task_id(301);
    let accepted = store
        .accept_encoded(id, request(8 * 1024 * 1024))
        .await
        .unwrap();
    let initial = store.get_summary(id).await.unwrap().unwrap();
    assert_eq!(initial, accepted.summary);

    let running = store
        .start_encoded(StartCommand {
            id,
            expected_state_version: initial.state_version,
            started_at_ms: 10,
        })
        .await
        .unwrap();
    let updated = store
        .update_progress(qubit_task::model::typed::ProgressCommand::new(
            id,
            running.attempt,
            1,
            None,
            Vec::new(),
            11,
        ))
        .await
        .unwrap();
    assert_eq!(store.get_summary(id).await.unwrap(), Some(updated.clone()));
    assert_eq!(store.get_summary(task_id(302)).await.unwrap(), None);

    let terminal = store
        .transition_encoded(TransitionCommand {
            id,
            expected_state_version: updated.state_version,
            expected_attempt: updated.attempt,
            retry_not_before_ms: None,
            state: TaskState::Failed {
                category: "test".to_owned(),
                message: "failed".to_owned(),
            },
            cancel_requested: false,
            cancel_error: None,
            finished_at_ms: Some(12),
            output: None,
        })
        .await
        .unwrap();
    assert_eq!(store.get_summary(id).await.unwrap(), Some(terminal));
}

#[tokio::test]
async fn sqlite_summary_reads_do_not_select_payload() {
    let path = std::env::temp_dir().join(format!(
        "qubit-task-summary-read-{}-{}.sqlite",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    ));
    let store = SqliteTaskStore::open(&path).unwrap();
    let id = task_id(401);
    let accepted = store.accept_encoded(id, request(1024)).await.unwrap();
    Connection::open(&path)
        .unwrap()
        .execute(
            "UPDATE tasks SET payload='not a blob' WHERE id=?1",
            [id.to_padded_decimal()],
        )
        .unwrap();

    assert_eq!(store.get_summary(id).await.unwrap(), Some(accepted.summary));
    assert!(matches!(store.get_encoded_task(id).await, Err(_)));
    assert_eq!(store.get_summary(task_id(402)).await.unwrap(), None);
    drop(store);
    let _ = std::fs::remove_file(&path);
    let mut lock_path = path.as_os_str().to_owned();
    lock_path.push(".owner.lock");
    let _ = std::fs::remove_file(std::path::PathBuf::from(lock_path));
}
