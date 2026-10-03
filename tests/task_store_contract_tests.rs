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
#[cfg(feature = "sqlite")]
use qubit_task::store::SqliteTaskStore;
use qubit_task::store::TaskStore;
use tokio::test as tokio_test;
#[cfg(feature = "sqlite")]
use uuid::Uuid;

fn assert_task_store_is_object_safe(_: &dyn TaskStore) {}

#[test]
fn test_typed_task_store_is_object_safe_and_reports_capabilities() {
    let store = MemoryTaskStore::new(8);
    assert_task_store_is_object_safe(&store);

    let store: &dyn TaskStore = &store;
    assert!(!store.capabilities().restart_recovery);
}

#[tokio_test]
async fn test_memory_store_obeys_core_contract() {
    let store = MemoryTaskStore::new(8);
    support::store_contract::check_core_contract(&store).await;
}

#[tokio_test]
async fn test_memory_store_obeys_terminal_prune_contract() {
    let store = MemoryTaskStore::new(16);
    support::store_contract::check_terminal_prune_contract(&store).await;
}

#[tokio_test]
async fn test_memory_terminal_prune_releases_payload_budget() {
    let store = MemoryTaskStore::with_payload_budget(
        16,
        std::num::NonZeroUsize::new(4).expect("non-zero memory payload budget"),
    );
    support::store_contract::check_memory_prune_reclaims_payload(&store).await;
}

#[cfg(feature = "sqlite")]
#[tokio_test]
async fn test_sqlite_store_obeys_core_contract() {
    let path = std::env::temp_dir().join(format!("rs-task-contract-{}.sqlite", Uuid::new_v4()));
    let store = SqliteTaskStore::open_next(&path).expect("typed sqlite store opens");
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

#[cfg(feature = "sqlite")]
#[tokio_test]
async fn test_sqlite_store_obeys_terminal_prune_contract() {
    let path = std::env::temp_dir().join(format!("rs-task-prune-contract-{}.sqlite", Uuid::new_v4()));
    let store = SqliteTaskStore::open_next(&path).expect("typed sqlite store opens");
    support::store_contract::check_terminal_prune_contract(&store).await;
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

#[cfg(feature = "sqlite")]
#[tokio_test]
async fn test_sqlite_terminal_prune_rolls_back_when_a_delete_fails() {
    use qubit_task::model::ResourceRequest;
    use qubit_task::model::StartCommand;
    use qubit_task::model::StoredPayload;
    use qubit_task::model::StoredTaskRequest;
    use qubit_task::model::TaskId;
    use qubit_task::model::TaskOutput;
    use qubit_task::model::TaskState;
    use qubit_task::model::TransitionCommand;

    let path = std::env::temp_dir().join(format!("rs-task-prune-atomic-{}.sqlite", Uuid::new_v4()));
    let store = SqliteTaskStore::open_next(&path).expect("typed sqlite store opens");
    let ids = [
        TaskId::from_id(qubit_id::Id::new(881)),
        TaskId::from_id(qubit_id::Id::new(882)),
    ];
    for (index, id) in ids.iter().copied().enumerate() {
        let accepted = store
            .accept_encoded(
                id,
                StoredTaskRequest {
                    kind_id: "prune.atomic".into(),
                    category: None,
                    payload: StoredPayload {
                        type_id: qubit_model_id::ModelIdBuf::parse("test.PruneAtomic").unwrap(),
                        schema_version: 1,
                        codec_id: "qubit.bytes.json".into(),
                        bytes: vec![index as u8],
                    },
                    metadata: qubit_metadata::Metadata::new(),
                    resource_limit: ResourceRequest::default(),
                    correlation_key: None,
                    idempotency_key: None,
                },
            )
            .await
            .unwrap();
        let running = store
            .start_encoded(StartCommand {
                id,
                expected_state_version: accepted.summary.state_version,
                started_at_ms: 1,
            })
            .await
            .unwrap();
        store
            .transition_encoded(TransitionCommand {
                id,
                expected_state_version: running.state_version,
                expected_attempt: running.attempt,
                retry_not_before_ms: None,
                state: TaskState::Succeeded,
                cancel_requested: false,
                cancel_error: None,
                finished_at_ms: Some(index as u64 + 10),
                output: Some(TaskOutput::default()),
            })
            .await
            .unwrap();
    }
    let blocker = rusqlite::Connection::open(&path).unwrap();
    blocker.execute_batch(&format!(
        "CREATE TRIGGER fail_second_prune_delete BEFORE DELETE ON tasks WHEN OLD.id='{}' BEGIN SELECT RAISE(ABORT,'injected prune failure'); END;",
        ids[1].to_padded_decimal(),
    )).unwrap();
    assert!(
        store
            .prune_terminal_before(u64::MAX, std::num::NonZeroUsize::new(2).unwrap())
            .await
            .is_err()
    );
    for id in ids {
        assert!(
            store.get_encoded_task(id).await.unwrap().is_some(),
            "transaction rollback retains every selected task"
        );
    }
    drop(blocker);
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
