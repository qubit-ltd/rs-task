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
use std::env;
#[cfg(feature = "sqlite")]
use std::fs;
use std::num::NonZeroUsize;
#[cfg(feature = "sqlite")]
use std::sync::Arc;
#[cfg(feature = "sqlite")]
use std::sync::atomic::AtomicUsize;
#[cfg(feature = "sqlite")]
use std::sync::atomic::Ordering;

#[cfg(feature = "sqlite")]
use common::sqlite_paths;
#[cfg(feature = "sqlite")]
use qubit_codec::ValueBytesCodecDescriptor;
#[cfg(feature = "sqlite")]
use qubit_codec::ValueBytesCodecRegistration;
#[cfg(feature = "sqlite")]
use qubit_codec::ValueBytesCodecRegistry;
#[cfg(feature = "sqlite")]
use qubit_codec::ValueCodecId;
#[cfg(feature = "sqlite")]
use qubit_codec::ValueCodecRegistrationSource;
use qubit_id::Id;
use qubit_metadata::Metadata;
#[cfg(feature = "sqlite")]
use qubit_model_id::HasModelId;
#[cfg(feature = "sqlite")]
use qubit_model_id::ModelId;
use qubit_model_id::ModelIdBuf;
#[cfg(feature = "sqlite")]
use qubit_task::CancellationMode;
#[cfg(feature = "sqlite")]
use qubit_task::TaskContext;
#[cfg(feature = "sqlite")]
use qubit_task::TaskExecutionServiceBuilder;
#[cfg(feature = "sqlite")]
use qubit_task::TaskHandler;
#[cfg(feature = "sqlite")]
use qubit_task::TaskHandlerDescriptor;
#[cfg(feature = "sqlite")]
use qubit_task::handler::TaskRunOutcome;
use qubit_task::model::ResourceRequest;
use qubit_task::model::StartCommand;
use qubit_task::model::StoredPayload;
use qubit_task::model::StoredTaskRequest;
use qubit_task::model::TaskId;
#[cfg(feature = "sqlite")]
use qubit_task::model::TaskOutput;
#[cfg(feature = "sqlite")]
use qubit_task::model::TaskRequest;
use qubit_task::model::TaskState;
use qubit_task::model::TransitionCommand;
use qubit_task::store::MemoryTaskStore;
#[cfg(feature = "sqlite")]
use qubit_task::store::SqliteTaskStore;
#[cfg(feature = "sqlite")]
use qubit_task::store::TaskFuture;
use qubit_task::store::TaskStore;
#[cfg(feature = "sqlite")]
use rusqlite::Connection;
use tokio::test as tokio_test;
#[cfg(feature = "sqlite")]
use uuid::Uuid;

fn assert_task_store_is_object_safe(_: &dyn TaskStore) {}

#[cfg(feature = "sqlite")]
#[derive(Clone, Copy)]
struct IdentityProbe(u8);

#[cfg(feature = "sqlite")]
impl HasModelId for IdentityProbe {
    const MODEL_ID: ModelId = ModelId::new("test.IdentityProbe");
}

#[cfg(feature = "sqlite")]
#[derive(Default)]
struct IdentityProbeCodec;

#[cfg(feature = "sqlite")]
impl qubit_codec::ValueEncoder<IdentityProbe> for IdentityProbeCodec {
    type Output = Vec<u8>;
    type Error = std::convert::Infallible;

    fn encode(&mut self, value: &IdentityProbe) -> Result<Vec<u8>, Self::Error> {
        Ok(vec![value.0])
    }
}

#[cfg(feature = "sqlite")]
impl qubit_codec::ValueDecoder<[u8]> for IdentityProbeCodec {
    type Output = IdentityProbe;
    type Error = std::io::Error;

    fn decode(&mut self, bytes: &[u8]) -> Result<IdentityProbe, Self::Error> {
        bytes
            .first()
            .copied()
            .map(IdentityProbe)
            .ok_or_else(|| std::io::Error::other("empty probe"))
    }
}

#[cfg(feature = "sqlite")]
static IDENTITY_PROBE_DESCRIPTOR: ValueBytesCodecDescriptor =
    ValueBytesCodecDescriptor::of::<IdentityProbeCodec, IdentityProbe>();
#[cfg(feature = "sqlite")]
static IDENTITY_PROBE_REGISTRATION: ValueBytesCodecRegistration = ValueBytesCodecRegistration::new(
    ValueCodecId::new("task_store_contract.identity_probe"),
    &IDENTITY_PROBE_DESCRIPTOR,
    ValueCodecRegistrationSource::new("qubit-task", "task_store_contract_tests", file!(), 1),
);

#[cfg(feature = "sqlite")]
struct SequenceIds(AtomicUsize);

#[cfg(feature = "sqlite")]
impl qubit_id::IdGenerator for SequenceIds {
    fn generate(&self) -> Result<Id, qubit_id::IdGenerationError> {
        let generated = match self.0.fetch_add(1, Ordering::Relaxed) {
            0 => 883,
            _ => 886,
        };
        Ok(Id::new(generated))
    }
}

#[cfg(feature = "sqlite")]
struct IdentityProbeHandler;

#[cfg(feature = "sqlite")]
impl TaskHandler<IdentityProbe> for IdentityProbeHandler {
    fn run<'a>(
        &'a self,
        _value: IdentityProbe,
        _context: TaskContext,
    ) -> TaskFuture<'a, qubit_task::handler::TaskRunResult> {
        Box::pin(async { Ok(TaskRunOutcome::Succeeded(TaskOutput::default())) })
    }
}

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
async fn test_memory_state_checkpoint_reports_current_state_and_version() {
    let store = MemoryTaskStore::new(8);
    let id = TaskId::from_id(Id::new(104));
    let accepted = store
        .accept_encoded(
            id,
            StoredTaskRequest {
                kind_id: "checkpoint.contract".into(),
                category: None,
                payload: StoredPayload {
                    type_id: ModelIdBuf::parse("test.Checkpoint").expect("valid model ID"),
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
        .expect("task acceptance succeeds");

    let checkpoint = store
        .get_state_checkpoint(id)
        .await
        .expect("checkpoint lookup succeeds")
        .expect("accepted task has an authoritative state");
    assert_eq!(checkpoint.id, id);
    assert_eq!(checkpoint.state_version, accepted.summary.state_version);
    assert_eq!(checkpoint.state, accepted.summary.state);
    assert!(
        store
            .get_state_checkpoint(TaskId::from_id(Id::new(105)))
            .await
            .expect("unknown checkpoint lookup succeeds")
            .is_none()
    );
}

#[tokio_test]
async fn test_memory_store_obeys_terminal_prune_contract() {
    let store = MemoryTaskStore::new(16);
    support::store_contract::check_terminal_prune_contract(&store).await;
    for raw_id in [201, 202, 203] {
        let id = TaskId::from_id(Id::new(raw_id));
        assert!(
            store
                .get_state_checkpoint(id)
                .await
                .expect("pruned checkpoint lookup succeeds")
                .is_none()
        );
    }
}

#[tokio_test]
async fn test_memory_state_checkpoint_disappears_after_capacity_eviction() {
    let store = MemoryTaskStore::new(1);
    let first_id = TaskId::from_id(Id::new(401));
    let second_id = TaskId::from_id(Id::new(402));

    for id in [first_id, second_id] {
        let accepted = store
            .accept_encoded(
                id,
                StoredTaskRequest {
                    kind_id: "checkpoint.eviction".into(),
                    category: None,
                    payload: StoredPayload {
                        type_id: ModelIdBuf::parse("test.Checkpoint").expect("valid model ID"),
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
            .expect("task acceptance succeeds");
        let running = store
            .start_encoded(StartCommand {
                id,
                expected_state_version: accepted.summary.state_version,
                started_at_ms: accepted.summary.accepted_at_ms,
            })
            .await
            .expect("task starts");
        store
            .transition_encoded(TransitionCommand {
                id,
                expected_state_version: running.state_version,
                expected_attempt: running.attempt,
                retry_not_before_ms: None,
                state: TaskState::Succeeded,
                output: None,
                cancel_requested: false,
                cancel_error: None,
                finished_at_ms: Some(running.accepted_at_ms),
            })
            .await
            .expect("task reaches terminal state");
    }

    assert!(
        store
            .get_encoded_task(first_id)
            .await
            .expect("evicted task lookup succeeds")
            .is_none()
    );
    assert!(
        store
            .get_state_checkpoint(first_id)
            .await
            .expect("evicted checkpoint lookup succeeds")
            .is_none()
    );
    let checkpoint = store
        .get_state_checkpoint(second_id)
        .await
        .expect("retained checkpoint lookup succeeds")
        .expect("newest terminal checkpoint remains available");
    assert_eq!(checkpoint.state, TaskState::Succeeded);
    assert_eq!(checkpoint.state_version, 2);
}

#[tokio_test]
async fn test_memory_terminal_prune_releases_payload_budget() {
    let store = MemoryTaskStore::with_payload_budget(16, NonZeroUsize::new(4).expect("non-zero memory payload budget"));
    support::store_contract::check_memory_prune_reclaims_payload(&store).await;
}

#[cfg(feature = "sqlite")]
#[tokio_test]
async fn test_sqlite_store_obeys_core_contract() {
    let path = env::temp_dir().join(format!("rs-task-contract-{}.sqlite", Uuid::new_v4()));
    let store = SqliteTaskStore::open(&path).expect("typed sqlite store opens");
    support::store_contract::check_core_contract(&store).await;
    drop(store);
    for file in [
        &path,
        &sqlite_paths::owner_lock_path(&path),
        &path.with_extension("sqlite-wal"),
        &path.with_extension("sqlite-shm"),
    ] {
        let _ = fs::remove_file(file);
    }
}

#[cfg(feature = "sqlite")]
#[tokio_test]
async fn test_sqlite_store_obeys_terminal_prune_contract() {
    let path = env::temp_dir().join(format!("rs-task-prune-contract-{}.sqlite", Uuid::new_v4()));
    let store = SqliteTaskStore::open(&path).expect("typed sqlite store opens");
    support::store_contract::check_terminal_prune_contract(&store).await;
    drop(store);
    for file in [
        &path,
        &sqlite_paths::owner_lock_path(&path),
        &path.with_extension("sqlite-wal"),
        &path.with_extension("sqlite-shm"),
    ] {
        let _ = fs::remove_file(file);
    }
}

#[cfg(feature = "sqlite")]
#[tokio_test]
async fn test_sqlite_pruned_task_identity_survives_checkpoint_and_reopen() {
    let path = env::temp_dir().join(format!("rs-task-identity-contract-{}.sqlite", Uuid::new_v4()));
    let id = TaskId::from_id(Id::new(883));
    let request = StoredTaskRequest {
        kind_id: "identity.contract".into(),
        category: None,
        payload: StoredPayload {
            type_id: ModelIdBuf::parse("test.IdentityContract").unwrap(),
            schema_version: 1,
            codec_id: "qubit.bytes.json".into(),
            bytes: vec![1],
        },
        metadata: Metadata::new(),
        resource_limit: ResourceRequest::default(),
        correlation_key: None,
        idempotency_key: Some("identity-key".into()),
    };
    let store = SqliteTaskStore::open(&path).expect("open typed sqlite store");
    let accepted = store.accept_encoded(id, request.clone()).await.expect("accept task");
    assert!(
        !store
            .accept_encoded(TaskId::from_id(Id::new(884)), request.clone())
            .await
            .unwrap()
            .created
    );
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
            finished_at_ms: Some(2),
            output: None,
        })
        .await
        .unwrap();
    assert_eq!(
        store
            .prune_terminal_before(i64::MAX as u64, NonZeroUsize::new(1).unwrap())
            .await
            .unwrap(),
        1
    );
    let checkpoint = store.get_state_checkpoint(id).await.unwrap().unwrap();
    assert_eq!(checkpoint.state, TaskState::Succeeded);
    assert_eq!(checkpoint.state_version, 2);
    assert!(store.get_encoded_task(id).await.unwrap().is_none());
    drop(store);

    let store = SqliteTaskStore::open(&path).expect("reopen typed sqlite store");
    let same_identity = store
        .accept_encoded(
            id,
            StoredTaskRequest {
                idempotency_key: None,
                ..request.clone()
            },
        )
        .await;
    assert!(matches!(
        same_identity,
        Err(qubit_task::store::StoreError::DuplicateTask)
    ));
    let checkpoint_after = store.get_state_checkpoint(id).await.unwrap().unwrap();
    assert_eq!(checkpoint_after.state, checkpoint.state);
    assert_eq!(checkpoint_after.state_version, checkpoint.state_version);
    assert!(store.get_encoded_task(id).await.unwrap().is_none());
    let released_key = store
        .accept_encoded(TaskId::from_id(Id::new(885)), request)
        .await
        .unwrap();
    assert!(
        released_key.created,
        "pruning releases the idempotency key for a new ID"
    );
    drop(store);
    for file in [
        &path,
        &sqlite_paths::owner_lock_path(&path),
        &path.with_extension("sqlite-wal"),
        &path.with_extension("sqlite-shm"),
    ] {
        let _ = fs::remove_file(file);
    }
}

#[cfg(feature = "sqlite")]
#[tokio_test]
async fn test_sqlite_duplicate_generated_identity_does_not_fault_service() {
    let path = env::temp_dir().join(format!("rs-task-service-identity-{}.sqlite", Uuid::new_v4()));
    let id = TaskId::from_id(Id::new(883));
    let store = SqliteTaskStore::open(&path).expect("open typed sqlite store");
    let accepted = store
        .accept_encoded(
            id,
            StoredTaskRequest {
                kind_id: "identity.service".into(),
                category: None,
                payload: StoredPayload {
                    type_id: ModelIdBuf::parse("test.IdentityProbe").unwrap(),
                    schema_version: 1,
                    codec_id: "task_store_contract.identity_probe".into(),
                    bytes: vec![1],
                },
                metadata: Metadata::new(),
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
            finished_at_ms: Some(2),
            output: None,
        })
        .await
        .unwrap();
    assert_eq!(
        store
            .prune_terminal_before(i64::MAX as u64, NonZeroUsize::new(1).unwrap())
            .await
            .unwrap(),
        1
    );
    drop(store);

    let store = Arc::new(SqliteTaskStore::open(&path).expect("reopen for task service"));
    let codecs = Arc::new(ValueBytesCodecRegistry::from_registrations([&IDENTITY_PROBE_REGISTRATION]).unwrap());
    let mut builder = TaskExecutionServiceBuilder::new(store, codecs, Arc::new(SequenceIds(AtomicUsize::new(0))));
    builder
        .handlers_mut()
        .register::<IdentityProbe, _>(
            TaskHandlerDescriptor {
                kind_id: "identity.service".into(),
                payload_type_id: ModelIdBuf::parse("test.IdentityProbe").unwrap(),
                accepted_schema_versions: vec![1],
                cancellation_mode: CancellationMode::Cooperative,
            },
            Arc::new(IdentityProbeHandler),
        )
        .expect("handler registers");
    let service = builder.build().await.expect("service starts");
    let request = || {
        TaskRequest::new(
            "identity.service",
            1,
            ValueCodecId::new("task_store_contract.identity_probe"),
            IdentityProbe(2),
        )
    };
    assert!(matches!(
        service.submit(request()).await,
        Err(qubit_task::service::TaskServiceError::DuplicateTaskId)
    ));
    let accepted_after_collision = service
        .submit(request())
        .await
        .expect("service accepts a new identity after collision");
    assert_eq!(accepted_after_collision.id, TaskId::from_id(Id::new(886)));
    service.shutdown().await.expect("collision did not fault service");
    drop(service);
    for file in [
        &path,
        &sqlite_paths::owner_lock_path(&path),
        &path.with_extension("sqlite-wal"),
        &path.with_extension("sqlite-shm"),
    ] {
        let _ = fs::remove_file(file);
    }
}

#[cfg(feature = "sqlite")]
#[tokio_test]
async fn test_sqlite_terminal_prune_rolls_back_when_a_delete_fails() {
    let path = env::temp_dir().join(format!("rs-task-prune-atomic-{}.sqlite", Uuid::new_v4()));
    let store = SqliteTaskStore::open(&path).expect("typed sqlite store opens");
    let ids = [TaskId::from_id(Id::new(881)), TaskId::from_id(Id::new(882))];
    for (index, id) in ids.iter().copied().enumerate() {
        let accepted = store
            .accept_encoded(
                id,
                StoredTaskRequest {
                    kind_id: "prune.atomic".into(),
                    category: None,
                    payload: StoredPayload {
                        type_id: ModelIdBuf::parse("test.PruneAtomic").unwrap(),
                        schema_version: 1,
                        codec_id: "qubit.bytes.json".into(),
                        bytes: vec![index as u8],
                    },
                    metadata: Metadata::new(),
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
    let blocker = Connection::open(&path).unwrap();
    blocker
        .execute_batch(&format!(
            concat!(
                "CREATE TRIGGER fail_second_prune_delete BEFORE DELETE ON tasks ",
                "WHEN OLD.id='{}' BEGIN SELECT RAISE(ABORT,",
                "'injected prune failure'); END;",
            ),
            ids[1].to_padded_decimal(),
        ))
        .unwrap();
    assert!(
        store
            .prune_terminal_before(u64::MAX, NonZeroUsize::new(2).unwrap())
            .await
            .is_err()
    );
    for id in ids {
        assert!(
            store.get_encoded_task(id).await.unwrap().is_some(),
            "transaction rollback retains every selected task"
        );
        let checkpoint = store
            .get_state_checkpoint(id)
            .await
            .unwrap()
            .expect("the retained task continues to report its current checkpoint");
        assert_eq!(checkpoint.id, id);
        assert_eq!(checkpoint.state_version, 2);
        assert_eq!(checkpoint.state, TaskState::Succeeded);
        let staged_checkpoints: i64 = blocker
            .query_row(
                "SELECT COUNT(*) FROM task_terminal_checkpoints WHERE task_id=?1",
                [id.to_padded_decimal()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(staged_checkpoints, 0, "transaction rollback removes staged checkpoints");
    }
    drop(blocker);
    drop(store);
    for file in [
        &path,
        &sqlite_paths::owner_lock_path(&path),
        &path.with_extension("sqlite-wal"),
        &path.with_extension("sqlite-shm"),
    ] {
        let _ = fs::remove_file(file);
    }
}
