// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::num::NonZeroUsize;
use std::time::Duration;

use qubit_task::model::AcceptOutcome;
use qubit_task::model::TaskId;
use qubit_task::model::TaskRecord;
use qubit_task::model::TaskRequest;
use qubit_task::model::TaskState;
use qubit_task::model::TaskStateCounts;
use qubit_task::model::TransitionCommand;
use qubit_task::store::MemoryTaskStore;
use qubit_task::store::StoreError;
use qubit_task::store::TaskStore;

/// Accepts a task with an optional idempotency key.
async fn accept(store: &dyn TaskStore, idempotency_key: Option<&str>) -> TaskRecord {
    let mut request = TaskRequest::new("retention", "1", Vec::new());
    request.idempotency_key = idempotency_key.map(str::to_owned);
    let outcome = store
        .accept(TaskId::generate(), request)
        .await
        .expect("store accepts task");
    match outcome {
        AcceptOutcome::Accepted(record) => record,
        AcceptOutcome::Existing(_) => panic!("fresh task ID has no existing request"),
    }
}

/// Applies a transition using the record's current revision.
async fn transition(store: &dyn TaskStore, record: &TaskRecord, state: TaskState) -> qubit_task::model::TaskSummary {
    store
        .transition(TransitionCommand {
            id: record.id,
            expected_version: record.state_version,
            expected_attempt: record.attempt,
            state,
            output: None,
            assigned_resources: Vec::new(),
            retry_not_before_ms: None,
            cancel_requested: false,
        })
        .await
        .expect("store transitions task")
}

async fn transition_summary(
    store: &dyn TaskStore,
    record: &qubit_task::model::TaskSummary,
    state: TaskState,
) -> qubit_task::model::TaskSummary {
    store
        .transition(TransitionCommand {
            id: record.id,
            expected_version: record.state_version,
            expected_attempt: record.attempt,
            state,
            output: None,
            assigned_resources: Vec::new(),
            retry_not_before_ms: None,
            cancel_requested: false,
        })
        .await
        .unwrap()
}

/// Creates a terminal record and returns its final snapshot.
async fn terminal(store: &dyn TaskStore, idempotency_key: Option<&str>) -> qubit_task::model::TaskSummary {
    let record = accept(store, idempotency_key).await;
    transition(store, &record, TaskState::Cancelled).await
}

/// Returns the current Unix epoch timestamp in milliseconds.
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock is after Unix epoch")
        .as_millis() as u64
}

/// Verifies bounded pruning, state protection, counts, and idempotency cleanup.
async fn check_pruning_contract(store: &dyn TaskStore) {
    let first_old = terminal(store, Some("expired-key")).await;
    tokio::time::sleep(Duration::from_millis(3)).await;
    let second_old = terminal(store, None).await;
    let blocked = transition(
        store,
        &accept(store, None).await,
        TaskState::Blocked {
            reason: "operator action".into(),
        },
    )
    .await;
    let running = transition(store, &accept(store, None).await, TaskState::Running).await;
    let cutoff = first_old
        .accepted_at_ms
        .max(second_old.accepted_at_ms)
        .max(blocked.accepted_at_ms)
        .max(running.accepted_at_ms)
        + 1;
    tokio::time::sleep(Duration::from_millis(3)).await;
    let newer = terminal(store, None).await;

    let max_rows = NonZeroUsize::new(1).expect("one is nonzero");
    assert_eq!(
        store
            .prune_terminal_before(cutoff, max_rows)
            .await
            .expect("first prune works"),
        1
    );
    assert!(store.get(first_old.id).await.expect("first old lookup works").is_none());
    assert!(
        store
            .get(second_old.id)
            .await
            .expect("second old lookup works")
            .is_some()
    );

    let replacement = store
        .accept(
            TaskId::generate(),
            TaskRequest {
                idempotency_key: Some("expired-key".into()),
                ..TaskRequest::new("retention", "1", Vec::new())
            },
        )
        .await
        .expect("deleted idempotency key may be reused");
    assert!(matches!(replacement, AcceptOutcome::Accepted(_)));

    assert_eq!(
        store
            .prune_terminal_before(cutoff, max_rows)
            .await
            .expect("second prune works"),
        1
    );
    assert!(
        store
            .get(second_old.id)
            .await
            .expect("second old lookup works")
            .is_none()
    );
    assert!(store.get(newer.id).await.expect("newer lookup works").is_some());
    assert!(store.get(blocked.id).await.expect("blocked lookup works").is_some());
    assert!(store.get(running.id).await.expect("running lookup works").is_some());
    assert_eq!(
        store.count_states().await.expect("counts reflect pruned rows"),
        TaskStateCounts {
            queued: 1,
            running: 1,
            blocked: 1,
            terminal: 1,
        }
    );
}

#[tokio::test]
async fn test_memory_store_prunes_only_bounded_expired_terminal_records() {
    let store = MemoryTaskStore::new(16);
    check_pruning_contract(&store).await;
}

async fn check_abandon_blocked_contract(store: &dyn TaskStore) {
    let original = accept(store, Some("abandon-version")).await;
    let blocked = transition(store, &original, TaskState::Blocked { reason: "stuck".into() }).await;
    let cancelled = store.abandon_blocked(blocked.id, blocked.state_version).await.unwrap();
    assert!(matches!(cancelled.state, TaskState::Cancelled));
    assert_eq!(cancelled.state_version, blocked.state_version + 1);
    assert!(matches!(
        store.abandon_blocked(blocked.id, blocked.state_version).await,
        Err(StoreError::Conflict)
    ));
    assert_eq!(store.count_states().await.unwrap().terminal, 1);

    let queued = accept(store, None).await;
    let blocked = transition(
        store,
        &queued,
        TaskState::Blocked {
            reason: "retry race".into(),
        },
    )
    .await;
    let retried = transition_summary(store, &blocked, TaskState::Queued).await;
    assert!(matches!(
        store.abandon_blocked(retried.id, blocked.state_version).await,
        Err(StoreError::Conflict)
    ));
    assert!(matches!(
        store.get(retried.id).await.unwrap().unwrap().state,
        TaskState::Queued
    ));
}

#[tokio::test]
async fn test_memory_store_abandons_blocked_only_at_expected_version() {
    let store = MemoryTaskStore::new(16);
    check_abandon_blocked_contract(&store).await;
    let limited = MemoryTaskStore::with_limits(4, NonZeroUsize::new(1024).unwrap(), NonZeroUsize::new(1).unwrap());
    let first = accept(&limited, None).await;
    let blocked = transition(&limited, &first, TaskState::Blocked { reason: "old".into() }).await;
    assert!(matches!(
        limited
            .accept(TaskId::generate(), TaskRequest::new("retention", "1", Vec::new()))
            .await,
        Err(StoreError::UnfinishedRecordLimitExceeded { .. })
    ));
    limited
        .abandon_blocked(blocked.id, blocked.state_version)
        .await
        .unwrap();
    assert!(
        limited
            .accept(TaskId::generate(), TaskRequest::new("retention", "1", Vec::new()))
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn test_service_abandon_blocked_uses_the_observed_revision() {
    use std::sync::Arc;

    use qubit_task::TaskExecutionServiceBuilder;
    use qubit_task::service::TaskServiceError;

    let store = Arc::new(MemoryTaskStore::new(16));
    let service = TaskExecutionServiceBuilder::default()
        .store(store)
        .build()
        .await
        .unwrap();
    let accepted = service
        .submit(TaskRequest::new("no-handler", "1", b"payload".to_vec()).with_idempotency_key("service-abandon"))
        .await
        .unwrap();
    assert!(matches!(
        service.wait(accepted.id).await,
        Err(TaskServiceError::Blocked)
    ));
    let blocked = service.get_summary(accepted.id).await.unwrap().unwrap();
    let page = service
        .list(qubit_task::model::TaskQuery {
            states: vec![qubit_task::model::TaskStateKind::Blocked],
            limit: 10,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(page.records, vec![blocked.clone()]);
    assert_eq!(
        service.get(accepted.id).await.unwrap().unwrap().request.payload,
        b"payload"
    );
    let cancelled = service
        .abandon_blocked(blocked.id, blocked.state_version)
        .await
        .unwrap();
    assert!(matches!(cancelled.state, TaskState::Cancelled));
    assert_eq!(service.wait(accepted.id).await.unwrap(), cancelled);
    assert!(matches!(
        service.abandon_blocked(blocked.id, blocked.state_version).await,
        Err(TaskServiceError::Store(StoreError::Conflict))
    ));
    let not_blocked = service
        .abandon_blocked(blocked.id, cancelled.state_version)
        .await
        .expect_err("terminal tasks cannot be abandoned as blocked");
    assert_eq!(
        not_blocked.to_string(),
        "task is not blocked (current state: Cancelled)"
    );
    assert!(matches!(
        not_blocked,
        TaskServiceError::NotBlocked {
            actual: qubit_task::model::TaskStateKind::Cancelled
        }
    ));
    service.shutdown().await.unwrap();
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_sqlite_store_abandons_blocked_only_at_expected_version() {
    let path = std::env::temp_dir().join(format!("qubit-task-abandon-{}.sqlite", TaskId::generate()));
    let store = qubit_task::store::SqliteTaskStore::open(&path).unwrap();
    check_abandon_blocked_contract(&store).await;
    drop(store);
    remove_database(&path);
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_sqlite_store_prunes_only_bounded_expired_terminal_records() {
    let path = std::env::temp_dir().join(format!("qubit-task-retention-{}.sqlite", TaskId::generate()));
    {
        let store = qubit_task::store::SqliteTaskStore::open(&path).expect("SQLite store opens");
        check_pruning_contract(&store).await;
    }
    remove_database(&path);
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_sqlite_pruning_is_persistent_after_reopen() {
    let path = std::env::temp_dir().join(format!("qubit-task-retention-reopen-{}.sqlite", TaskId::generate()));
    let id;
    {
        let store = qubit_task::store::SqliteTaskStore::open(&path).expect("SQLite store opens");
        id = terminal(&store, None).await.id;
        assert_eq!(
            store
                .prune_terminal_before(now_ms() + 10_000, NonZeroUsize::new(1).expect("one is nonzero"))
                .await
                .expect("terminal record is pruned"),
            1
        );
    }
    {
        let store = qubit_task::store::SqliteTaskStore::open(&path).expect("SQLite store reopens");
        assert!(store.get(id).await.expect("record lookup works").is_none());
    }
    remove_database(&path);
}

#[tokio::test]
async fn test_task_store_default_pruning_reports_unsupported_capability() {
    struct NonPrunableStore(MemoryTaskStore);

    impl TaskStore for NonPrunableStore {
        fn capabilities(&self) -> qubit_task::model::StoreCapabilities {
            self.0.capabilities()
        }

        fn accept<'a>(
            &'a self,
            id: TaskId,
            request: TaskRequest,
        ) -> qubit_task::store::TaskFuture<'a, Result<AcceptOutcome, StoreError>> {
            self.0.accept(id, request)
        }

        fn get_by_idempotency_key<'a>(
            &'a self,
            key: &'a str,
        ) -> qubit_task::store::TaskFuture<'a, Result<Option<TaskRecord>, StoreError>> {
            self.0.get_by_idempotency_key(key)
        }

        fn transition<'a>(
            &'a self,
            command: TransitionCommand,
        ) -> qubit_task::store::TaskFuture<'a, Result<qubit_task::model::TaskSummary, StoreError>> {
            self.0.transition(command)
        }

        fn get_summary<'a>(
            &'a self,
            id: TaskId,
        ) -> qubit_task::store::TaskFuture<'a, Result<Option<qubit_task::model::TaskSummary>, StoreError>> {
            self.0.get_summary(id)
        }

        fn get<'a>(&'a self, id: TaskId) -> qubit_task::store::TaskFuture<'a, Result<Option<TaskRecord>, StoreError>> {
            self.0.get(id)
        }

        fn list<'a>(
            &'a self,
            query: qubit_task::model::TaskQuery,
        ) -> qubit_task::store::TaskFuture<'a, Result<qubit_task::model::TaskPage, StoreError>> {
            self.0.list(query)
        }

        fn count_states<'a>(&'a self) -> qubit_task::store::TaskFuture<'a, Result<TaskStateCounts, StoreError>> {
            self.0.count_states()
        }

        fn acquire_owner<'a>(
            &'a self,
        ) -> qubit_task::store::TaskFuture<'a, Result<qubit_task::model::OwnerEpoch, StoreError>> {
            self.0.acquire_owner()
        }

        fn has_unfinished_over_limit<'a>(
            &'a self,
            limit: usize,
        ) -> qubit_task::store::TaskFuture<'a, Result<bool, StoreError>> {
            self.0.has_unfinished_over_limit(limit)
        }

        fn scan_unfinished<'a>(
            &'a self,
            cursor: Option<TaskId>,
        ) -> qubit_task::store::TaskFuture<'a, Result<qubit_task::model::StoredTaskPage, StoreError>> {
            self.0.scan_unfinished(cursor)
        }

        fn release_owner<'a>(
            &'a self,
            epoch: qubit_task::model::OwnerEpoch,
        ) -> qubit_task::store::TaskFuture<'a, Result<(), StoreError>> {
            self.0.release_owner(epoch)
        }
    }

    let store = NonPrunableStore(MemoryTaskStore::new(8));
    let blocked = accept(&store, None).await;
    let blocked = transition(
        &store,
        &blocked,
        TaskState::Blocked {
            reason: "manual review".into(),
        },
    )
    .await;
    assert!(matches!(
        store.abandon_blocked(blocked.id, blocked.state_version).await,
        Err(StoreError::UnsupportedCapability)
    ));
    assert!(matches!(
        store
            .prune_terminal_before(now_ms(), NonZeroUsize::new(1).expect("one is nonzero"))
            .await,
        Err(StoreError::UnsupportedCapability)
    ));
}

#[tokio::test]
async fn test_service_pruning_uses_admission_and_removes_terminal_history() {
    let store = std::sync::Arc::new(MemoryTaskStore::new(8));
    let service = qubit_task::TaskExecutionServiceBuilder::in_memory()
        .store(store)
        .build()
        .await
        .expect("service builds");
    let task_id = service
        .submit_local(
            |_| qubit_task::service::LocalTaskOutcome::<(), std::io::Error>::Succeeded {
                value: (),
                summary: qubit_task::model::TaskOutput::default(),
            },
        )
        .await
        .expect("task is accepted")
        .task_id();
    service.wait(task_id).await.expect("task reaches terminal state");

    assert_eq!(
        service
            .prune_terminal_before(now_ms() + 10_000, NonZeroUsize::new(1).expect("one is nonzero"))
            .await
            .expect("service prunes terminal history"),
        1
    );
    assert!(service.get(task_id).await.expect("lookup succeeds").is_none());
    service.shutdown().await.expect("service shuts down");
}

#[cfg(feature = "sqlite")]
fn remove_database(path: &std::path::Path) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(path.with_extension("owner.lock"));
    let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
    let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
}
