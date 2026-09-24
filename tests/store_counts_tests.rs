// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::model::AcceptOutcome;
use qubit_task::model::OwnerEpoch;
use qubit_task::model::StoreCapabilities;
use qubit_task::model::StoredTaskPage;
use qubit_task::model::TaskId;
use qubit_task::model::TaskPage;
use qubit_task::model::TaskQuery;
use qubit_task::model::TaskRecord;
use qubit_task::model::TaskRequest;
use qubit_task::model::TaskState;
use qubit_task::model::TaskStateCounts;
use qubit_task::model::TransitionCommand;
use qubit_task::store::MemoryTaskStore;
use qubit_task::store::StoreError;
use qubit_task::store::TaskFuture;
use qubit_task::store::TaskStore;

struct FailListStore {
    inner: Arc<dyn TaskStore>,
    list_calls: AtomicUsize,
    count_calls: AtomicUsize,
}

impl TaskStore for FailListStore {
    fn capabilities(&self) -> StoreCapabilities {
        self.inner.capabilities()
    }

    fn accept<'a>(&'a self, id: TaskId, request: TaskRequest) -> TaskFuture<'a, Result<AcceptOutcome, StoreError>> {
        self.inner.accept(id, request)
    }

    fn find_idempotent<'a>(&'a self, request: TaskRequest) -> TaskFuture<'a, Result<Option<TaskRecord>, StoreError>> {
        self.inner.find_idempotent(request)
    }

    fn transition<'a>(&'a self, command: TransitionCommand) -> TaskFuture<'a, Result<TaskRecord, StoreError>> {
        self.inner.transition(command)
    }

    fn get<'a>(&'a self, id: TaskId) -> TaskFuture<'a, Result<Option<TaskRecord>, StoreError>> {
        self.inner.get(id)
    }

    fn list<'a>(&'a self, _query: TaskQuery) -> TaskFuture<'a, Result<TaskPage, StoreError>> {
        self.list_calls.fetch_add(1, Ordering::Relaxed);
        Box::pin(async { Err(StoreError::Failure("injected list failure".into())) })
    }

    fn count_states<'a>(&'a self) -> TaskFuture<'a, Result<TaskStateCounts, StoreError>> {
        self.count_calls.fetch_add(1, Ordering::Relaxed);
        self.inner.count_states()
    }

    fn acquire_owner<'a>(&'a self) -> TaskFuture<'a, Result<OwnerEpoch, StoreError>> {
        self.inner.acquire_owner()
    }

    fn scan_unfinished<'a>(&'a self, cursor: Option<TaskId>) -> TaskFuture<'a, Result<StoredTaskPage, StoreError>> {
        self.inner.scan_unfinished(cursor)
    }

    fn release_owner<'a>(&'a self, epoch: OwnerEpoch) -> TaskFuture<'a, Result<(), StoreError>> {
        self.inner.release_owner(epoch)
    }
}

/// Accepts one task through the public store contract.
async fn accept(store: &dyn TaskStore) -> TaskRecord {
    let outcome = store
        .accept(TaskId::generate(), TaskRequest::new("count", "1", Vec::new()))
        .await
        .expect("store accepts task");
    match outcome {
        AcceptOutcome::Accepted(record) => record,
        AcceptOutcome::Existing(_) => panic!("fresh request has no idempotency key"),
    }
}

/// Advances a record with its current revision and attempt.
async fn transition(store: &dyn TaskStore, record: &TaskRecord, state: TaskState) -> TaskRecord {
    store
        .transition(TransitionCommand {
            id: record.id,
            expected_version: record.state_version,
            expected_attempt: record.attempt,
            state,
            output: None,
            assigned_resources: Vec::new(),
            cancel_requested: false,
        })
        .await
        .expect("store updates task")
}

/// Verifies every state category against the same object-safe store API.
async fn check_all_state_categories(store: &dyn TaskStore) {
    assert_eq!(
        store.count_states().await.expect("empty store counts"),
        TaskStateCounts::default()
    );

    let _queued = accept(store).await;
    let running = transition(store, &accept(store).await, TaskState::Running).await;
    let _blocked = transition(
        store,
        &accept(store).await,
        TaskState::Blocked {
            reason: "operator review".into(),
        },
    )
    .await;
    let successful = transition(store, &accept(store).await, TaskState::Running).await;
    transition(store, &successful, TaskState::Succeeded).await;
    let failed = transition(store, &accept(store).await, TaskState::Running).await;
    transition(
        store,
        &failed,
        TaskState::Failed {
            category: "business".into(),
            message: "failed".into(),
        },
    )
    .await;
    transition(store, &accept(store).await, TaskState::Cancelled).await;
    let panicked = transition(store, &accept(store).await, TaskState::Running).await;
    transition(
        store,
        &panicked,
        TaskState::Panicked {
            message: "panic".into(),
        },
    )
    .await;

    assert_eq!(
        store.count_states().await.expect("all state counts"),
        TaskStateCounts {
            queued: 1,
            running: 1,
            blocked: 1,
            terminal: 4,
        }
    );
    transition(store, &running, TaskState::Cancelled).await;
    assert_eq!(
        store.count_states().await.expect("updated state counts"),
        TaskStateCounts {
            queued: 1,
            running: 0,
            blocked: 1,
            terminal: 5,
        }
    );
}

#[tokio::test]
async fn test_memory_store_counts_all_state_categories() {
    let store = MemoryTaskStore::new(16);
    check_all_state_categories(&store).await;
}

#[tokio::test]
async fn test_memory_store_counts_only_retained_terminal_records() {
    let store = MemoryTaskStore::new(1);
    let queued = accept(&store).await;
    let running = transition(&store, &accept(&store).await, TaskState::Running).await;
    let blocked = transition(
        &store,
        &accept(&store).await,
        TaskState::Blocked { reason: "hold".into() },
    )
    .await;
    let first = transition(&store, &accept(&store).await, TaskState::Cancelled).await;
    let second = transition(&store, &accept(&store).await, TaskState::Cancelled).await;

    assert!(store.get(first.id).await.expect("query succeeds").is_none());
    assert!(store.get(second.id).await.expect("query succeeds").is_some());
    assert!(store.get(queued.id).await.expect("query succeeds").is_some());
    assert!(store.get(running.id).await.expect("query succeeds").is_some());
    assert!(store.get(blocked.id).await.expect("query succeeds").is_some());
    assert_eq!(
        store.count_states().await.expect("retained state counts"),
        TaskStateCounts {
            queued: 1,
            running: 1,
            blocked: 1,
            terminal: 1,
        }
    );
}

#[tokio::test]
async fn test_store_count_works_when_list_fails() {
    let store = FailListStore {
        inner: Arc::new(MemoryTaskStore::new(8)),
        list_calls: AtomicUsize::new(0),
        count_calls: AtomicUsize::new(0),
    };
    accept(&store).await;
    assert!(matches!(
        store.list(TaskQuery::default()).await,
        Err(StoreError::Failure(_))
    ));
    assert_eq!(
        store.count_states().await.expect("count uses store aggregation"),
        TaskStateCounts {
            queued: 1,
            ..TaskStateCounts::default()
        }
    );
}

#[tokio::test]
async fn test_service_stats_uses_one_aggregate_without_listing_history() {
    let store = Arc::new(FailListStore {
        inner: Arc::new(MemoryTaskStore::new(8)),
        list_calls: AtomicUsize::new(0),
        count_calls: AtomicUsize::new(0),
    });
    let queued = accept(store.as_ref()).await;
    transition(store.as_ref(), &queued, TaskState::Cancelled).await;
    let service = TaskExecutionServiceBuilder::in_memory()
        .store(store.clone())
        .build()
        .await
        .expect("service builds");

    let stats = service.stats().await.expect("stats use store aggregation");
    assert_eq!(stats.queued, 0);
    assert_eq!(stats.running, 0);
    assert_eq!(stats.blocked, 0);
    assert_eq!(stats.terminal, 1);
    assert_eq!(store.count_calls.load(Ordering::Relaxed), 1);
    assert_eq!(store.list_calls.load(Ordering::Relaxed), 0);

    service.shutdown().await.expect("service shuts down");
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_sqlite_store_counts_all_state_categories() {
    use qubit_task::store::SqliteTaskStore;

    let path = std::env::temp_dir().join(format!("qubit-task-counts-{}.sqlite", TaskId::generate()));
    let store = SqliteTaskStore::open(&path).expect("SQLite store opens");
    check_all_state_categories(&store).await;
    drop(store);
    for file in [
        &path,
        &path.with_extension("owner.lock"),
        &path.with_extension("sqlite-wal"),
        &path.with_extension("sqlite-shm"),
    ] {
        let _ = std::fs::remove_file(file);
    }
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_service_stats_uses_one_sqlite_aggregate_for_large_history() {
    use qubit_task::store::SqliteTaskStore;

    let path = std::env::temp_dir().join(format!("qubit-task-stats-large-{}.sqlite", TaskId::generate()));
    let sqlite = Arc::new(SqliteTaskStore::open(&path).expect("SQLite store opens"));
    let store = Arc::new(FailListStore {
        inner: sqlite.clone(),
        list_calls: AtomicUsize::new(0),
        count_calls: AtomicUsize::new(0),
    });
    for _ in 0..520 {
        let accepted = accept(store.as_ref()).await;
        transition(store.as_ref(), &accepted, TaskState::Cancelled).await;
    }
    let service = TaskExecutionServiceBuilder::in_memory()
        .store(store.clone())
        .build()
        .await
        .expect("service recovers with the selected SQLite store");

    let stats = service.stats().await.expect("stats use one aggregate query");
    assert_eq!(stats.terminal, 520);
    assert_eq!(store.count_calls.load(Ordering::Relaxed), 1);
    assert_eq!(store.list_calls.load(Ordering::Relaxed), 0);

    service.shutdown().await.expect("service releases SQLite ownership");
    drop(service);
    drop(store);
    drop(sqlite);
    for file in [
        &path,
        &path.with_extension("owner.lock"),
        &path.with_extension("sqlite-wal"),
        &path.with_extension("sqlite-shm"),
    ] {
        let _ = std::fs::remove_file(file);
    }
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_sqlite_store_count_rejects_unknown_state_kind() {
    use qubit_task::store::SqliteTaskStore;

    let path = std::env::temp_dir().join(format!("qubit-task-counts-invalid-{}.sqlite", TaskId::generate()));
    let store = SqliteTaskStore::open(&path).expect("SQLite store opens");
    let record = accept(&store).await;
    rusqlite::Connection::open(&path)
        .expect("second connection opens")
        .execute(
            "UPDATE tasks SET state_kind='Unknown' WHERE id=?1",
            [record.id.to_string()],
        )
        .expect("test corrupts state kind");
    assert!(matches!(store.count_states().await, Err(StoreError::Failure(_))));
    drop(store);
    for file in [
        &path,
        &path.with_extension("owner.lock"),
        &path.with_extension("sqlite-wal"),
        &path.with_extension("sqlite-shm"),
    ] {
        let _ = std::fs::remove_file(file);
    }
}
