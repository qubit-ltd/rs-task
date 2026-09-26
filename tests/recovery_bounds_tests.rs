// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
#![cfg(feature = "sqlite")]

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::handler::TaskContext;
use qubit_task::handler::TaskHandler;
use qubit_task::handler::TaskHandlerDescriptor;
use qubit_task::handler::TaskRunOutcome;
use qubit_task::model::AcceptOutcome;
use qubit_task::model::OwnerEpoch;
use qubit_task::model::StoreCapabilities;
use qubit_task::model::StoredTask;
use qubit_task::model::StoredTaskPage;
use qubit_task::model::TaskId;
use qubit_task::model::TaskOutput;
use qubit_task::model::TaskPage;
use qubit_task::model::TaskQuery;
use qubit_task::model::TaskRequest;
use qubit_task::model::TaskState;
use qubit_task::model::TaskStateCounts;
use qubit_task::model::TransitionCommand;
use qubit_task::service::TaskServiceBuildError;
use qubit_task::service::TaskServiceError;
use qubit_task::store::SqliteTaskStore;
use qubit_task::store::StoreError;
use qubit_task::store::TaskFuture;
use qubit_task::store::TaskStore;

struct Echo;

impl TaskHandler for Echo {
    fn descriptor(&self) -> TaskHandlerDescriptor {
        TaskHandlerDescriptor {
            task_type: "echo".into(),
            version: "1".into(),
        }
    }

    fn run<'a>(
        &'a self,
        _payload: &'a [u8],
        _context: TaskContext,
    ) -> qubit_task::store::TaskFuture<'a, qubit_task::handler::TaskRunResult> {
        Box::pin(async { Ok(TaskRunOutcome::Succeeded(TaskOutput::default())) })
    }
}

fn temp_db() -> std::path::PathBuf {
    std::env::temp_dir().join(format!("qubit-task-recovery-{}.sqlite", TaskId::generate()))
}

fn cleanup(path: &std::path::Path) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(path.with_extension("owner.lock"));
    let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
    let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
}

async fn accept(store: &SqliteTaskStore) -> qubit_task::model::TaskRecord {
    match store
        .accept(TaskId::generate(), TaskRequest::new("echo", "1", Vec::new()))
        .await
        .unwrap()
    {
        AcceptOutcome::Accepted(record) => record,
        AcceptOutcome::Existing(_) => unreachable!(),
    }
}

#[tokio::test]
async fn test_recovery_blocks_exhausted_attempts_and_manual_retry_preserves_record() {
    let path = temp_db();
    let store = SqliteTaskStore::open(&path).unwrap();
    let running = accept(&store).await;
    let running = store
        .transition(TransitionCommand {
            id: running.id,
            expected_version: running.state_version,
            expected_attempt: running.attempt,
            state: TaskState::Running,
            output: None,
            assigned_resources: Vec::new(),
            retry_not_before_ms: None,
            cancel_requested: false,
        })
        .await
        .unwrap();
    let queued = accept(&store).await;
    let queued_attempt = store
        .transition(TransitionCommand {
            id: queued.id,
            expected_version: queued.state_version,
            expected_attempt: queued.attempt,
            state: TaskState::Running,
            output: None,
            assigned_resources: Vec::new(),
            retry_not_before_ms: None,
            cancel_requested: false,
        })
        .await
        .unwrap();
    let queued_attempt = store
        .transition(TransitionCommand {
            id: queued_attempt.id,
            expected_version: queued_attempt.state_version,
            expected_attempt: queued_attempt.attempt,
            state: TaskState::Queued,
            output: None,
            assigned_resources: Vec::new(),
            retry_not_before_ms: None,
            cancel_requested: false,
        })
        .await
        .unwrap();
    drop(store);

    let service = TaskExecutionServiceBuilder::recoverable_sqlite(&path)
        .unwrap()
        .max_attempts(1)
        .register_handler(Arc::new(Echo))
        .unwrap()
        .build()
        .await
        .unwrap();
    let recovered_running = service.get(running.id).await.unwrap().unwrap();
    let recovered_queued = service.get(queued.id).await.unwrap().unwrap();
    assert_eq!(recovered_running.attempt, 1);
    assert_eq!(recovered_queued.attempt, 1);
    assert!(matches!(recovered_running.state, TaskState::Blocked { .. }));
    assert!(matches!(recovered_queued.state, TaskState::Blocked { .. }));
    assert_eq!(queued_attempt.attempt, 1);
    assert!(matches!(service.wait(queued.id).await, Err(TaskServiceError::Blocked)));
    service.shutdown().await.unwrap();
    drop(service);
    cleanup(&path);
}

#[tokio::test]
async fn test_recovery_capacity_failure_preserves_records_and_releases_owner() {
    let path = temp_db();
    let inner = Arc::new(SqliteTaskStore::open(&path).unwrap());
    let mut ids = Vec::new();
    for _ in 0..4 {
        ids.push(accept(&inner).await.id);
    }
    let store = Arc::new(BadScanStore {
        inner,
        mode: BadPage::Normal,
        stored: Mutex::new(None),
        cursor: TaskId::generate(),
        precheck_calls: AtomicUsize::new(0),
        scan_calls: AtomicUsize::new(0),
    });

    let result = TaskExecutionServiceBuilder::default()
        .store(store.clone())
        .queue_capacity(2)
        .max_running_tasks(std::num::NonZeroUsize::new(1).unwrap())
        .require_recovery(true)
        .build()
        .await;
    let error = match result {
        Ok(_) => panic!("recovery exceeding the configured bound must fail"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        TaskServiceBuildError::RecoveryCapacityExceeded { limit: 3 }
    ));
    assert_eq!(store.precheck_calls.load(Ordering::Relaxed), 1);
    assert_eq!(store.scan_calls.load(Ordering::Relaxed), 0);
    drop(store);

    let check = SqliteTaskStore::open(&path).unwrap();
    for id in &ids {
        let record = check.get(*id).await.unwrap().unwrap();
        assert!(matches!(record.state, TaskState::Queued));
        assert_eq!(record.attempt, 0);
    }
    drop(check);

    let service = TaskExecutionServiceBuilder::recoverable_sqlite(&path)
        .unwrap()
        .queue_capacity(4)
        .max_running_tasks(std::num::NonZeroUsize::new(1).unwrap())
        .register_handler(Arc::new(Echo))
        .unwrap()
        .build()
        .await
        .unwrap();
    for id in ids {
        assert!(matches!(service.wait(id).await.unwrap().state, TaskState::Succeeded));
    }
    service.shutdown().await.unwrap();
    drop(service);
    cleanup(&path);
}

#[tokio::test]
async fn test_recovery_scans_across_page_boundary() {
    let path = temp_db();
    let store = SqliteTaskStore::open(&path).unwrap();
    for _ in 0..257 {
        accept(&store).await;
    }
    drop(store);

    let service = TaskExecutionServiceBuilder::recoverable_sqlite(&path)
        .unwrap()
        .queue_capacity(255)
        .max_running_tasks(std::num::NonZeroUsize::new(2).unwrap())
        .build()
        .await
        .unwrap();
    assert_eq!(service.stats().await.unwrap().blocked, 257);
    service.shutdown().await.unwrap();
    drop(service);
    cleanup(&path);
}

#[tokio::test]
async fn test_retry_blocked_rejects_exhausted_budget_without_mutation() {
    let path = temp_db();
    let store = SqliteTaskStore::open(&path).unwrap();
    let record = accept(&store).await;
    let running = store
        .transition(TransitionCommand {
            id: record.id,
            expected_version: record.state_version,
            expected_attempt: record.attempt,
            state: TaskState::Running,
            output: None,
            assigned_resources: Vec::new(),
            retry_not_before_ms: None,
            cancel_requested: false,
        })
        .await
        .unwrap();
    let blocked = store
        .transition(TransitionCommand {
            id: running.id,
            expected_version: running.state_version,
            expected_attempt: running.attempt,
            state: TaskState::Blocked { reason: "test".into() },
            output: None,
            assigned_resources: Vec::new(),
            retry_not_before_ms: None,
            cancel_requested: false,
        })
        .await
        .unwrap();
    drop(store);
    let service = TaskExecutionServiceBuilder::recoverable_sqlite(&path)
        .unwrap()
        .max_attempts(1)
        .build()
        .await
        .unwrap();
    let before = service.get(record.id).await.unwrap().unwrap();
    assert!(matches!(
        service.retry_blocked(record.id).await,
        Err(TaskServiceError::AttemptsExhausted { attempts: 1, limit: 1 })
    ));
    let after = service.get(record.id).await.unwrap().unwrap();
    assert_eq!(before.state_version, after.state_version);
    assert_eq!(before.attempt, after.attempt);
    assert_eq!(before.state, after.state);
    assert_eq!(blocked.attempt, after.attempt);
    service.shutdown().await.unwrap();
    drop(service);
    cleanup(&path);
}

#[derive(Clone, Copy)]
enum BadPage {
    Normal,
    EmptyWithNext,
    StuckCursor,
    TooManyRecords,
}

struct BadScanStore {
    inner: Arc<SqliteTaskStore>,
    mode: BadPage,
    stored: Mutex<Option<StoredTask>>,
    cursor: TaskId,
    precheck_calls: AtomicUsize,
    scan_calls: AtomicUsize,
}

impl TaskStore for BadScanStore {
    fn capabilities(&self) -> StoreCapabilities {
        self.inner.capabilities()
    }
    fn accept<'a>(&'a self, id: TaskId, request: TaskRequest) -> TaskFuture<'a, Result<AcceptOutcome, StoreError>> {
        self.inner.accept(id, request)
    }
    fn get_by_idempotency_key<'a>(
        &'a self,
        key: &'a str,
    ) -> TaskFuture<'a, Result<Option<qubit_task::model::TaskRecord>, StoreError>> {
        self.inner.get_by_idempotency_key(key)
    }
    fn transition<'a>(
        &'a self,
        command: TransitionCommand,
    ) -> TaskFuture<'a, Result<qubit_task::model::TaskSummary, StoreError>> {
        self.inner.transition(command)
    }

    fn get_summary<'a>(
        &'a self,
        id: TaskId,
    ) -> TaskFuture<'a, Result<Option<qubit_task::model::TaskSummary>, StoreError>> {
        self.inner.get_summary(id)
    }
    fn get<'a>(&'a self, id: TaskId) -> TaskFuture<'a, Result<Option<qubit_task::model::TaskRecord>, StoreError>> {
        self.inner.get(id)
    }
    fn list<'a>(&'a self, query: TaskQuery) -> TaskFuture<'a, Result<TaskPage, StoreError>> {
        self.inner.list(query)
    }
    fn count_states<'a>(&'a self) -> TaskFuture<'a, Result<TaskStateCounts, StoreError>> {
        self.inner.count_states()
    }
    fn prune_terminal_before<'a>(
        &'a self,
        _accepted_before_ms: u64,
        _max_rows: std::num::NonZeroUsize,
    ) -> TaskFuture<'a, Result<usize, StoreError>> {
        Box::pin(async { Err(StoreError::UnsupportedCapability) })
    }

    fn acquire_owner<'a>(&'a self) -> TaskFuture<'a, Result<OwnerEpoch, StoreError>> {
        self.inner.acquire_owner()
    }
    fn release_owner<'a>(&'a self, epoch: OwnerEpoch) -> TaskFuture<'a, Result<(), StoreError>> {
        self.inner.release_owner(epoch)
    }

    fn has_unfinished_over_limit<'a>(&'a self, limit: usize) -> TaskFuture<'a, Result<bool, StoreError>> {
        self.precheck_calls.fetch_add(1, Ordering::Relaxed);
        self.inner.has_unfinished_over_limit(limit)
    }

    fn scan_unfinished<'a>(&'a self, cursor: Option<TaskId>) -> TaskFuture<'a, Result<StoredTaskPage, StoreError>> {
        self.scan_calls.fetch_add(1, Ordering::Relaxed);
        Box::pin(async move {
            match self.mode {
                BadPage::Normal => self.inner.scan_unfinished(cursor).await,
                BadPage::EmptyWithNext => Ok(StoredTaskPage {
                    tasks: Vec::new(),
                    next: Some(self.cursor),
                }),
                BadPage::StuckCursor => {
                    if cursor.is_none() {
                        let page = self.inner.scan_unfinished(None).await?;
                        if let Some(task) = page.tasks.first() {
                            *self.stored.lock().unwrap() = Some(task.clone());
                        }
                    }
                    let task = self.stored.lock().unwrap().clone().ok_or(StoreError::NotFound)?;
                    Ok(StoredTaskPage {
                        tasks: vec![task],
                        next: Some(self.cursor),
                    })
                }
                BadPage::TooManyRecords => {
                    let page = self.inner.scan_unfinished(None).await?;
                    let task = page.tasks.first().cloned().ok_or(StoreError::NotFound)?;
                    Ok(StoredTaskPage {
                        tasks: vec![task; 257],
                        next: None,
                    })
                }
            }
        })
    }
}

#[tokio::test]
async fn test_invalid_recovery_pages_fail_without_looping() {
    for mode in [BadPage::EmptyWithNext, BadPage::StuckCursor, BadPage::TooManyRecords] {
        let path = temp_db();
        let inner = Arc::new(SqliteTaskStore::open(&path).unwrap());
        accept(&inner).await;
        let store = Arc::new(BadScanStore {
            inner,
            mode,
            stored: Mutex::new(None),
            cursor: TaskId::generate(),
            precheck_calls: AtomicUsize::new(0),
            scan_calls: AtomicUsize::new(0),
        });
        let result = TaskExecutionServiceBuilder::default()
            .store(store.clone())
            .require_recovery(true)
            .build()
            .await;
        assert!(matches!(result, Err(TaskServiceBuildError::InvalidRecoveryPage(_))));
        drop(store);
        let reopened = SqliteTaskStore::open(&path).unwrap();
        let epoch = reopened.acquire_owner().await.unwrap();
        reopened.release_owner(epoch).await.unwrap();
        drop(reopened);
        cleanup(&path);
    }
}

#[tokio::test]
async fn test_recovery_prechecks_once_then_scans_each_page_once() {
    let path = temp_db();
    let inner = Arc::new(SqliteTaskStore::open(&path).unwrap());
    for _ in 0..257 {
        accept(&inner).await;
    }
    let store = Arc::new(BadScanStore {
        inner,
        mode: BadPage::Normal,
        stored: Mutex::new(None),
        cursor: TaskId::generate(),
        precheck_calls: AtomicUsize::new(0),
        scan_calls: AtomicUsize::new(0),
    });
    let service = TaskExecutionServiceBuilder::default()
        .store(store.clone())
        .queue_capacity(255)
        .max_running_tasks(std::num::NonZeroUsize::new(2).unwrap())
        .require_recovery(true)
        .build()
        .await
        .unwrap();
    assert_eq!(store.precheck_calls.load(Ordering::Relaxed), 1);
    assert_eq!(store.scan_calls.load(Ordering::Relaxed), 2);
    assert_eq!(service.stats().await.unwrap().blocked, 257);
    service.shutdown().await.unwrap();
    drop(service);
    drop(store);
    cleanup(&path);
}

#[tokio::test]
async fn test_recovery_retries_running_attempt_below_limit() {
    let path = temp_db();
    let store = SqliteTaskStore::open(&path).unwrap();
    let record = accept(&store).await;
    let running = store
        .transition(TransitionCommand {
            id: record.id,
            expected_version: record.state_version,
            expected_attempt: record.attempt,
            state: TaskState::Running,
            output: None,
            assigned_resources: Vec::new(),
            retry_not_before_ms: None,
            cancel_requested: false,
        })
        .await
        .unwrap();
    drop(store);
    let service = TaskExecutionServiceBuilder::recoverable_sqlite(&path)
        .unwrap()
        .max_attempts(2)
        .register_handler(Arc::new(Echo))
        .unwrap()
        .build()
        .await
        .unwrap();
    let finished = service.wait(record.id).await.unwrap();
    assert_eq!(running.attempt, 1);
    assert_eq!(finished.attempt, 2);
    assert!(matches!(finished.state, TaskState::Succeeded));
    service.shutdown().await.unwrap();
    drop(service);
    cleanup(&path);
}

#[tokio::test]
async fn test_recovery_preserves_retry_deadline_for_queued_record() {
    let path = temp_db();
    let store = SqliteTaskStore::open(&path).unwrap();
    let accepted = accept(&store).await;
    let running = store
        .transition(TransitionCommand {
            id: accepted.id,
            expected_version: accepted.state_version,
            expected_attempt: accepted.attempt,
            state: TaskState::Running,
            retry_not_before_ms: None,
            output: None,
            assigned_resources: Vec::new(),
            cancel_requested: false,
        })
        .await
        .unwrap();
    let deadline = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        + 3_000;
    let queued = store
        .transition(TransitionCommand {
            id: running.id,
            expected_version: running.state_version,
            expected_attempt: running.attempt,
            state: TaskState::Queued,
            retry_not_before_ms: Some(deadline),
            output: None,
            assigned_resources: Vec::new(),
            cancel_requested: false,
        })
        .await
        .unwrap();
    let later = accept(&store).await;
    let later_running = store
        .transition(TransitionCommand {
            id: later.id,
            expected_version: later.state_version,
            expected_attempt: later.attempt,
            state: TaskState::Running,
            retry_not_before_ms: None,
            output: None,
            assigned_resources: Vec::new(),
            cancel_requested: false,
        })
        .await
        .unwrap();
    let later_deadline = deadline + 50;
    let later_queued = store
        .transition(TransitionCommand {
            id: later_running.id,
            expected_version: later_running.state_version,
            expected_attempt: later_running.attempt,
            state: TaskState::Queued,
            retry_not_before_ms: Some(later_deadline),
            output: None,
            assigned_resources: Vec::new(),
            cancel_requested: false,
        })
        .await
        .unwrap();
    drop(store);

    let service = TaskExecutionServiceBuilder::recoverable_sqlite(&path)
        .unwrap()
        .register_handler(Arc::new(Echo))
        .unwrap()
        .max_attempts(2)
        .build()
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let waiting = service.get(queued.id).await.unwrap().unwrap();
    assert_eq!(waiting.retry_not_before_ms, Some(deadline));
    assert_eq!(waiting.attempt, 1);
    let later_waiting = service.get(later_queued.id).await.unwrap().unwrap();
    assert_eq!(later_waiting.retry_not_before_ms, Some(later_deadline));
    assert_eq!(later_waiting.attempt, 1);
    let finished = tokio::time::timeout(std::time::Duration::from_secs(5), service.wait(queued.id))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(finished.attempt, 2);
    assert!(matches!(finished.state, TaskState::Succeeded));
    let later_finished = tokio::time::timeout(std::time::Duration::from_secs(5), service.wait(later_queued.id))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(later_finished.attempt, 2);
    assert!(matches!(later_finished.state, TaskState::Succeeded));
    service.shutdown().await.unwrap();
    drop(service);
    cleanup(&path);
}
