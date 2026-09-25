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
    let store = SqliteTaskStore::open(&path).unwrap();
    let mut ids = Vec::new();
    for _ in 0..4 {
        ids.push(accept(&store).await.id);
    }
    drop(store);

    let result = TaskExecutionServiceBuilder::recoverable_sqlite(&path)
        .unwrap()
        .queue_capacity(2)
        .max_running_tasks(std::num::NonZeroUsize::new(1).unwrap())
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
    EmptyWithNext,
    StuckCursor,
    TooManyRecords,
}

struct BadScanStore {
    inner: Arc<SqliteTaskStore>,
    mode: BadPage,
    stored: Mutex<Option<StoredTask>>,
    cursor: TaskId,
}

impl TaskStore for BadScanStore {
    fn capabilities(&self) -> StoreCapabilities {
        self.inner.capabilities()
    }
    fn accept<'a>(&'a self, id: TaskId, request: TaskRequest) -> TaskFuture<'a, Result<AcceptOutcome, StoreError>> {
        self.inner.accept(id, request)
    }
    fn find_idempotent<'a>(
        &'a self,
        request: TaskRequest,
    ) -> TaskFuture<'a, Result<Option<qubit_task::model::TaskRecord>, StoreError>> {
        self.inner.find_idempotent(request)
    }
    fn transition<'a>(
        &'a self,
        command: TransitionCommand,
    ) -> TaskFuture<'a, Result<qubit_task::model::TaskRecord, StoreError>> {
        self.inner.transition(command)
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

    fn scan_unfinished<'a>(&'a self, cursor: Option<TaskId>) -> TaskFuture<'a, Result<StoredTaskPage, StoreError>> {
        Box::pin(async move {
            match self.mode {
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
