// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::engine::EngineError;
use qubit_task::engine::ExecutionHandle;
use qubit_task::engine::PreparedExecution;
use qubit_task::engine::TaskExecutionEngine;
use qubit_task::handler::TaskContext;
use qubit_task::handler::TaskHandler;
#[cfg(feature = "sqlite")]
use qubit_task::handler::TaskRunOutcome;
use qubit_task::model::AcceptOutcome;
use qubit_task::model::OwnerEpoch;
use qubit_task::model::ResourceCapacity;
use qubit_task::model::ResourceRequest;
use qubit_task::model::ResourceSnapshot;
use qubit_task::model::StoreCapabilities;
use qubit_task::model::StoredTaskPage;
use qubit_task::model::TaskId;
use qubit_task::model::TaskOutput;
use qubit_task::model::TaskPage;
use qubit_task::model::TaskQuery;
use qubit_task::model::TaskRecord;
use qubit_task::model::TaskRequest;
use qubit_task::model::TaskStateCounts;
use qubit_task::model::TaskSummary;
use qubit_task::model::TransitionCommand;
use qubit_task::service::LocalTaskOutcome;
use qubit_task::service::LocalTaskResultError;
use qubit_task::service::TaskServiceError;
use qubit_task::store::MemoryTaskStore;
use qubit_task::store::StoreError;
use qubit_task::store::TaskFuture;
use qubit_task::store::TaskStore;

struct ActivationGateEngine {
    entered: parking_lot::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    release: tokio::sync::Semaphore,
}

impl TaskExecutionEngine for ActivationGateEngine {
    fn capacity(&self) -> ResourceSnapshot {
        ResourceSnapshot {
            capacity: ResourceCapacity {
                cpu_slots: 1,
                ..ResourceCapacity::default()
            },
            ..ResourceSnapshot::default()
        }
    }
    fn prepare<'a>(
        &'a self,
        id: TaskId,
        _request: ResourceRequest,
    ) -> TaskFuture<'a, Result<PreparedExecution, EngineError>> {
        Box::pin(async move { Ok(PreparedExecution::new(id, Vec::new(), || {})) })
    }
    fn activate<'a>(
        &'a self,
        _prepared: PreparedExecution,
        _handler: Arc<dyn TaskHandler>,
        _payload: Vec<u8>,
        _context: TaskContext,
    ) -> TaskFuture<'a, Result<ExecutionHandle, EngineError>> {
        Box::pin(async move {
            if let Some(sender) = self.entered.lock().take() {
                let _ = sender.send(());
            }
            self.release
                .acquire()
                .await
                .expect("activation gate remains open")
                .forget();
            Err(EngineError::Closed)
        })
    }
}

struct PanickingPrepareEngine;

impl TaskExecutionEngine for PanickingPrepareEngine {
    fn capacity(&self) -> ResourceSnapshot {
        ResourceSnapshot {
            capacity: ResourceCapacity {
                cpu_slots: 1,
                ..ResourceCapacity::default()
            },
            ..ResourceSnapshot::default()
        }
    }

    fn prepare<'a>(
        &'a self,
        _id: TaskId,
        _request: ResourceRequest,
    ) -> TaskFuture<'a, Result<PreparedExecution, EngineError>> {
        panic!("injected prepare panic");
    }

    fn activate<'a>(
        &'a self,
        _prepared: PreparedExecution,
        _handler: Arc<dyn TaskHandler>,
        _payload: Vec<u8>,
        _context: TaskContext,
    ) -> TaskFuture<'a, Result<ExecutionHandle, EngineError>> {
        unreachable!("prepare panic prevents activation");
    }
}

struct FailFirstGetStore {
    inner: Arc<dyn TaskStore>,
    should_fail_get: AtomicBool,
    fail_next_list: AtomicBool,
    release_owner_calls: std::sync::atomic::AtomicUsize,
}

impl FailFirstGetStore {
    fn new() -> Self {
        Self {
            inner: Arc::new(MemoryTaskStore::new(16)),
            should_fail_get: AtomicBool::new(true),
            fail_next_list: AtomicBool::new(false),
            release_owner_calls: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    #[cfg(feature = "sqlite")]
    fn with_inner(inner: Arc<dyn TaskStore>) -> Self {
        Self {
            inner,
            should_fail_get: AtomicBool::new(false),
            fail_next_list: AtomicBool::new(false),
            release_owner_calls: std::sync::atomic::AtomicUsize::new(0),
        }
    }
}

impl TaskStore for FailFirstGetStore {
    fn capabilities(&self) -> StoreCapabilities {
        self.inner.capabilities()
    }

    fn accept<'a>(&'a self, id: TaskId, request: TaskRequest) -> TaskFuture<'a, Result<AcceptOutcome, StoreError>> {
        self.inner.accept(id, request)
    }

    fn get_by_idempotency_key<'a>(&'a self, key: &'a str) -> TaskFuture<'a, Result<Option<TaskRecord>, StoreError>> {
        self.inner.get_by_idempotency_key(key)
    }

    fn transition<'a>(&'a self, command: TransitionCommand) -> TaskFuture<'a, Result<TaskSummary, StoreError>> {
        self.inner.transition(command)
    }

    fn get_summary<'a>(&'a self, id: TaskId) -> TaskFuture<'a, Result<Option<TaskSummary>, StoreError>> {
        self.inner.get_summary(id)
    }

    fn get<'a>(&'a self, id: TaskId) -> TaskFuture<'a, Result<Option<TaskRecord>, StoreError>> {
        if self.should_fail_get.swap(false, Ordering::AcqRel) {
            Box::pin(async { Err(StoreError::Failure("injected get failure".into())) })
        } else {
            self.inner.get(id)
        }
    }

    fn list<'a>(&'a self, query: TaskQuery) -> TaskFuture<'a, Result<TaskPage, StoreError>> {
        if self.fail_next_list.swap(false, Ordering::AcqRel) {
            Box::pin(async { Err(StoreError::Failure("injected list failure".into())) })
        } else {
            self.inner.list(query)
        }
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

    fn has_unfinished_over_limit<'a>(&'a self, limit: usize) -> TaskFuture<'a, Result<bool, StoreError>> {
        self.inner.has_unfinished_over_limit(limit)
    }

    fn scan_unfinished<'a>(&'a self, cursor: Option<TaskId>) -> TaskFuture<'a, Result<StoredTaskPage, StoreError>> {
        self.inner.scan_unfinished(cursor)
    }

    fn release_owner<'a>(&'a self, epoch: OwnerEpoch) -> TaskFuture<'a, Result<(), StoreError>> {
        self.release_owner_calls.fetch_add(1, Ordering::AcqRel);
        self.inner.release_owner(epoch)
    }
}

#[cfg(feature = "sqlite")]
struct NoopHandler;

#[cfg(feature = "sqlite")]
impl TaskHandler for NoopHandler {
    fn descriptor(&self) -> qubit_task::handler::TaskHandlerDescriptor {
        qubit_task::handler::TaskHandlerDescriptor {
            task_type: "owner-test".into(),
            version: "1".into(),
        }
    }
    fn run<'a>(
        &'a self,
        _payload: &'a [u8],
        _context: TaskContext,
    ) -> TaskFuture<'a, qubit_task::handler::TaskRunResult> {
        Box::pin(async { Ok(TaskRunOutcome::Succeeded(TaskOutput::default())) })
    }
}

#[tokio::test]
async fn test_store_fault_shutdown_waits_for_active_attempt_after_caller_timeout() {
    let store = Arc::new(FailFirstGetStore::new());
    store.should_fail_get.store(false, Ordering::Release);
    let service = TaskExecutionServiceBuilder::default()
        .store(store.clone())
        .build()
        .await
        .unwrap();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let handle = service
        .submit_local(move |_| {
            let _ = started_tx.send(());
            release_rx.recv().expect("test execution is released");
            LocalTaskOutcome::<(), std::io::Error>::Succeeded {
                value: (),
                summary: TaskOutput::default(),
            }
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), started_rx)
        .await
        .unwrap()
        .unwrap();

    store.fail_next_list.store(true, Ordering::Release);
    assert!(service.list(TaskQuery::default()).await.is_err());
    assert!(matches!(
        service
            .shutdown_until(tokio::time::Instant::now() + Duration::from_millis(30))
            .await,
        Err(TaskServiceError::ShutdownTimedOut)
    ));
    assert!(matches!(
        handle.result().await,
        Err(LocalTaskResultError::StoreUnavailable(_))
    ));
    release_tx.send(()).unwrap();
    assert!(matches!(
        service.shutdown().await,
        Err(TaskServiceError::StoreUnavailable(_))
    ));
}

#[tokio::test]
async fn test_store_fault_shutdown_waits_for_scheduler_activation_to_return() {
    let store = Arc::new(FailFirstGetStore::new());
    store.should_fail_get.store(false, Ordering::Release);
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let engine = Arc::new(ActivationGateEngine {
        entered: parking_lot::Mutex::new(Some(entered_tx)),
        release: tokio::sync::Semaphore::new(0),
    });
    let service = TaskExecutionServiceBuilder::default()
        .store(store.clone())
        .engine(engine.clone())
        .build()
        .await
        .unwrap();
    let handle = service
        .submit_local(|_| LocalTaskOutcome::<(), std::io::Error>::Succeeded {
            value: (),
            summary: TaskOutput::default(),
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), entered_rx)
        .await
        .unwrap()
        .unwrap();

    store.fail_next_list.store(true, Ordering::Release);
    assert!(service.list(TaskQuery::default()).await.is_err());
    assert!(matches!(
        service
            .shutdown_until(tokio::time::Instant::now() + Duration::from_millis(30))
            .await,
        Err(TaskServiceError::ShutdownTimedOut)
    ));
    engine.release.add_permits(1);
    assert!(matches!(
        service.shutdown().await,
        Err(TaskServiceError::StoreUnavailable(_))
    ));
    assert!(matches!(
        handle.result().await,
        Err(LocalTaskResultError::StoreUnavailable(_))
    ));
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_store_fault_keeps_sqlite_owner_until_scheduler_activation_stops() {
    let path = std::env::temp_dir().join(format!("qubit-task-fault-drain-{}.sqlite", TaskId::generate()));
    let sqlite = Arc::new(qubit_task::store::SqliteTaskStore::open(&path).unwrap());
    let store = Arc::new(FailFirstGetStore::with_inner(sqlite));
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let engine = Arc::new(ActivationGateEngine {
        entered: parking_lot::Mutex::new(Some(entered_tx)),
        release: tokio::sync::Semaphore::new(0),
    });
    let service = TaskExecutionServiceBuilder::default()
        .store(store.clone())
        .engine(engine.clone())
        .register_handler(Arc::new(NoopHandler))
        .unwrap()
        .build()
        .await
        .unwrap();
    let task = service
        .submit(TaskRequest::new("owner-test", "1", vec![1]).with_idempotency_key("owner-test-key"))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), entered_rx)
        .await
        .unwrap()
        .unwrap();

    store.fail_next_list.store(true, Ordering::Release);
    assert!(service.list(TaskQuery::default()).await.is_err());
    assert!(matches!(
        service
            .shutdown_until(tokio::time::Instant::now() + Duration::from_millis(30))
            .await,
        Err(TaskServiceError::ShutdownTimedOut)
    ));
    assert_eq!(store.release_owner_calls.load(Ordering::Acquire), 0);
    engine.release.add_permits(1);
    assert!(matches!(
        service.shutdown().await,
        Err(TaskServiceError::StoreUnavailable(_))
    ));
    assert_eq!(store.release_owner_calls.load(Ordering::Acquire), 1);
    drop(service);

    let reopened = qubit_task::store::SqliteTaskStore::open(&path).expect("owner lock was released after drain");
    drop(reopened);
    for suffix in ["", "-wal", "-shm", ".owner.lock"] {
        let file = if suffix == ".owner.lock" {
            path.with_extension("owner.lock")
        } else {
            std::path::PathBuf::from(format!("{}{suffix}", path.display()))
        };
        let _ = std::fs::remove_file(file);
    }
    let _ = task;
}

#[tokio::test]
async fn test_scheduler_store_failure_pauses_service_and_prevents_execution() {
    let service = TaskExecutionServiceBuilder::default()
        .store(Arc::new(FailFirstGetStore::new()))
        .build()
        .await
        .expect("service builds");
    let ran = Arc::new(AtomicBool::new(false));
    let handler_ran = Arc::clone(&ran);
    let handle = service
        .submit_local(move |_| {
            handler_ran.store(true, Ordering::Release);
            LocalTaskOutcome::<(), std::io::Error>::Succeeded {
                value: (),
                summary: TaskOutput::default(),
            }
        })
        .await
        .expect("task is accepted before scheduler reads it");

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if service.last_store_error().is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("scheduler records the storage failure");

    let result = tokio::time::timeout(Duration::from_secs(2), handle.result())
        .await
        .expect("typed handle receives the store fault");
    assert!(
        matches!(result, Err(LocalTaskResultError::StoreUnavailable(message)) if message.contains("injected get failure"))
    );

    let error = service
        .submit_local(|_| LocalTaskOutcome::<(), std::io::Error>::Succeeded {
            value: (),
            summary: TaskOutput::default(),
        })
        .await
        .expect_err("service rejects submissions after a store failure");
    assert!(matches!(error, TaskServiceError::StoreUnavailable(_)));
    assert!(!ran.load(Ordering::Acquire), "failed task handler must not run");
    assert!(matches!(
        service.shutdown().await,
        Err(TaskServiceError::StoreUnavailable(_))
    ));
}

#[tokio::test]
async fn test_engine_prepare_panic_is_reported_as_scheduler_unavailable() {
    let service = TaskExecutionServiceBuilder::default()
        .store(Arc::new(MemoryTaskStore::new(16)))
        .engine(Arc::new(PanickingPrepareEngine))
        .build()
        .await
        .expect("service builds");
    let handle = service
        .submit_local(|_| LocalTaskOutcome::<(), std::io::Error>::Succeeded {
            value: (),
            summary: TaskOutput::default(),
        })
        .await
        .expect("task is accepted before engine preparation");
    let id = handle.task_id();

    let result = tokio::time::timeout(Duration::from_secs(1), handle.result())
        .await
        .expect("local waiter is woken by scheduler failure");
    assert!(
        matches!(result, Err(LocalTaskResultError::Infrastructure(message)) if message.contains("injected prepare panic"))
    );
    assert!(matches!(
        service.wait(id).await,
        Err(TaskServiceError::SchedulerUnavailable(message)) if message.contains("injected prepare panic")
    ));
    assert!(matches!(
        service.shutdown().await,
        Err(TaskServiceError::SchedulerUnavailable(_))
    ));
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_recoverable_sqlite_store_rejects_local_closure_without_accepting_it() {
    let path = std::env::temp_dir().join(format!("qubit-task-local-submit-{}.sqlite", TaskId::generate()));
    let service = TaskExecutionServiceBuilder::recoverable_sqlite(&path)
        .expect("SQLite service builds")
        .build()
        .await
        .expect("service builds");

    let error = service
        .submit_local(|_| LocalTaskOutcome::<(), std::io::Error>::Succeeded {
            value: (),
            summary: TaskOutput::default(),
        })
        .await
        .expect_err("recoverable stores cannot retain process-local closures");
    assert!(matches!(error, TaskServiceError::UnsupportedCapability));
    let page = service
        .list(TaskQuery {
            limit: 10,
            ..TaskQuery::default()
        })
        .await
        .expect("history query succeeds");
    assert!(page.records.is_empty(), "rejected closure must not be stored");
    service.shutdown().await.expect("empty service shuts down");

    for suffix in ["", "-wal", "-shm", ".owner.lock"] {
        let file = if suffix == ".owner.lock" {
            path.with_extension("owner.lock")
        } else {
            std::path::PathBuf::from(format!("{}{suffix}", path.display()))
        };
        let _ = std::fs::remove_file(file);
    }
}
