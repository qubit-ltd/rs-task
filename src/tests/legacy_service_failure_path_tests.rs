// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

#[cfg(feature = "sqlite")]
use common::sqlite_paths;
use parking_lot::Mutex;
use tokio::sync;
use tokio::test as tokio_test;
use tokio::time;

#[cfg(feature = "sqlite")]
use super::common;
use crate::engine::EngineError;
use crate::engine::ExecutionHandle;
use crate::engine::PreparedExecution;
use crate::engine::TaskExecutionEngine;
use crate::handler::TaskContext;
use crate::handler::TaskHandler;
#[cfg(feature = "sqlite")]
use crate::handler::TaskHandlerDescriptor;
#[cfg(feature = "sqlite")]
use crate::handler::TaskRunOutcome;
#[cfg(feature = "sqlite")]
use crate::handler::TaskRunResult;
use crate::model::AcceptOutcome;
use crate::model::OwnerEpoch;
use crate::model::RecoveryPage;
use crate::model::ResourceCapacity;
use crate::model::ResourceRequest;
use crate::model::ResourceSnapshot;
use crate::model::StoreCapabilities;
use crate::model::TaskCursor;
use crate::model::TaskId;
use crate::model::TaskOutput;
use crate::model::TaskPage;
use crate::model::TaskQuery;
use crate::model::TaskRecord;
use crate::model::TaskRequest;
use crate::model::TaskState;
use crate::model::TaskStateCounts;
use crate::model::TaskSummary;
use crate::model::TransitionCommand;
use crate::service::LocalTaskOutcome;
use crate::service::LocalTaskResultError;
use crate::service::TaskServiceError;
use crate::service::task_execution_service_builder::TaskExecutionServiceBuilder;
use crate::store::LegacyTaskStore as TaskStore;
use crate::store::MemoryTaskStore;
#[cfg(feature = "sqlite")]
use crate::store::SqliteTaskStore;
use crate::store::StoreError;
use crate::store::TaskFuture;

struct ActivationGateEngine {
    entered: Mutex<Option<sync::oneshot::Sender<()>>>,
    release: sync::Semaphore,
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
    fn try_prepare(&self, id: TaskId, _request: ResourceRequest) -> Result<PreparedExecution, EngineError> {
        Ok(PreparedExecution::new(id, Vec::new(), || {}))
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

    fn try_prepare(&self, _id: TaskId, _request: ResourceRequest) -> Result<PreparedExecution, EngineError> {
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

struct ClosedPrepareEngine {
    prepare_calls: AtomicUsize,
}

impl TaskExecutionEngine for ClosedPrepareEngine {
    fn capacity(&self) -> ResourceSnapshot {
        ResourceSnapshot {
            capacity: ResourceCapacity {
                cpu_slots: 1,
                ..ResourceCapacity::default()
            },
            ..ResourceSnapshot::default()
        }
    }

    fn try_prepare(&self, _id: TaskId, _request: ResourceRequest) -> Result<PreparedExecution, EngineError> {
        self.prepare_calls.fetch_add(1, Ordering::AcqRel);
        Err(EngineError::Closed)
    }

    fn activate<'a>(
        &'a self,
        _prepared: PreparedExecution,
        _handler: Arc<dyn TaskHandler>,
        _payload: Vec<u8>,
        _context: TaskContext,
    ) -> TaskFuture<'a, Result<ExecutionHandle, EngineError>> {
        unreachable!("prepare returns Closed before activation")
    }
}

struct FailFirstGetStore {
    inner: Arc<dyn TaskStore>,
    should_fail_get: AtomicBool,
    should_fail_keyed_record_lookup: AtomicBool,
    fail_next_list: AtomicBool,
    panic_after_accept: AtomicBool,
    accept_started: Arc<sync::Notify>,
    override_summary_state: Mutex<Option<TaskState>>,
    release_owner_calls: std::sync::atomic::AtomicUsize,
}

impl FailFirstGetStore {
    fn new() -> Self {
        Self {
            inner: Arc::new(MemoryTaskStore::new(16)),
            should_fail_get: AtomicBool::new(true),
            should_fail_keyed_record_lookup: AtomicBool::new(false),
            fail_next_list: AtomicBool::new(false),
            panic_after_accept: AtomicBool::new(false),
            accept_started: Arc::new(sync::Notify::new()),
            override_summary_state: Mutex::new(None),
            release_owner_calls: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    #[cfg(feature = "sqlite")]
    fn with_inner(inner: Arc<dyn TaskStore>) -> Self {
        Self {
            inner,
            should_fail_get: AtomicBool::new(false),
            should_fail_keyed_record_lookup: AtomicBool::new(false),
            fail_next_list: AtomicBool::new(false),
            panic_after_accept: AtomicBool::new(false),
            accept_started: Arc::new(sync::Notify::new()),
            override_summary_state: Mutex::new(None),
            release_owner_calls: std::sync::atomic::AtomicUsize::new(0),
        }
    }
}

impl TaskStore for FailFirstGetStore {
    fn capabilities(&self) -> StoreCapabilities {
        self.inner.capabilities()
    }

    fn accept<'a>(&'a self, id: TaskId, request: TaskRequest) -> TaskFuture<'a, Result<AcceptOutcome, StoreError>> {
        let inner = Arc::clone(&self.inner);
        let panic_after_accept = self.panic_after_accept.swap(false, Ordering::AcqRel);
        let accept_started = Arc::clone(&self.accept_started);
        Box::pin(async move {
            accept_started.notify_one();
            let outcome = inner.accept(id, request).await?;
            if panic_after_accept {
                panic!("injected panic after task acceptance was committed");
            }
            Ok(outcome)
        })
    }

    fn get_by_idempotency_key<'a>(&'a self, key: &'a str) -> TaskFuture<'a, Result<Option<TaskRecord>, StoreError>> {
        if self.should_fail_keyed_record_lookup.swap(false, Ordering::AcqRel) {
            Box::pin(async { Err(StoreError::Failure("injected full keyed lookup failure".into())) })
        } else {
            self.inner.get_by_idempotency_key(key)
        }
    }

    fn get_summary_by_idempotency_key<'a>(
        &'a self,
        key: &'a str,
    ) -> TaskFuture<'a, Result<Option<TaskSummary>, StoreError>> {
        self.inner.get_summary_by_idempotency_key(key)
    }

    fn transition<'a>(&'a self, command: TransitionCommand) -> TaskFuture<'a, Result<TaskSummary, StoreError>> {
        self.inner.transition(command)
    }

    fn get_summary<'a>(&'a self, id: TaskId) -> TaskFuture<'a, Result<Option<TaskSummary>, StoreError>> {
        let state = self.override_summary_state.lock().clone();
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            let mut summary = inner.get_summary(id).await?;
            if let (Some(summary), Some(state)) = (&mut summary, state) {
                summary.state = state;
            }
            Ok(summary)
        })
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

    fn scan_unfinished<'a>(&'a self, cursor: Option<TaskCursor>) -> TaskFuture<'a, Result<RecoveryPage, StoreError>> {
        self.inner.scan_unfinished(cursor)
    }

    fn release_owner<'a>(&'a self, epoch: OwnerEpoch) -> TaskFuture<'a, Result<(), StoreError>> {
        self.release_owner_calls.fetch_add(1, Ordering::AcqRel);
        self.inner.release_owner(epoch)
    }
}

#[tokio_test]
async fn test_service_idempotency_summary_lookup_does_not_load_full_record() {
    let store = Arc::new(FailFirstGetStore::new());
    let service = TaskExecutionServiceBuilder::default()
        .store(store.clone())
        .build()
        .await
        .expect("service builds with the decorated store");
    let key = "summary-only-service-lookup";
    let accepted = service
        .submit(TaskRequest::new("missing-handler", "1", vec![1, 2, 3]).with_idempotency_key(key))
        .await
        .expect("task is accepted");
    store.should_fail_keyed_record_lookup.store(true, Ordering::Release);

    let summary = service
        .get_by_idempotency_key(key)
        .await
        .expect("summary lookup succeeds without a full record read")
        .expect("accepted task is still retained");
    assert_eq!(summary.id, accepted.id);
    assert_eq!(summary.request.idempotency_key.as_deref(), Some(key));
    assert!(matches!(
        store.get_by_idempotency_key(key).await,
        Err(StoreError::Failure(_))
    ));

    service.shutdown().await.expect("service shuts down");
}

#[cfg(feature = "sqlite")]
struct NoopHandler;

#[cfg(feature = "sqlite")]
impl TaskHandler for NoopHandler {
    fn descriptor(&self) -> TaskHandlerDescriptor {
        TaskHandlerDescriptor {
            task_type: "owner-test".into(),
            version: "1".into(),
        }
    }
    fn run<'a>(&'a self, _payload: &'a [u8], _context: TaskContext) -> TaskFuture<'a, TaskRunResult> {
        Box::pin(async { Ok(TaskRunOutcome::Succeeded(TaskOutput::default())) })
    }
}

#[tokio_test]
async fn test_store_fault_shutdown_waits_for_active_attempt_after_caller_timeout() {
    let store = Arc::new(FailFirstGetStore::new());
    store.should_fail_get.store(false, Ordering::Release);
    let service = TaskExecutionServiceBuilder::default()
        .store(store.clone())
        .build()
        .await
        .unwrap();
    let (started_tx, started_rx) = sync::oneshot::channel();
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
    time::timeout(Duration::from_secs(2), started_rx)
        .await
        .unwrap()
        .unwrap();

    store.fail_next_list.store(true, Ordering::Release);
    assert!(service.list(TaskQuery::default()).await.is_err());
    assert!(matches!(
        service
            .shutdown_until(time::Instant::now() + Duration::from_millis(30))
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

#[tokio_test]
async fn test_store_fault_shutdown_waits_for_scheduler_activation_to_return() {
    let store = Arc::new(FailFirstGetStore::new());
    store.should_fail_get.store(false, Ordering::Release);
    let (entered_tx, entered_rx) = sync::oneshot::channel();
    let engine = Arc::new(ActivationGateEngine {
        entered: Mutex::new(Some(entered_tx)),
        release: sync::Semaphore::new(0),
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
    time::timeout(Duration::from_secs(2), entered_rx)
        .await
        .unwrap()
        .unwrap();

    store.fail_next_list.store(true, Ordering::Release);
    assert!(service.list(TaskQuery::default()).await.is_err());
    assert!(matches!(
        service
            .shutdown_until(time::Instant::now() + Duration::from_millis(30))
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
#[tokio_test]
async fn test_store_fault_keeps_sqlite_owner_until_scheduler_activation_stops() {
    let path = std::env::temp_dir().join(format!("qubit-task-fault-drain-{}.sqlite", TaskId::generate()));
    let sqlite = Arc::new(SqliteTaskStore::open(&path).unwrap());
    let store = Arc::new(FailFirstGetStore::with_inner(sqlite));
    let (entered_tx, entered_rx) = sync::oneshot::channel();
    let engine = Arc::new(ActivationGateEngine {
        entered: Mutex::new(Some(entered_tx)),
        release: sync::Semaphore::new(0),
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
    time::timeout(Duration::from_secs(2), entered_rx)
        .await
        .unwrap()
        .unwrap();

    store.fail_next_list.store(true, Ordering::Release);
    assert!(service.list(TaskQuery::default()).await.is_err());
    assert!(matches!(
        service
            .shutdown_until(time::Instant::now() + Duration::from_millis(30))
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

    let reopened = SqliteTaskStore::open(&path).expect("owner lock was released after drain");
    drop(reopened);
    for suffix in ["", "-wal", "-shm", ".owner.lock"] {
        let file = if suffix == ".owner.lock" {
            sqlite_paths::owner_lock_path(&path)
        } else {
            std::path::PathBuf::from(format!("{}{suffix}", path.display()))
        };
        let _ = std::fs::remove_file(file);
    }
    let _ = task;
}

#[tokio_test]
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

    time::timeout(Duration::from_secs(2), async {
        loop {
            if service.last_store_error().is_some() {
                break;
            }
            time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("scheduler records the storage failure");

    let result = time::timeout(Duration::from_secs(2), handle.result())
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

#[tokio_test]
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

    let result = time::timeout(Duration::from_secs(1), handle.result())
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

#[tokio_test]
async fn test_engine_prepare_closed_stops_scheduler_and_preserves_queued_task() {
    let engine = Arc::new(ClosedPrepareEngine {
        prepare_calls: AtomicUsize::new(0),
    });
    let service = TaskExecutionServiceBuilder::default()
        .store(Arc::new(MemoryTaskStore::new(16)))
        .engine(engine.clone())
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

    let shutdown = time::timeout(Duration::from_secs(1), service.shutdown())
        .await
        .expect("permanently closed engine must not leave shutdown polling");
    assert!(matches!(shutdown, Err(TaskServiceError::SchedulerUnavailable(message)) if message.contains("closed")));
    assert_eq!(engine.prepare_calls.load(Ordering::Acquire), 1);
    assert!(matches!(
        service.get_summary(id).await.unwrap().unwrap().state,
        TaskState::Queued
    ));
    assert!(matches!(
        service.wait(id).await,
        Err(TaskServiceError::SchedulerUnavailable(message)) if message.contains("closed")
    ));
    assert!(matches!(
        service
            .submit_local(|_| LocalTaskOutcome::<(), std::io::Error>::Succeeded {
                value: (),
                summary: TaskOutput::default(),
            })
            .await,
        Err(TaskServiceError::SchedulerUnavailable(message)) if message.contains("closed")
    ));
    assert!(matches!(
        handle.result().await,
        Err(LocalTaskResultError::Infrastructure(message)) if message.contains("closed")
    ));
}

#[cfg(feature = "sqlite")]
#[tokio_test]
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
            sqlite_paths::owner_lock_path(&path)
        } else {
            std::path::PathBuf::from(format!("{}{suffix}", path.display()))
        };
        let _ = std::fs::remove_file(file);
    }
}

#[tokio_test]
async fn test_admission_worker_panic_after_accept_latches_fault_and_unblocks_shutdown() {
    let store = Arc::new(FailFirstGetStore::new());
    store.should_fail_get.store(false, Ordering::Release);
    store.panic_after_accept.store(true, Ordering::Release);
    let service = TaskExecutionServiceBuilder::default()
        .store(store.clone())
        .build()
        .await
        .expect("service builds");

    let error = service
        .submit(TaskRequest::new("panic-after-accept", "1", vec![]).with_idempotency_key("panic-after-accept"))
        .await
        .expect_err("a panicked worker cannot report successful acceptance");
    assert!(matches!(error, TaskServiceError::StoreUnavailable(ref message) if message.contains("panic")));
    assert!(
        service.last_store_error().is_some(),
        "worker panic must suspend the service"
    );
    assert!(matches!(
        time::timeout(Duration::from_secs(1), service.shutdown()).await,
        Ok(Err(TaskServiceError::StoreUnavailable(_)))
    ));
    assert!(
        store
            .inner
            .get_by_idempotency_key("panic-after-accept")
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio_test]
async fn test_admission_worker_panic_is_observed_after_submit_waiter_is_dropped() {
    let store = Arc::new(FailFirstGetStore::new());
    store.should_fail_get.store(false, Ordering::Release);
    store.panic_after_accept.store(true, Ordering::Release);
    let service = TaskExecutionServiceBuilder::default()
        .store(store.clone())
        .build()
        .await
        .expect("service builds");
    let accept_started = store.accept_started.notified();
    tokio::pin!(accept_started);
    accept_started.as_mut().enable();

    let submit_service = service.clone();
    let submit = tokio::spawn(async move {
        submit_service
            .submit(TaskRequest::new("cancelled-waiter", "1", vec![]).with_idempotency_key("cancelled-waiter"))
            .await
    });
    time::timeout(Duration::from_secs(1), accept_started)
        .await
        .expect("detached worker entered store acceptance");
    submit.abort();
    let _ = submit.await;

    time::timeout(Duration::from_secs(1), async {
        while service.last_store_error().is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("detached worker panic must latch a service fault without its caller");
    assert!(matches!(
        time::timeout(Duration::from_secs(1), service.shutdown()).await,
        Ok(Err(TaskServiceError::StoreUnavailable(_)))
    ));
}

#[tokio_test]
async fn test_retry_blocked_non_blocked_state() {
    let store = Arc::new(FailFirstGetStore::new());
    let service = TaskExecutionServiceBuilder::in_memory()
        .store(store.clone())
        .build()
        .await
        .expect("service builds");
    let record = service
        .submit(TaskRequest::new("unhandled", "1", Vec::new()).with_idempotency_key("retry-state"))
        .await
        .expect("request is accepted");
    time::timeout(Duration::from_secs(1), async {
        loop {
            if matches!(
                service
                    .get_summary(record.id)
                    .await
                    .expect("summary read")
                    .unwrap()
                    .state,
                TaskState::Blocked { .. }
            ) {
                break;
            }
            time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("missing handler becomes blocked");
    for (state, expected) in [
        (TaskState::Queued, crate::model::TaskStateKind::Queued),
        (TaskState::Running, crate::model::TaskStateKind::Running),
        (TaskState::Succeeded, crate::model::TaskStateKind::Succeeded),
        (
            TaskState::Failed {
                category: "test".into(),
                message: "failed".into(),
            },
            crate::model::TaskStateKind::Failed,
        ),
    ] {
        *store.override_summary_state.lock() = Some(state);
        assert!(matches!(
            service.retry_blocked(record.id).await,
            Err(TaskServiceError::NotBlocked { actual }) if actual == expected
        ));
    }
    service.shutdown().await.expect("service shuts down");
}
