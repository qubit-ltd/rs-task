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

use parking_lot::Mutex;
use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::engine::EngineError;
use qubit_task::engine::ExecutionHandle;
use qubit_task::engine::LocalTaskExecutionEngine;
use qubit_task::engine::PreparedExecution;
use qubit_task::engine::TaskExecutionEngine;
use qubit_task::handler::TaskContext;
use qubit_task::handler::TaskHandler;
use qubit_task::handler::TaskHandlerDescriptor;
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
use qubit_task::model::TaskRunError;
use qubit_task::model::TaskState;
use qubit_task::model::TaskStateCounts;
use qubit_task::model::TransitionCommand;
use qubit_task::service::LocalTaskOutcome;
use qubit_task::service::LocalTaskResultError;
use qubit_task::service::TaskServiceError;
use qubit_task::store::MemoryTaskStore;
use qubit_task::store::StoreError;
use qubit_task::store::TaskFuture;
use qubit_task::store::TaskStore;
use tokio::sync::Semaphore;
use tokio::sync::oneshot;

struct ControlledStore {
    inner: Arc<MemoryTaskStore>,
    accept_entered: Mutex<Option<oneshot::Sender<()>>>,
    accept_release: Arc<Semaphore>,
    detached_accept: bool,
    get_entered: Mutex<Option<oneshot::Sender<()>>>,
    get_release: Semaphore,
    get_calls: AtomicUsize,
    fail_get_on_call: AtomicUsize,
    get_failed: Mutex<Option<oneshot::Sender<()>>>,
    recoverable: bool,
    release_count: AtomicUsize,
    fail_next_get: AtomicBool,
    fail_next_statistics: AtomicBool,
    fail_next_block_transition: AtomicBool,
    block_transition_failed: Mutex<Option<oneshot::Sender<()>>>,
    block_transition_entered: Mutex<Option<oneshot::Sender<()>>>,
    block_transition_release: Arc<Semaphore>,
    running_transition_entered: Mutex<Option<oneshot::Sender<()>>>,
    running_transition_release: Arc<Semaphore>,
    fail_release: bool,
}

impl ControlledStore {
    fn new() -> Self {
        Self {
            inner: Arc::new(MemoryTaskStore::new(16)),
            accept_entered: Mutex::new(None),
            accept_release: Arc::new(Semaphore::new(0)),
            detached_accept: false,
            get_entered: Mutex::new(None),
            get_release: Semaphore::new(0),
            get_calls: AtomicUsize::new(0),
            fail_get_on_call: AtomicUsize::new(0),
            get_failed: Mutex::new(None),
            recoverable: false,
            release_count: AtomicUsize::new(0),
            fail_next_get: AtomicBool::new(false),
            fail_next_statistics: AtomicBool::new(false),
            fail_next_block_transition: AtomicBool::new(false),
            block_transition_failed: Mutex::new(None),
            block_transition_entered: Mutex::new(None),
            block_transition_release: Arc::new(Semaphore::new(0)),
            running_transition_entered: Mutex::new(None),
            running_transition_release: Arc::new(Semaphore::new(0)),
            fail_release: false,
        }
    }
}

impl TaskStore for ControlledStore {
    fn capabilities(&self) -> StoreCapabilities {
        StoreCapabilities {
            persistent_history: self.recoverable,
            restart_recovery: self.recoverable,
        }
    }

    fn accept<'a>(&'a self, id: TaskId, request: TaskRequest) -> TaskFuture<'a, Result<AcceptOutcome, StoreError>> {
        let signal = self.accept_entered.lock().take();
        if self.detached_accept {
            let inner = Arc::clone(&self.inner);
            let release = Arc::clone(&self.accept_release);
            let (sender, receiver) = oneshot::channel();
            tokio::spawn(async move {
                if let Some(signal) = signal {
                    let _ = signal.send(());
                    release
                        .acquire()
                        .await
                        .expect("detached accept gate stays open")
                        .forget();
                }
                let _ = sender.send(inner.accept(id, request).await);
            });
            return Box::pin(async move {
                receiver
                    .await
                    .map_err(|_| StoreError::Failure("detached accept worker stopped".into()))?
            });
        }
        Box::pin(async move {
            if let Some(signal) = signal {
                let _ = signal.send(());
                self.accept_release
                    .acquire()
                    .await
                    .expect("test accept gate stays open")
                    .forget();
            }
            self.inner.accept(id, request).await
        })
    }

    fn find_idempotent<'a>(&'a self, request: TaskRequest) -> TaskFuture<'a, Result<Option<TaskRecord>, StoreError>> {
        self.inner.find_idempotent(request)
    }

    fn transition<'a>(&'a self, command: TransitionCommand) -> TaskFuture<'a, Result<TaskRecord, StoreError>> {
        if matches!(command.state, TaskState::Blocked { .. })
            && self.fail_next_block_transition.swap(false, Ordering::AcqRel)
        {
            let signal = self.block_transition_failed.lock().take();
            Box::pin(async move {
                if let Some(signal) = signal {
                    let _ = signal.send(());
                }
                Err(StoreError::Failure("injected blocked transition failure".into()))
            })
        } else if matches!(command.state, TaskState::Blocked { .. }) {
            if let Some(signal) = self.block_transition_entered.lock().take() {
                return Box::pin(async move {
                    let _ = signal.send(());
                    self.block_transition_release
                        .acquire()
                        .await
                        .expect("block transition gate stays open")
                        .forget();
                    self.inner.transition(command).await
                });
            }
            self.inner.transition(command)
        } else if matches!(command.state, TaskState::Running) {
            if let Some(signal) = self.running_transition_entered.lock().take() {
                return Box::pin(async move {
                    let _ = signal.send(());
                    self.running_transition_release
                        .acquire()
                        .await
                        .expect("running transition gate stays open")
                        .forget();
                    self.inner.transition(command).await
                });
            }
            self.inner.transition(command)
        } else {
            self.inner.transition(command)
        }
    }

    fn get<'a>(&'a self, id: TaskId) -> TaskFuture<'a, Result<Option<TaskRecord>, StoreError>> {
        let signal = self.get_entered.lock().take();
        let call = self.get_calls.fetch_add(1, Ordering::AcqRel) + 1;
        let fail =
            self.fail_next_get.swap(false, Ordering::AcqRel) || self.fail_get_on_call.load(Ordering::Acquire) == call;
        let failed_signal = if fail { self.get_failed.lock().take() } else { None };
        Box::pin(async move {
            if let Some(signal) = signal {
                let _ = signal.send(());
                self.get_release
                    .acquire()
                    .await
                    .expect("test get gate stays open")
                    .forget();
            }
            if fail {
                if let Some(signal) = failed_signal {
                    let _ = signal.send(());
                }
                Err(StoreError::Failure("injected scheduler get failure".into()))
            } else {
                self.inner.get(id).await
            }
        })
    }

    fn list<'a>(&'a self, query: TaskQuery) -> TaskFuture<'a, Result<TaskPage, StoreError>> {
        if self.fail_next_statistics.swap(false, Ordering::AcqRel) {
            Box::pin(async { Err(StoreError::Failure("injected statistics failure".into())) })
        } else {
            self.inner.list(query)
        }
    }

    fn count_states<'a>(&'a self) -> TaskFuture<'a, Result<TaskStateCounts, StoreError>> {
        if self.fail_next_statistics.swap(false, Ordering::AcqRel) {
            Box::pin(async { Err(StoreError::Failure("injected statistics failure".into())) })
        } else {
            self.inner.count_states()
        }
    }

    fn acquire_owner<'a>(&'a self) -> TaskFuture<'a, Result<OwnerEpoch, StoreError>> {
        Box::pin(async { Ok(OwnerEpoch(1)) })
    }

    fn scan_unfinished<'a>(&'a self, _cursor: Option<TaskId>) -> TaskFuture<'a, Result<StoredTaskPage, StoreError>> {
        Box::pin(async { Ok(StoredTaskPage::default()) })
    }

    fn release_owner<'a>(&'a self, _epoch: OwnerEpoch) -> TaskFuture<'a, Result<(), StoreError>> {
        self.release_count.fetch_add(1, Ordering::AcqRel);
        Box::pin(async move {
            if self.fail_release {
                Err(StoreError::Failure("injected owner release failure".into()))
            } else {
                Ok(())
            }
        })
    }
}

struct RetryAfterClosingHandler {
    attempts: AtomicUsize,
    started: Mutex<Option<oneshot::Sender<()>>>,
    resume: Mutex<Option<oneshot::Receiver<()>>>,
}

struct RejectActivationEngine {
    inner: LocalTaskExecutionEngine,
}

impl TaskExecutionEngine for RejectActivationEngine {
    fn capacity(&self) -> ResourceSnapshot {
        self.inner.capacity()
    }

    fn prepare<'a>(
        &'a self,
        id: TaskId,
        request: ResourceRequest,
    ) -> TaskFuture<'a, Result<PreparedExecution, EngineError>> {
        self.inner.prepare(id, request)
    }

    fn activate<'a>(
        &'a self,
        _prepared: PreparedExecution,
        _handler: Arc<dyn TaskHandler>,
        _payload: Vec<u8>,
        _context: TaskContext,
    ) -> TaskFuture<'a, Result<ExecutionHandle, EngineError>> {
        Box::pin(async { Err(EngineError::Closed) })
    }
}

impl TaskHandler for RetryAfterClosingHandler {
    fn descriptor(&self) -> TaskHandlerDescriptor {
        TaskHandlerDescriptor {
            task_type: "retry-after-close".into(),
            version: "1".into(),
        }
    }

    fn run<'a>(
        &'a self,
        _payload: &'a [u8],
        _context: TaskContext,
    ) -> TaskFuture<'a, qubit_task::handler::TaskRunResult> {
        if self.attempts.fetch_add(1, Ordering::AcqRel) == 0 {
            let started = self.started.lock().take().expect("first attempt has start signal");
            let resume = self.resume.lock().take().expect("first attempt has resume signal");
            Box::pin(async move {
                let _ = started.send(());
                resume.await.expect("first attempt is resumed");
                Err(TaskRunError {
                    category: "transient".into(),
                    message: "try again".into(),
                    retryable: true,
                })
            })
        } else {
            Box::pin(async { Ok(TaskRunOutcome::Succeeded(TaskOutput::default())) })
        }
    }
}

#[tokio::test]
async fn test_shutdown_waits_for_inflight_acceptance_and_rejects_new_admission() {
    let (entered_tx, entered_rx) = oneshot::channel();
    let store = Arc::new(ControlledStore {
        accept_entered: Mutex::new(Some(entered_tx)),
        ..ControlledStore::new()
    });
    let service = TaskExecutionServiceBuilder::default()
        .store(store.clone())
        .build()
        .await
        .expect("service builds");

    let submitting_service = service.clone();
    let submission = tokio::spawn(async move {
        submitting_service
            .submit_local(|_| LocalTaskOutcome::<(), std::io::Error>::Succeeded {
                value: (),
                summary: TaskOutput::default(),
            })
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), entered_rx)
        .await
        .expect("accept operation was entered")
        .expect("accept entry was signalled");

    let closing_service = service.clone();
    let closing = tokio::spawn(async move { closing_service.shutdown().await });
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match service.retry_blocked(TaskId::generate()).await {
                Err(TaskServiceError::ShuttingDown) => break,
                Err(TaskServiceError::Store(StoreError::NotFound)) => tokio::task::yield_now().await,
                result => panic!("unexpected retry result while closing: {result:?}"),
            }
        }
    })
    .await
    .expect("shutdown closes admission");

    assert!(matches!(
        service.submit(TaskRequest::new("new", "1", Vec::new())).await,
        Err(TaskServiceError::ShuttingDown)
    ));
    assert!(matches!(
        service
            .submit_local(|_| LocalTaskOutcome::<(), std::io::Error>::Succeeded {
                value: (),
                summary: TaskOutput::default()
            })
            .await,
        Err(TaskServiceError::ShuttingDown)
    ));
    assert!(
        !closing.is_finished(),
        "shutdown must wait for the in-flight acceptance"
    );

    store.accept_release.add_permits(1);
    let id = tokio::time::timeout(Duration::from_secs(2), submission)
        .await
        .expect("old submission completes")
        .expect("submission task joins")
        .expect("old submission is accepted")
        .task_id();
    tokio::time::timeout(Duration::from_secs(2), closing)
        .await
        .expect("shutdown completes after accepted work")
        .expect("shutdown task joins")
        .expect("shutdown succeeds");
    let record = service
        .get(id)
        .await
        .expect("record remains readable")
        .expect("accepted record exists");
    assert!(
        record.state.is_terminal(),
        "accepted work must settle before shutdown returns"
    );
}

#[tokio::test]
async fn test_concurrent_shutdown_releases_owner_once() {
    let store = Arc::new(ControlledStore {
        recoverable: true,
        ..ControlledStore::new()
    });
    let service = TaskExecutionServiceBuilder::default()
        .store(store.clone())
        .build()
        .await
        .expect("recoverable service builds");

    let (first, second) = tokio::join!(service.shutdown(), service.shutdown());
    first.expect("first shutdown succeeds");
    second.expect("second shutdown succeeds");
    assert_eq!(store.release_count.load(Ordering::Acquire), 1);
}

#[tokio::test]
async fn test_shutdown_continues_after_first_caller_is_cancelled() {
    let (entered_tx, entered_rx) = oneshot::channel();
    let store = Arc::new(ControlledStore {
        accept_entered: Mutex::new(Some(entered_tx)),
        ..ControlledStore::new()
    });
    let service = TaskExecutionServiceBuilder::default()
        .store(store.clone())
        .build()
        .await
        .expect("service builds");
    let submitting_service = service.clone();
    let submission = tokio::spawn(async move {
        submitting_service
            .submit_local(|_| LocalTaskOutcome::<(), std::io::Error>::Succeeded {
                value: (),
                summary: TaskOutput::default(),
            })
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), entered_rx)
        .await
        .expect("accept operation was entered")
        .expect("accept entry was signalled");

    let closing_service = service.clone();
    let first_close = tokio::spawn(async move { closing_service.shutdown().await });
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match service.retry_blocked(TaskId::generate()).await {
                Err(TaskServiceError::ShuttingDown) => break,
                Err(TaskServiceError::Store(StoreError::NotFound)) => tokio::task::yield_now().await,
                result => panic!("unexpected retry result while closing: {result:?}"),
            }
        }
    })
    .await
    .expect("first shutdown closes admission");
    first_close.abort();
    let _ = first_close.await.expect_err("first shutdown caller was cancelled");

    store.accept_release.add_permits(1);
    let id = tokio::time::timeout(Duration::from_secs(2), submission)
        .await
        .expect("submission finishes")
        .expect("submission task joins")
        .expect("old task remains accepted")
        .task_id();
    tokio::time::timeout(Duration::from_secs(2), service.shutdown())
        .await
        .expect("another shutdown caller observes completed coordination")
        .expect("shutdown succeeds");
    assert!(
        service
            .get(id)
            .await
            .expect("record loads")
            .expect("record exists")
            .state
            .is_terminal(),
        "accepted work settles after the first caller cancels"
    );
}

#[tokio::test]
async fn test_aborted_submission_keeps_permit_until_detached_store_accept_finishes() {
    let (entered_tx, entered_rx) = oneshot::channel();
    let store = Arc::new(ControlledStore {
        accept_entered: Mutex::new(Some(entered_tx)),
        detached_accept: true,
        ..ControlledStore::new()
    });
    let service = TaskExecutionServiceBuilder::default()
        .store(store.clone())
        .build()
        .await
        .expect("service builds");
    let submitting_service = service.clone();
    let submission = tokio::spawn(async move {
        submitting_service
            .submit_local(|_| LocalTaskOutcome::<(), std::io::Error>::Succeeded {
                value: (),
                summary: TaskOutput::default(),
            })
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), entered_rx)
        .await
        .expect("detached store worker enters accept")
        .expect("accept entry signalled");
    submission.abort();
    let _ = submission.await.expect_err("submission caller was cancelled");

    let closing_service = service.clone();
    let mut closing = tokio::spawn(async move { closing_service.shutdown().await });
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut closing)
            .await
            .is_err(),
        "shutdown must wait while the detached store worker can still commit"
    );
    store.accept_release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(2), closing)
        .await
        .expect("shutdown finishes after detached accept and scheduling")
        .expect("shutdown task joins")
        .expect("shutdown succeeds");
    assert_eq!(service.stats().await.expect("stats load").terminal, 1);
}

#[tokio::test]
async fn test_shutdown_keeps_scheduler_running_for_retry_after_close() {
    let (started_tx, started_rx) = oneshot::channel();
    let (resume_tx, resume_rx) = oneshot::channel();
    let handler = Arc::new(RetryAfterClosingHandler {
        attempts: AtomicUsize::new(0),
        started: Mutex::new(Some(started_tx)),
        resume: Mutex::new(Some(resume_rx)),
    });
    let service = TaskExecutionServiceBuilder::in_memory()
        .register_handler(handler.clone())
        .expect("handler registers")
        .build()
        .await
        .expect("service builds");
    let accepted = service
        .submit(TaskRequest::new("retry-after-close", "1", Vec::new()))
        .await
        .expect("task accepted");
    tokio::time::timeout(Duration::from_secs(2), started_rx)
        .await
        .expect("first attempt starts")
        .expect("start signal arrives");

    let closing_service = service.clone();
    let closing = tokio::spawn(async move { closing_service.shutdown().await });
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match service.retry_blocked(TaskId::generate()).await {
                Err(TaskServiceError::ShuttingDown) => break,
                Err(TaskServiceError::Store(StoreError::NotFound)) => tokio::task::yield_now().await,
                result => panic!("unexpected retry result while closing: {result:?}"),
            }
        }
    })
    .await
    .expect("shutdown closes admission");
    resume_tx.send(()).expect("first attempt awaits resume");

    tokio::time::timeout(Duration::from_secs(2), closing)
        .await
        .expect("shutdown waits for automatic retry")
        .expect("shutdown task joins")
        .expect("shutdown succeeds");
    let finished = service
        .get(accepted.id)
        .await
        .expect("record loads")
        .expect("record exists");
    assert_eq!(finished.state, TaskState::Succeeded);
    assert_eq!(handler.attempts.load(Ordering::Acquire), 2);
}

#[tokio::test]
async fn test_store_fault_wakes_waiter_with_diagnostic() {
    let store = Arc::new(ControlledStore {
        fail_next_get: AtomicBool::new(true),
        ..ControlledStore::new()
    });
    let service = TaskExecutionServiceBuilder::default()
        .store(store)
        .build()
        .await
        .expect("service builds");
    let id = service
        .submit_local(|_| LocalTaskOutcome::<(), std::io::Error>::Succeeded {
            value: (),
            summary: TaskOutput::default(),
        })
        .await
        .expect("task accepted")
        .task_id();
    tokio::time::timeout(Duration::from_secs(2), async {
        while service.last_store_error().is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("scheduler fault recorded");

    let error = tokio::time::timeout(Duration::from_secs(2), service.wait(id))
        .await
        .expect("wait resolves after store fault")
        .expect_err("wait returns store fault");
    assert!(
        matches!(error, TaskServiceError::StoreUnavailable(message) if message.contains("injected scheduler get failure"))
    );
}

#[tokio::test]
async fn test_failed_queued_to_blocked_transition_pauses_service() {
    let (failed_tx, failed_rx) = oneshot::channel();
    let store = Arc::new(ControlledStore {
        fail_next_block_transition: AtomicBool::new(true),
        block_transition_failed: Mutex::new(Some(failed_tx)),
        ..ControlledStore::new()
    });
    let service = TaskExecutionServiceBuilder::default()
        .store(store)
        .build()
        .await
        .expect("service builds");
    let accepted = service
        .submit(TaskRequest::new("missing-handler", "1", Vec::new()))
        .await
        .expect("task accepted before scheduler blocks it");
    tokio::time::timeout(Duration::from_secs(2), failed_rx)
        .await
        .expect("scheduler attempts blocked transition")
        .expect("transition failure signalled");

    let error = tokio::time::timeout(Duration::from_secs(2), service.wait(accepted.id))
        .await
        .expect("wait resolves after blocked transition failure")
        .expect_err("wait reports store fault");
    assert!(
        matches!(error, TaskServiceError::StoreUnavailable(message) if message.contains("injected blocked transition failure"))
    );
    assert!(service.last_store_error().is_some());
    assert!(matches!(
        service.shutdown().await,
        Err(TaskServiceError::StoreUnavailable(_))
    ));
}

#[tokio::test]
async fn test_failed_post_activation_get_pauses_service() {
    let (failed_tx, failed_rx) = oneshot::channel();
    let store = Arc::new(ControlledStore {
        fail_get_on_call: AtomicUsize::new(2),
        get_failed: Mutex::new(Some(failed_tx)),
        ..ControlledStore::new()
    });
    let service = TaskExecutionServiceBuilder::default()
        .store(store)
        .build()
        .await
        .expect("service builds");
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    let id = service
        .submit_local(move |_| {
            resume_rx.recv().expect("test handler is released");
            LocalTaskOutcome::<(), std::io::Error>::Succeeded {
                value: (),
                summary: TaskOutput::default(),
            }
        })
        .await
        .expect("task accepted")
        .task_id();
    tokio::time::timeout(Duration::from_secs(2), failed_rx)
        .await
        .expect("post-activation get is attempted")
        .expect("get failure signalled");

    let error = tokio::time::timeout(Duration::from_secs(2), service.wait(id))
        .await
        .expect("wait resolves after post-activation get failure")
        .expect_err("wait reports store fault");
    assert!(
        matches!(error, TaskServiceError::StoreUnavailable(message) if message.contains("injected scheduler get failure"))
    );
    resume_tx.send(()).expect("handler is released");
}

#[tokio::test]
async fn test_evicted_cancelled_task_does_not_pause_scheduler() {
    let (get_entered_tx, get_entered_rx) = oneshot::channel();
    let store = Arc::new(ControlledStore {
        inner: Arc::new(MemoryTaskStore::new(0)),
        get_entered: Mutex::new(Some(get_entered_tx)),
        ..ControlledStore::new()
    });
    let service = TaskExecutionServiceBuilder::default()
        .store(store.clone())
        .build()
        .await
        .expect("service builds");
    let cancelled_id = service
        .submit_local::<_, (), std::io::Error>(|_| panic!("cancelled task must not run"))
        .await
        .expect("task accepted")
        .task_id();
    tokio::time::timeout(Duration::from_secs(2), get_entered_rx)
        .await
        .expect("scheduler enters its first get")
        .expect("scheduler get entry signalled");
    assert_eq!(
        service.cancel(cancelled_id).await.expect("queued task cancels"),
        qubit_task::service::CancelOutcome::CancelledBeforeStart
    );
    store.get_release.add_permits(1);

    let (started_tx, started_rx) = oneshot::channel();
    service
        .submit_local(move |_| {
            let _ = started_tx.send(());
            LocalTaskOutcome::<(), std::io::Error>::Succeeded {
                value: (),
                summary: TaskOutput::default(),
            }
        })
        .await
        .expect("service still admits work after terminal record eviction");
    tokio::time::timeout(Duration::from_secs(2), started_rx)
        .await
        .expect("scheduler continues after evicted cancellation")
        .expect("replacement handler starts");
    service.shutdown().await.expect("service shuts down normally");
    assert!(service.last_store_error().is_none());
}

#[tokio::test]
async fn test_evicted_cancel_racing_blocked_transition_does_not_pause_service() {
    let (entered_tx, entered_rx) = oneshot::channel();
    let store = Arc::new(ControlledStore {
        inner: Arc::new(MemoryTaskStore::new(0)),
        block_transition_entered: Mutex::new(Some(entered_tx)),
        ..ControlledStore::new()
    });
    let service = TaskExecutionServiceBuilder::default()
        .store(store.clone())
        .build()
        .await
        .expect("service builds");
    let accepted = service
        .submit(TaskRequest::new("missing-handler", "1", Vec::new()))
        .await
        .expect("task accepted");
    tokio::time::timeout(Duration::from_secs(2), entered_rx)
        .await
        .expect("scheduler begins blocked transition")
        .expect("blocked transition signalled");
    assert_eq!(
        service.cancel(accepted.id).await.expect("queued task cancels"),
        qubit_task::service::CancelOutcome::CancelledBeforeStart
    );
    store.block_transition_release.add_permits(1);

    let (started_tx, started_rx) = oneshot::channel();
    service
        .submit_local(move |_| {
            let _ = started_tx.send(());
            LocalTaskOutcome::<(), std::io::Error>::Succeeded {
                value: (),
                summary: TaskOutput::default(),
            }
        })
        .await
        .expect("service remains open after the normal version conflict");
    tokio::time::timeout(Duration::from_secs(2), started_rx)
        .await
        .expect("scheduler continues after version conflict")
        .expect("later task starts");
    assert!(
        service
            .get(accepted.id)
            .await
            .expect("cancelled task lookup succeeds")
            .is_none()
    );
    service.shutdown().await.expect("service shuts down normally");
    assert!(service.last_store_error().is_none());
}

#[tokio::test]
async fn test_evicted_cancel_racing_running_transition_does_not_pause_service() {
    let (entered_tx, entered_rx) = oneshot::channel();
    let store = Arc::new(ControlledStore {
        inner: Arc::new(MemoryTaskStore::new(0)),
        running_transition_entered: Mutex::new(Some(entered_tx)),
        ..ControlledStore::new()
    });
    let service = TaskExecutionServiceBuilder::default()
        .store(store.clone())
        .build()
        .await
        .expect("service builds");
    let cancelled_id = service
        .submit_local::<_, (), std::io::Error>(|_| panic!("cancelled task must not run"))
        .await
        .expect("task accepted")
        .task_id();
    tokio::time::timeout(Duration::from_secs(2), entered_rx)
        .await
        .expect("scheduler begins Running transition")
        .expect("Running transition entry signalled");
    assert_eq!(
        service.cancel(cancelled_id).await.expect("queued task cancels"),
        qubit_task::service::CancelOutcome::CancelledBeforeStart
    );
    store.running_transition_release.add_permits(1);

    let (started_tx, started_rx) = oneshot::channel();
    service
        .submit_local(move |_| {
            let _ = started_tx.send(());
            LocalTaskOutcome::<(), std::io::Error>::Succeeded {
                value: (),
                summary: TaskOutput::default(),
            }
        })
        .await
        .expect("service remains open after evicted cancellation");
    tokio::time::timeout(Duration::from_secs(2), started_rx)
        .await
        .expect("scheduler executes later task")
        .expect("later task starts");
    service.shutdown().await.expect("service shuts down normally");
    assert!(service.last_store_error().is_none());
}

#[tokio::test]
async fn test_activation_failure_retries_blocked_transition_after_cancel_conflict() {
    let (entered_tx, entered_rx) = oneshot::channel();
    let store = Arc::new(ControlledStore {
        block_transition_entered: Mutex::new(Some(entered_tx)),
        ..ControlledStore::new()
    });
    let engine = Arc::new(RejectActivationEngine {
        inner: LocalTaskExecutionEngine::new(ResourceCapacity {
            cpu_slots: 1,
            ..ResourceCapacity::default()
        }),
    });
    let service = TaskExecutionServiceBuilder::default()
        .store(store.clone())
        .engine(engine)
        .build()
        .await
        .expect("service builds");
    let id = service
        .submit_local::<_, (), std::io::Error>(|_| panic!("activation failure prevents handler execution"))
        .await
        .expect("task accepted")
        .task_id();
    tokio::time::timeout(Duration::from_secs(2), entered_rx)
        .await
        .expect("scheduler begins activation-failure block")
        .expect("blocked transition entry signalled");
    assert_eq!(
        service
            .cancel(id)
            .await
            .expect("running task cancellation request persists"),
        qubit_task::service::CancelOutcome::CancellationRequested
    );
    store.block_transition_release.add_permits(1);

    let record = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let record = service.get(id).await.expect("record loads").expect("record exists");
            if matches!(record.state, TaskState::Blocked { .. }) {
                break record;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("activation failure eventually blocks latest running revision");
    assert!(record.cancel_requested);
    assert!(service.last_store_error().is_none());
    service.shutdown().await.expect("blocked task allows shutdown");
}

#[tokio::test]
async fn test_zero_history_fast_handler_completion_keeps_service_healthy() {
    let service = TaskExecutionServiceBuilder::default()
        .store(Arc::new(MemoryTaskStore::new(0)))
        .build()
        .await
        .expect("service builds");
    let (ran_tx, ran_rx) = oneshot::channel();
    service
        .submit_local(move |_| {
            let _ = ran_tx.send(());
            LocalTaskOutcome::<(), std::io::Error>::Succeeded {
                value: (),
                summary: TaskOutput::default(),
            }
        })
        .await
        .expect("task accepted");
    tokio::time::timeout(Duration::from_secs(2), ran_rx)
        .await
        .expect("fast handler runs")
        .expect("handler run signalled");
    service
        .shutdown()
        .await
        .expect("service shuts down after fast completion");
    assert!(service.last_store_error().is_none());
}

#[tokio::test]
async fn test_store_fault_shutdown_waits_for_inflight_accept_side_effects() {
    struct DropProbe(Arc<AtomicBool>);
    impl Drop for DropProbe {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    let (get_entered_tx, get_entered_rx) = oneshot::channel();
    let store = Arc::new(ControlledStore {
        get_entered: Mutex::new(Some(get_entered_tx)),
        fail_next_get: AtomicBool::new(true),
        ..ControlledStore::new()
    });
    let service = TaskExecutionServiceBuilder::default()
        .store(store.clone())
        .build()
        .await
        .expect("service builds");
    service
        .submit_local(|_| LocalTaskOutcome::<(), std::io::Error>::Succeeded {
            value: (),
            summary: TaskOutput::default(),
        })
        .await
        .expect("first task accepted");
    tokio::time::timeout(Duration::from_secs(2), get_entered_rx)
        .await
        .expect("scheduler enters get")
        .expect("get entry signalled");

    let (accept_entered_tx, accept_entered_rx) = oneshot::channel();
    *store.accept_entered.lock() = Some(accept_entered_tx);
    let handler_dropped = Arc::new(AtomicBool::new(false));
    let handler_ran = Arc::new(AtomicBool::new(false));
    let drop_probe = DropProbe(handler_dropped.clone());
    let ran = handler_ran.clone();
    let submitting_service = service.clone();
    let submission = tokio::spawn(async move {
        submitting_service
            .submit_local(move |_| {
                let _probe = drop_probe;
                ran.store(true, Ordering::Release);
                LocalTaskOutcome::<(), std::io::Error>::Succeeded {
                    value: (),
                    summary: TaskOutput::default(),
                }
            })
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), accept_entered_rx)
        .await
        .expect("second accept entered")
        .expect("accept entry signalled");

    store.get_release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(2), async {
        while service.last_store_error().is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("scheduler fault recorded");

    let closing = service.shutdown();
    tokio::pin!(closing);
    assert!(matches!(futures::poll!(closing.as_mut()), std::task::Poll::Pending));
    store.accept_release.add_permits(1);
    let accepted_handle = tokio::time::timeout(Duration::from_secs(2), submission)
        .await
        .expect("second submission finishes")
        .expect("submission task joins")
        .expect("second task accepted");
    let accepted_id = accepted_handle.task_id();
    let error = tokio::time::timeout(Duration::from_secs(2), closing)
        .await
        .expect("fault shutdown finishes after acceptance")
        .expect_err("fault is reported");
    assert!(
        matches!(error, TaskServiceError::StoreUnavailable(message) if message.contains("injected scheduler get failure"))
    );
    assert!(service.get(accepted_id).await.expect("accepted record loads").is_some());
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), accepted_handle.result())
            .await
            .expect("late accepted handle finalizes after store fault"),
        Err(LocalTaskResultError::StoreUnavailable(message)) if message.contains("injected scheduler get failure")
    ));
    assert!(
        handler_dropped.load(Ordering::Acquire),
        "faulted admission releases the local closure"
    );
    assert!(
        !handler_ran.load(Ordering::Acquire),
        "faulted admission never runs the local closure"
    );
}

#[tokio::test]
async fn test_shutdown_failure_is_shared_with_other_callers() {
    let store = Arc::new(ControlledStore {
        recoverable: true,
        fail_release: true,
        ..ControlledStore::new()
    });
    let service = TaskExecutionServiceBuilder::default()
        .store(store.clone())
        .build()
        .await
        .expect("recoverable service builds");

    let (first, second) = tokio::join!(service.shutdown(), service.shutdown());
    let first = first.expect_err("first shutdown reports release failure");
    let second = second.expect_err("second shutdown reports release failure");
    assert!(first.to_string().contains("injected owner release failure"));
    assert_eq!(first.to_string(), second.to_string());
    assert_eq!(store.release_count.load(Ordering::Acquire), 1);
    assert!(
        service.last_store_error().is_some(),
        "owner release failure pauses the service"
    );
}

#[tokio::test]
async fn test_shutdown_statistics_failure_wakes_waiter_with_store_diagnostic() {
    let store = Arc::new(ControlledStore::new());
    let service = TaskExecutionServiceBuilder::default()
        .store(store.clone())
        .build()
        .await
        .expect("service builds");
    let (started_tx, started_rx) = oneshot::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    let id = service
        .submit_local(move |_| {
            let _ = started_tx.send(());
            resume_rx.recv().expect("test handler is released");
            LocalTaskOutcome::<(), std::io::Error>::Succeeded {
                value: (),
                summary: TaskOutput::default(),
            }
        })
        .await
        .expect("task accepted")
        .task_id();
    tokio::time::timeout(Duration::from_secs(2), started_rx)
        .await
        .expect("handler starts")
        .expect("handler start signalled");

    store.fail_next_statistics.store(true, Ordering::Release);
    let error = tokio::time::timeout(Duration::from_secs(2), service.shutdown())
        .await
        .expect("shutdown returns statistics failure")
        .expect_err("shutdown must fail");
    assert!(error.to_string().contains("injected statistics failure"));
    assert!(
        service
            .last_store_error()
            .is_some_and(|message| message.contains("injected statistics failure")),
        "coordinator preserves the storage fault diagnostic"
    );
    let error = tokio::time::timeout(Duration::from_secs(2), service.wait(id))
        .await
        .expect("wait resolves after coordinator fault")
        .expect_err("wait reports storage fault");
    assert!(
        matches!(error, TaskServiceError::StoreUnavailable(message) if message.contains("injected statistics failure"))
    );
    resume_tx.send(()).expect("blocked handler is released");
}
