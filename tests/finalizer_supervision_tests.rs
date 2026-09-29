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

use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::handler::TaskContext;
use qubit_task::handler::TaskHandler;
use qubit_task::handler::TaskHandlerDescriptor;
use qubit_task::handler::TaskRunOutcome;
use qubit_task::handler::TaskRunResult;
use qubit_task::model::AcceptOutcome;
use qubit_task::model::OwnerEpoch;
use qubit_task::model::RecoveryPage;
use qubit_task::model::StoreCapabilities;
use qubit_task::model::TaskCursor;
use qubit_task::model::TaskId;
use qubit_task::model::TaskOutput;
use qubit_task::model::TaskPage;
use qubit_task::model::TaskQuery;
use qubit_task::model::TaskRecord;
use qubit_task::model::TaskRequest;
use qubit_task::model::TaskState;
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
use tokio::sync;
use tokio::test as tokio_test;
use tokio::time;

#[derive(Clone, Copy)]
enum Mode {
    Method,
    Transition,
    Retry,
    NonString,
    Conflict,
    Summary,
    Healthy,
}

struct PanicStore {
    inner: MemoryTaskStore,
    mode: Mode,
    finalizations: AtomicUsize,
    summary_panic: AtomicBool,
    entered: sync::Notify,
}

impl PanicStore {
    /// Creates an isolated store whose finalizer failure is selected
    /// explicitly.
    fn new(mode: Mode) -> Self {
        Self {
            inner: MemoryTaskStore::new(16),
            mode,
            finalizations: AtomicUsize::new(0),
            summary_panic: AtomicBool::new(false),
            entered: sync::Notify::new(),
        }
    }
}
impl TaskStore for PanicStore {
    fn capabilities(&self) -> StoreCapabilities {
        self.inner.capabilities()
    }

    fn accept<'a>(&'a self, id: TaskId, request: TaskRequest) -> TaskFuture<'a, Result<AcceptOutcome, StoreError>> {
        self.inner.accept(id, request)
    }

    fn get_by_idempotency_key<'a>(&'a self, key: &'a str) -> TaskFuture<'a, Result<Option<TaskRecord>, StoreError>> {
        self.inner.get_by_idempotency_key(key)
    }

    fn get_summary_by_idempotency_key<'a>(
        &'a self,
        key: &'a str,
    ) -> TaskFuture<'a, Result<Option<TaskSummary>, StoreError>> {
        self.inner.get_summary_by_idempotency_key(key)
    }

    fn transition<'a>(&'a self, command: TransitionCommand) -> TaskFuture<'a, Result<TaskSummary, StoreError>> {
        if matches!(self.mode, Mode::Method) && command.expected_attempt == 1 {
            self.entered.notify_one();
            panic!("injected transition method panic");
        }
        Box::pin(async move {
            if command.expected_attempt == 1 && !matches!(command.state, TaskState::Running) {
                let call = self.finalizations.fetch_add(1, Ordering::AcqRel);
                self.entered.notify_one();
                match self.mode {
                    Mode::Transition | Mode::Retry => panic!("injected finalizer transition panic"),
                    Mode::NonString => std::panic::panic_any(17_u32),
                    Mode::Conflict if call == 0 => return Err(StoreError::Conflict),
                    Mode::Conflict => panic!("injected second CAS panic"),
                    Mode::Summary if call == 0 => {
                        self.summary_panic.store(true, Ordering::Release);
                        return Err(StoreError::Conflict);
                    }
                    _ => {}
                }
            }
            self.inner.transition(command).await
        })
    }

    fn get_summary<'a>(&'a self, id: TaskId) -> TaskFuture<'a, Result<Option<TaskSummary>, StoreError>> {
        Box::pin(async move {
            if self.summary_panic.swap(false, Ordering::AcqRel) {
                panic!("injected finalizer summary panic");
            }
            self.inner.get_summary(id).await
        })
    }

    fn get<'a>(&'a self, id: TaskId) -> TaskFuture<'a, Result<Option<TaskRecord>, StoreError>> {
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

    fn has_unfinished_over_limit<'a>(&'a self, limit: usize) -> TaskFuture<'a, Result<bool, StoreError>> {
        self.inner.has_unfinished_over_limit(limit)
    }

    fn scan_unfinished<'a>(&'a self, cursor: Option<TaskCursor>) -> TaskFuture<'a, Result<RecoveryPage, StoreError>> {
        self.inner.scan_unfinished(cursor)
    }

    fn release_owner<'a>(&'a self, epoch: OwnerEpoch) -> TaskFuture<'a, Result<(), StoreError>> {
        self.inner.release_owner(epoch)
    }
}

struct TestHandler {
    retry: bool,
}
impl TaskHandler for TestHandler {
    fn descriptor(&self) -> TaskHandlerDescriptor {
        TaskHandlerDescriptor {
            task_type: "finalizer".into(),
            version: "1".into(),
        }
    }
    fn run<'a>(&'a self, _payload: &'a [u8], _context: TaskContext) -> TaskFuture<'a, TaskRunResult> {
        Box::pin(async move {
            if self.retry {
                Err(qubit_task::model::TaskRunError {
                    category: "retry".into(),
                    message: "retry".into(),
                    retryable: true,
                })
            } else {
                Ok(TaskRunOutcome::Succeeded(TaskOutput::default()))
            }
        })
    }
}

/// Observes the public fault after a handshake proves finalization was polled.
async fn assert_finalizer_fault(mode: Mode) {
    let store = Arc::new(PanicStore::new(mode));
    let service = TaskExecutionServiceBuilder::default()
        .runtime_handle(tokio::runtime::Handle::current())
        .store(store.clone())
        .register_handler(Arc::new(TestHandler {
            retry: matches!(mode, Mode::Retry),
        }))
        .expect("handler")
        .build()
        .await
        .expect("service");
    let task = service
        .submit(TaskRequest::new("finalizer", "1", vec![]).with_idempotency_key("finalizer"))
        .await
        .expect("accepted");
    time::timeout(Duration::from_secs(2), store.entered.notified())
        .await
        .expect("finalization entered");
    let result = time::timeout(Duration::from_secs(1), service.wait(task.id)).await;
    assert!(
        matches!(result, Ok(Err(TaskServiceError::StoreUnavailable(_)))),
        "wait must observe finalizer fault: {result:?}"
    );
    let retained = store
        .inner
        .get_summary(task.id)
        .await
        .expect("raw store read")
        .expect("retained task");
    assert_eq!(
        retained.state,
        TaskState::Running,
        "a store panic must not fabricate a terminal state"
    );
    let diagnostic = service.last_store_error().expect("fault latched");
    assert!(diagnostic.contains(&task.id.to_string()) && diagnostic.contains("attempt 1"));
    assert!(matches!(
        time::timeout(Duration::from_secs(1), service.shutdown()).await,
        Ok(Err(TaskServiceError::StoreUnavailable(_)))
    ));
}

#[tokio_test]
async fn test_finalizer_transition_panic() {
    assert_finalizer_fault(Mode::Transition).await;
}
#[tokio_test]
async fn test_finalizer_retry_reservation_panic() {
    assert_finalizer_fault(Mode::Retry).await;
}
#[tokio_test]
async fn test_finalizer_second_cas_panic() {
    assert_finalizer_fault(Mode::Conflict).await;
}
#[tokio_test]
async fn test_finalizer_summary_panic() {
    assert_finalizer_fault(Mode::Summary).await;
}
#[tokio_test]
async fn test_finalizer_non_string_panic() {
    assert_finalizer_fault(Mode::NonString).await;
}

#[tokio_test]
async fn test_finalizer_panic_wakes_local_handle() {
    let service = TaskExecutionServiceBuilder::default()
        .runtime_handle(tokio::runtime::Handle::current())
        .store(Arc::new(PanicStore::new(Mode::Transition)))
        .build()
        .await
        .expect("service");
    let handle = service
        .submit_local(|_| LocalTaskOutcome::<(), std::io::Error>::Succeeded {
            value: (),
            summary: TaskOutput::default(),
        })
        .await
        .expect("accepted");
    assert!(matches!(
        time::timeout(Duration::from_secs(1), handle.result()).await,
        Ok(Err(LocalTaskResultError::StoreUnavailable(_)))
    ));
    assert!(matches!(
        service.shutdown().await,
        Err(TaskServiceError::StoreUnavailable(_))
    ));
}

#[tokio_test]
async fn test_handler_panic_and_success_keep_service_healthy() {
    let service = TaskExecutionServiceBuilder::default()
        .runtime_handle(tokio::runtime::Handle::current())
        .store(Arc::new(PanicStore::new(Mode::Healthy)))
        .build()
        .await
        .expect("service");
    let handle = service
        .submit_local(|_| -> LocalTaskOutcome<(), std::io::Error> { panic!("handler panic") })
        .await
        .expect("accepted");
    assert!(matches!(
        service.wait(handle.task_id()).await.expect("terminal").state,
        TaskState::Panicked { .. }
    ));
    let handle = service
        .submit_local(|_| LocalTaskOutcome::<(), std::io::Error>::Succeeded {
            value: (),
            summary: TaskOutput::default(),
        })
        .await
        .expect("accepted");
    handle.result().await.expect("finalized").expect("success");
    assert!(service.last_store_error().is_none());
    service.shutdown().await.expect("clean shutdown");
}

#[tokio_test]
async fn test_finalizer_store_method_panic() {
    assert_finalizer_fault(Mode::Method).await;
}
