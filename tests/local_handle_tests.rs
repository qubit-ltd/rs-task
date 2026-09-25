// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::time::Duration;

use parking_lot::Mutex;
use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::model::AcceptOutcome;
use qubit_task::model::OwnerEpoch;
use qubit_task::model::StoreCapabilities;
use qubit_task::model::StoredTaskPage;
use qubit_task::model::TaskId;
use qubit_task::model::TaskOutput;
use qubit_task::model::TaskPage;
use qubit_task::model::TaskQuery;
use qubit_task::model::TaskRecord;
use qubit_task::model::TaskRequest;
use qubit_task::model::TaskState;
use qubit_task::model::TaskStateCounts;
use qubit_task::model::TaskStateKind;
use qubit_task::model::TransitionCommand;
use qubit_task::service::CancelOutcome;
use qubit_task::service::LocalTaskOutcome;
use qubit_task::service::LocalTaskResultError;
use qubit_task::service::TaskServiceError;
use qubit_task::store::MemoryTaskStore;
use qubit_task::store::StoreError;
use qubit_task::store::TaskFuture;
use qubit_task::store::TaskStore;
use tokio::sync::Semaphore;
use tokio::sync::oneshot;

const WAIT_LIMIT: Duration = Duration::from_secs(3);

struct NonCloneValue(String);

#[derive(Debug, PartialEq, Eq)]
enum DomainError {
    InvalidInput,
}

impl fmt::Display for DomainError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid input")
    }
}

struct PauseAfterAcceptStore {
    inner: MemoryTaskStore,
    accepted: Mutex<Option<oneshot::Sender<()>>>,
    release: Semaphore,
}

struct PauseEvictedGetStore {
    inner: MemoryTaskStore,
    pause_first_get: AtomicBool,
    get_entered: Mutex<Option<oneshot::Sender<()>>>,
    get_release: Semaphore,
    cancel_persisted: Mutex<Option<oneshot::Sender<()>>>,
    cancel_release: Semaphore,
}

impl TaskStore for PauseEvictedGetStore {
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
        Box::pin(async move {
            let cancel = matches!(command.state, TaskState::Cancelled);
            let updated = self.inner.transition(command).await?;
            if cancel {
                if let Some(sender) = self.cancel_persisted.lock().take() {
                    let _ = sender.send(());
                }
                self.cancel_release
                    .acquire()
                    .await
                    .expect("cancel transition released")
                    .forget();
            }
            Ok(updated)
        })
    }

    fn get<'a>(&'a self, id: TaskId) -> TaskFuture<'a, Result<Option<TaskRecord>, StoreError>> {
        Box::pin(async move {
            if self.pause_first_get.swap(false, Ordering::AcqRel) {
                if let Some(sender) = self.get_entered.lock().take() {
                    let _ = sender.send(());
                }
                self.get_release
                    .acquire()
                    .await
                    .expect("scheduler get released")
                    .forget();
            }
            self.inner.get(id).await
        })
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

    fn scan_unfinished<'a>(&'a self, cursor: Option<TaskId>) -> TaskFuture<'a, Result<StoredTaskPage, StoreError>> {
        self.inner.scan_unfinished(cursor)
    }

    fn release_owner<'a>(&'a self, epoch: OwnerEpoch) -> TaskFuture<'a, Result<(), StoreError>> {
        self.inner.release_owner(epoch)
    }
}

impl TaskStore for PauseAfterAcceptStore {
    fn capabilities(&self) -> StoreCapabilities {
        self.inner.capabilities()
    }

    fn accept<'a>(&'a self, id: TaskId, request: TaskRequest) -> TaskFuture<'a, Result<AcceptOutcome, StoreError>> {
        Box::pin(async move {
            let accepted = self.inner.accept(id, request).await?;
            if let Some(sender) = self.accepted.lock().take() {
                let _ = sender.send(());
            }
            self.release.acquire().await.expect("test accept is released").forget();
            Ok(accepted)
        })
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

    fn scan_unfinished<'a>(&'a self, cursor: Option<TaskId>) -> TaskFuture<'a, Result<StoredTaskPage, StoreError>> {
        self.inner.scan_unfinished(cursor)
    }

    fn release_owner<'a>(&'a self, epoch: OwnerEpoch) -> TaskFuture<'a, Result<(), StoreError>> {
        self.inner.release_owner(epoch)
    }
}

async fn cancel_during_accept(history_capacity: usize) {
    let (accepted_tx, accepted_rx) = oneshot::channel();
    let store = Arc::new(PauseAfterAcceptStore {
        inner: MemoryTaskStore::new(history_capacity),
        accepted: Mutex::new(Some(accepted_tx)),
        release: Semaphore::new(0),
    });
    let service = TaskExecutionServiceBuilder::default()
        .store(store.clone())
        .build()
        .await
        .expect("service builds");
    let ran = Arc::new(AtomicBool::new(false));
    let handler_ran = Arc::clone(&ran);
    let submitting = service.clone();
    let submission = tokio::spawn(async move {
        submitting
            .submit_local(move |_| {
                handler_ran.store(true, Ordering::Release);
                LocalTaskOutcome::<(), DomainError>::Succeeded {
                    value: (),
                    summary: TaskOutput::default(),
                }
            })
            .await
    });
    tokio::time::timeout(WAIT_LIMIT, accepted_rx)
        .await
        .expect("store persisted Queued")
        .expect("store sent accept signal");
    let page = service
        .list(TaskQuery {
            states: vec![TaskStateKind::Queued],
            limit: 2,
            ..TaskQuery::default()
        })
        .await
        .expect("persisted Queued record can be found before accept returns");
    assert_eq!(page.records.len(), 1);
    let id = page.records[0].id;
    assert_eq!(
        service.cancel(id).await.expect("persisted Queued task cancels"),
        CancelOutcome::CancelledBeforeStart
    );
    store.release.add_permits(1);
    let handle = tokio::time::timeout(WAIT_LIMIT, submission)
        .await
        .expect("submit finishes after accept gate opens")
        .expect("submit task joins")
        .expect("accepted task still returns a handle");
    assert_eq!(handle.task_id(), id);
    assert!(matches!(
        tokio::time::timeout(WAIT_LIMIT, handle.result())
            .await
            .expect("cancelled handle finalizes"),
        Err(LocalTaskResultError::Cancelled)
    ));
    assert!(!ran.load(Ordering::Acquire), "cancelled handler must never run");
    let record = service.get(id).await.expect("record lookup succeeds");
    if history_capacity == 0 {
        assert!(record.is_none(), "zero-capacity history evicts cancellation");
    } else {
        assert_eq!(record.expect("cancelled record retained").state, TaskState::Cancelled);
    }
    service.shutdown().await.expect("service shuts down");
}

#[tokio::test]
async fn test_cancel_persisted_local_task_before_accept_returns() {
    cancel_during_accept(16).await;
}

#[tokio::test]
async fn test_cancel_persisted_local_task_before_accept_returns_with_zero_history() {
    cancel_during_accept(0).await;
}

#[tokio::test]
async fn test_evicted_cancel_waits_for_authoritative_transition_response() {
    let (get_entered_tx, get_entered_rx) = oneshot::channel();
    let (cancel_persisted_tx, cancel_persisted_rx) = oneshot::channel();
    let store = Arc::new(PauseEvictedGetStore {
        inner: MemoryTaskStore::new(0),
        pause_first_get: AtomicBool::new(true),
        get_entered: Mutex::new(Some(get_entered_tx)),
        get_release: Semaphore::new(0),
        cancel_persisted: Mutex::new(Some(cancel_persisted_tx)),
        cancel_release: Semaphore::new(0),
    });
    let service = TaskExecutionServiceBuilder::default()
        .store(store.clone())
        .queue_capacity(1)
        .build()
        .await
        .expect("service builds");
    let handle = service
        .submit_local(|_| -> LocalTaskOutcome<(), DomainError> { panic!("cancelled handler must not run") })
        .await
        .expect("first task accepted");
    let id = handle.task_id();
    tokio::time::timeout(WAIT_LIMIT, get_entered_rx)
        .await
        .expect("scheduler entered first get")
        .expect("scheduler get signal received");
    let cancelling_service = service.clone();
    let cancellation = tokio::spawn(async move { cancelling_service.cancel(id).await });
    tokio::time::timeout(WAIT_LIMIT, cancel_persisted_rx)
        .await
        .expect("Cancelled was persisted and evicted")
        .expect("cancel persistence signal received");
    store.get_release.add_permits(1);
    let replacement = tokio::time::timeout(WAIT_LIMIT, async {
        loop {
            match service
                .submit_local(|_| LocalTaskOutcome::<(), DomainError>::Succeeded {
                    value: (),
                    summary: TaskOutput::default(),
                })
                .await
            {
                Ok(handle) => break handle,
                Err(TaskServiceError::QueueFull) => tokio::task::yield_now().await,
                Err(error) => panic!("unexpected replacement submission error: {error}"),
            }
        }
    })
    .await
    .expect("scheduler releases first queue slot after get(None)");
    replacement
        .result()
        .await
        .expect("replacement finalizes")
        .expect("replacement succeeds");

    let mut result = Box::pin(handle.result());
    assert!(matches!(futures::poll!(result.as_mut()), std::task::Poll::Pending));
    store.cancel_release.add_permits(1);
    assert_eq!(
        tokio::time::timeout(WAIT_LIMIT, cancellation)
            .await
            .expect("cancel finishes")
            .expect("cancel task joins")
            .expect("cancel succeeds"),
        CancelOutcome::CancelledBeforeStart
    );
    assert!(matches!(
        tokio::time::timeout(WAIT_LIMIT, result)
            .await
            .expect("authoritative cancellation reaches handle"),
        Err(LocalTaskResultError::Cancelled)
    ));
    service.shutdown().await.expect("service shuts down");
}

#[tokio::test]
async fn test_local_handle_delivers_non_clone_value_after_summary_is_persisted() {
    let service = TaskExecutionServiceBuilder::in_memory()
        .build()
        .await
        .expect("service builds");
    let handle = service
        .submit_local(|_| LocalTaskOutcome::<NonCloneValue, DomainError>::Succeeded {
            value: NonCloneValue("full in-process value".into()),
            summary: TaskOutput {
                summary: b"small persisted summary".to_vec(),
            },
        })
        .await
        .expect("local task accepted");
    let task_id = handle.task_id();
    let NonCloneValue(value) = tokio::time::timeout(WAIT_LIMIT, handle.result())
        .await
        .expect("handle result arrives")
        .expect("task succeeded")
        .expect("typed result succeeded");
    assert_eq!(value, "full in-process value");
    let record = service
        .get(task_id)
        .await
        .expect("record query succeeds")
        .expect("record retained");
    assert_eq!(record.state, TaskState::Succeeded);
    assert_eq!(
        record.output.expect("summary persisted").summary,
        b"small persisted summary"
    );
    service.shutdown().await.expect("service shuts down");
}

#[tokio::test]
async fn test_local_handle_preserves_domain_error_type() {
    let service = TaskExecutionServiceBuilder::in_memory()
        .build()
        .await
        .expect("service builds");
    let handle = service
        .submit_local(|_| LocalTaskOutcome::<(), DomainError>::Failed(DomainError::InvalidInput))
        .await
        .expect("local task accepted");
    let id = handle.task_id();
    let error: DomainError = tokio::time::timeout(WAIT_LIMIT, handle.result())
        .await
        .expect("handle result arrives")
        .expect("task finalizes")
        .expect_err("domain failure is retained");
    assert_eq!(error, DomainError::InvalidInput);
    let record = service
        .get(id)
        .await
        .expect("record query succeeds")
        .expect("record retained");
    assert!(matches!(record.state, TaskState::Failed { .. }));
    service.shutdown().await.expect("service shuts down");
}

#[tokio::test]
async fn test_queued_local_handle_reports_cancelled_without_running_handler() {
    let (started, started_rx) = tokio::sync::oneshot::channel();
    let (release, release_rx) = mpsc::channel();
    let service = TaskExecutionServiceBuilder::in_memory()
        .capacity(qubit_task::model::ResourceCapacity {
            cpu_slots: 1,
            ..Default::default()
        })
        .build()
        .await
        .expect("service builds");
    let holding = service
        .submit_local(move |_| {
            let _ = started.send(());
            release_rx.recv_timeout(WAIT_LIMIT).expect("holding task released");
            LocalTaskOutcome::<(), DomainError>::Succeeded {
                value: (),
                summary: TaskOutput::default(),
            }
        })
        .await
        .expect("holding task accepted");
    tokio::time::timeout(WAIT_LIMIT, started_rx)
        .await
        .expect("holding task starts")
        .expect("start signal received");

    let ran = Arc::new(AtomicBool::new(false));
    let handler_ran = Arc::clone(&ran);
    let queued = service
        .submit_local(move |_| {
            handler_ran.store(true, Ordering::Release);
            LocalTaskOutcome::<(), DomainError>::Succeeded {
                value: (),
                summary: TaskOutput::default(),
            }
        })
        .await
        .expect("second task queued");
    assert_eq!(
        service.cancel(queued.task_id()).await.expect("queued task cancels"),
        CancelOutcome::CancelledBeforeStart
    );
    assert!(matches!(
        tokio::time::timeout(WAIT_LIMIT, queued.result())
            .await
            .expect("queued handle finalizes"),
        Err(LocalTaskResultError::Cancelled)
    ));
    assert!(!ran.load(Ordering::Acquire), "cancelled queued handler must not run");
    release.send(()).expect("release holding task");
    holding
        .result()
        .await
        .expect("holding task finalizes")
        .expect("holding task succeeds");
    service.shutdown().await.expect("service shuts down");
}

#[tokio::test]
async fn test_handler_initiated_cancellation_has_distinct_handle_result() {
    let service = TaskExecutionServiceBuilder::in_memory()
        .build()
        .await
        .expect("service builds");
    let handle = service
        .submit_local(|_| LocalTaskOutcome::<(), DomainError>::Cancelled)
        .await
        .expect("local task accepted");
    let id = handle.task_id();
    assert!(matches!(
        tokio::time::timeout(WAIT_LIMIT, handle.result())
            .await
            .expect("cancelled handle finalizes"),
        Err(LocalTaskResultError::Cancelled)
    ));
    assert_eq!(
        service
            .get(id)
            .await
            .expect("record query succeeds")
            .expect("record retained")
            .state,
        TaskState::Cancelled
    );
    service.shutdown().await.expect("service shuts down");
}

#[tokio::test]
async fn test_unsatisfiable_local_task_is_rejected_before_acceptance() {
    let service = TaskExecutionServiceBuilder::in_memory()
        .capacity(qubit_task::model::ResourceCapacity {
            cpu_slots: 0,
            ..Default::default()
        })
        .build()
        .await
        .expect("service builds");
    let ran = Arc::new(AtomicBool::new(false));
    let handler_ran = Arc::clone(&ran);
    let result = service
        .submit_local(move |_| {
            handler_ran.store(true, Ordering::Release);
            LocalTaskOutcome::<(), DomainError>::Succeeded {
                value: (),
                summary: TaskOutput::default(),
            }
        })
        .await;
    assert!(matches!(
        result,
        Err(qubit_task::service::TaskServiceError::Unsatisfiable)
    ));
    assert!(!ran.load(Ordering::Acquire));
    assert_eq!(service.stats().await.expect("stats query succeeds").queued, 0);
    assert!(
        service
            .list(qubit_task::model::TaskQuery::default())
            .await
            .expect("history query succeeds")
            .records
            .is_empty()
    );
    service.shutdown().await.expect("service shuts down");
}

#[tokio::test]
async fn test_panicking_local_handler_reports_panic_without_typed_result() {
    let service = TaskExecutionServiceBuilder::in_memory()
        .build()
        .await
        .expect("service builds");
    let handle = service
        .submit_local::<_, (), DomainError>(|_| panic!("local handle panic marker"))
        .await
        .expect("local task accepted");
    let id = handle.task_id();
    let result = tokio::time::timeout(WAIT_LIMIT, handle.result())
        .await
        .expect("panicking handle finalizes");
    assert!(
        matches!(result, Err(LocalTaskResultError::Panicked(message)) if message.contains("local handle panic marker"))
    );
    assert!(matches!(
        service
            .get(id)
            .await
            .expect("record query succeeds")
            .expect("record retained")
            .state,
        TaskState::Panicked { .. }
    ));
    service.shutdown().await.expect("service shuts down");
}

#[tokio::test]
async fn test_dropping_local_handle_does_not_cancel_accepted_task() {
    let service = TaskExecutionServiceBuilder::in_memory()
        .build()
        .await
        .expect("service builds");
    let handle = service
        .submit_local(|_| LocalTaskOutcome::<(), DomainError>::Succeeded {
            value: (),
            summary: TaskOutput::default(),
        })
        .await
        .expect("local task accepted");
    let id = handle.task_id();
    drop(handle);
    let record = tokio::time::timeout(WAIT_LIMIT, service.wait(id))
        .await
        .expect("accepted task finishes")
        .expect("wait succeeds");
    assert_eq!(record.state, TaskState::Succeeded);
    service.shutdown().await.expect("service shuts down");
}

#[tokio::test]
async fn test_local_handle_result_survives_zero_history_retention() {
    let service = TaskExecutionServiceBuilder::default()
        .store(Arc::new(MemoryTaskStore::new(0)))
        .build()
        .await
        .expect("service builds");
    let handle = service
        .submit_local(|_| LocalTaskOutcome::<NonCloneValue, DomainError>::Succeeded {
            value: NonCloneValue("retained by handle".into()),
            summary: TaskOutput::default(),
        })
        .await
        .expect("local task accepted");
    let id = handle.task_id();
    let NonCloneValue(value) = tokio::time::timeout(WAIT_LIMIT, handle.result())
        .await
        .expect("handle result arrives")
        .expect("task finalized")
        .expect("task succeeded");
    assert_eq!(value, "retained by handle");
    assert!(service.get(id).await.expect("history query succeeds").is_none());
    service.shutdown().await.expect("service shuts down");
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_recoverable_store_rejects_typed_local_submission() {
    let path = std::env::temp_dir().join(format!(
        "qubit-task-typed-handle-{}.sqlite",
        qubit_task::TaskId::generate()
    ));
    let service = TaskExecutionServiceBuilder::recoverable_sqlite(&path)
        .expect("SQLite builder created")
        .build()
        .await
        .expect("service builds");
    let result = service
        .submit_local(|_| LocalTaskOutcome::<(), DomainError>::Succeeded {
            value: (),
            summary: TaskOutput::default(),
        })
        .await;
    assert!(matches!(result, Err(TaskServiceError::UnsupportedCapability)));
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
