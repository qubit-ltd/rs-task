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
use qubit_task::model::TransitionCommand;
use qubit_task::service::CancelOutcome;
use qubit_task::store::MemoryTaskStore;
use qubit_task::store::StoreError;
use qubit_task::store::TaskFuture;
use qubit_task::store::TaskStore;
use tokio::sync::Notify;

struct HoldTerminalStore {
    inner: MemoryTaskStore,
    hold_once: AtomicBool,
    conflict_cancel_once: AtomicBool,
    entered: Notify,
    release: Notify,
    terminal_writes: AtomicUsize,
    hold_cancel_return: AtomicBool,
    cancel_persisted: Notify,
    release_cancel: Notify,
    second_registered: Notify,
}

impl HoldTerminalStore {
    fn new() -> Self {
        Self {
            inner: MemoryTaskStore::new(16),
            hold_once: AtomicBool::new(true),
            conflict_cancel_once: AtomicBool::new(true),
            entered: Notify::new(),
            release: Notify::new(),
            terminal_writes: AtomicUsize::new(0),
            hold_cancel_return: AtomicBool::new(false),
            cancel_persisted: Notify::new(),
            release_cancel: Notify::new(),
            second_registered: Notify::new(),
        }
    }

    fn for_late_cancel_return() -> Self {
        let store = Self::new();
        store.hold_once.store(false, Ordering::Release);
        store.conflict_cancel_once.store(false, Ordering::Release);
        store.hold_cancel_return.store(true, Ordering::Release);
        store
    }
}

impl TaskStore for HoldTerminalStore {
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
            if matches!(command.state, TaskState::Running)
                && command.cancel_requested
                && self.conflict_cancel_once.swap(false, Ordering::AcqRel)
            {
                let mut concurrent = command.clone();
                concurrent.cancel_requested = false;
                self.inner
                    .transition(concurrent)
                    .await
                    .expect("competing revision advances");
                return Err(StoreError::Conflict);
            }
            if matches!(command.state, TaskState::Running)
                && command.cancel_requested
                && self.hold_cancel_return.swap(false, Ordering::AcqRel)
            {
                let updated = self.inner.transition(command).await?;
                self.cancel_persisted.notify_one();
                self.release_cancel.notified().await;
                return Ok(updated);
            }
            if command.state.is_terminal() && self.hold_once.swap(false, Ordering::AcqRel) {
                self.entered.notify_one();
                self.release.notified().await;
            }
            let terminal = command.state.is_terminal();
            let updated = self.inner.transition(command).await?;
            if terminal {
                self.terminal_writes.fetch_add(1, Ordering::AcqRel);
            }
            Ok(updated)
        })
    }
    fn get<'a>(&'a self, id: TaskId) -> TaskFuture<'a, Result<Option<TaskRecord>, StoreError>> {
        Box::pin(async move {
            let record = self.inner.get(id).await?;
            if record
                .as_ref()
                .is_some_and(|value| value.attempt == 2 && matches!(value.state, TaskState::Running))
            {
                self.second_registered.notify_one();
            }
            Ok(record)
        })
    }
    fn list<'a>(&'a self, query: TaskQuery) -> TaskFuture<'a, Result<TaskPage, StoreError>> {
        self.inner.list(query)
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

struct RetryOnceHandler {
    first_started: Notify,
    release_first: Notify,
    release_second: Notify,
    second_signal: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<Arc<AtomicBool>>>>,
}

impl TaskHandler for RetryOnceHandler {
    fn descriptor(&self) -> TaskHandlerDescriptor {
        TaskHandlerDescriptor {
            task_type: "retry-once".into(),
            version: "1".into(),
        }
    }

    fn run<'a>(&'a self, _payload: &'a [u8], context: TaskContext) -> TaskFuture<'a, TaskRunResult> {
        Box::pin(async move {
            if context.attempt() == 1 {
                self.first_started.notify_one();
                self.release_first.notified().await;
                Err(TaskRunError {
                    category: "temporary".into(),
                    message: "retry requested".into(),
                    retryable: true,
                })
            } else {
                self.second_signal
                    .lock()
                    .expect("signal sender lock")
                    .take()
                    .expect("second attempt sender")
                    .send(context.cancellation_signal())
                    .expect("test receives second attempt signal");
                self.release_second.notified().await;
                Ok(TaskRunOutcome::Succeeded(TaskOutput::default()))
            }
        })
    }
}

#[tokio::test]
async fn test_success_after_cancellation_request_stays_succeeded() {
    let service = TaskExecutionServiceBuilder::in_memory()
        .build()
        .await
        .expect("service builds");
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let id = service
        .submit_local(move |context| {
            started_tx
                .send(context.cancellation_signal())
                .expect("test receives cancellation signal");
            release_rx.recv().expect("test releases handler");
            assert!(
                context.is_cancelled(),
                "handler observes persisted cancellation request"
            );
            Ok(TaskRunOutcome::Succeeded(TaskOutput {
                summary: b"completed".to_vec(),
            }))
        })
        .await
        .expect("task accepted");
    let signal = started_rx.await.expect("handler started");
    assert_eq!(
        service.cancel(id).await.expect("request succeeds"),
        CancelOutcome::CancellationRequested
    );
    tokio::time::timeout(Duration::from_secs(2), async {
        while !signal.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("handler signal follows persisted request");
    release_tx.send(()).expect("release handler");
    let record = tokio::time::timeout(Duration::from_secs(2), service.wait(id))
        .await
        .expect("task settles")
        .expect("wait succeeds");
    assert_eq!(record.state, TaskState::Succeeded);
    assert_eq!(record.output.expect("successful summary").summary, b"completed");
    service.shutdown().await.expect("service shuts down");
}

#[tokio::test]
async fn test_handler_explicitly_confirms_cancellation() {
    let service = TaskExecutionServiceBuilder::in_memory()
        .build()
        .await
        .expect("service builds");
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let id = service
        .submit_local(move |context| {
            started_tx
                .send(context.cancellation_signal())
                .expect("test receives cancellation signal");
            release_rx.recv().expect("test releases handler");
            assert!(context.is_cancelled());
            Ok(TaskRunOutcome::Cancelled)
        })
        .await
        .expect("task accepted");
    let signal = started_rx.await.expect("handler started");
    assert_eq!(
        service.cancel(id).await.expect("request succeeds"),
        CancelOutcome::CancellationRequested
    );
    tokio::time::timeout(Duration::from_secs(2), async {
        while !signal.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("handler signal follows persisted request");
    release_tx.send(()).expect("release handler");
    let record = tokio::time::timeout(Duration::from_secs(2), service.wait(id))
        .await
        .expect("task settles")
        .expect("wait succeeds");
    assert_eq!(record.state, TaskState::Cancelled);
    service.shutdown().await.expect("service shuts down");
}

#[tokio::test]
async fn test_handler_result_racing_cancellation_commits_one_terminal_state() {
    let store = Arc::new(HoldTerminalStore::new());
    let service = TaskExecutionServiceBuilder::default()
        .store(store.clone())
        .build()
        .await
        .expect("service builds");
    let id = service
        .submit_local(|_| Ok(TaskRunOutcome::Succeeded(TaskOutput::default())))
        .await
        .expect("task accepted");
    tokio::time::timeout(Duration::from_secs(2), store.entered.notified())
        .await
        .expect("handler result reached terminal write");
    assert_eq!(
        service.cancel(id).await.expect("normal race must not return Conflict"),
        CancelOutcome::CancellationRequested
    );
    store.release.notify_one();
    let record = tokio::time::timeout(Duration::from_secs(2), service.wait(id))
        .await
        .expect("task settles")
        .expect("wait succeeds");
    assert_eq!(record.state, TaskState::Succeeded);
    assert_eq!(store.terminal_writes.load(Ordering::Acquire), 1);
    service.shutdown().await.expect("service shuts down");
}

#[tokio::test]
async fn test_blocked_task_can_be_cancelled_directly() {
    let service = TaskExecutionServiceBuilder::in_memory()
        .build()
        .await
        .expect("service builds");
    let accepted = service
        .submit(TaskRequest::new("missing", "1", Vec::new()))
        .await
        .expect("task accepted");
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let record = service
                .get(accepted.id)
                .await
                .expect("query succeeds")
                .expect("record exists");
            if matches!(record.state, TaskState::Blocked { .. }) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("task becomes blocked");
    assert_eq!(
        service
            .cancel(accepted.id)
            .await
            .expect("blocked cancellation succeeds"),
        CancelOutcome::CancelledBeforeStart
    );
    assert_eq!(
        service.wait(accepted.id).await.expect("cancelled task settles").state,
        TaskState::Cancelled
    );
    service.shutdown().await.expect("service shuts down");
}

#[tokio::test]
async fn test_late_cancel_response_does_not_signal_next_attempt() {
    let store = Arc::new(HoldTerminalStore::for_late_cancel_return());
    let (second_signal_tx, second_signal_rx) = tokio::sync::oneshot::channel();
    let handler = Arc::new(RetryOnceHandler {
        first_started: Notify::new(),
        release_first: Notify::new(),
        release_second: Notify::new(),
        second_signal: std::sync::Mutex::new(Some(second_signal_tx)),
    });
    let service = TaskExecutionServiceBuilder::default()
        .store(store.clone())
        .register_handler(handler.clone())
        .expect("handler registers")
        .build()
        .await
        .expect("service builds");
    let accepted = service
        .submit(TaskRequest::new("retry-once", "1", Vec::new()))
        .await
        .expect("task accepted");
    tokio::time::timeout(Duration::from_secs(2), handler.first_started.notified())
        .await
        .expect("first attempt starts");
    let cancel_service = service.clone();
    let cancel = tokio::spawn(async move { cancel_service.cancel(accepted.id).await });
    tokio::time::timeout(Duration::from_secs(2), store.cancel_persisted.notified())
        .await
        .expect("first attempt cancellation request persisted");
    handler.release_first.notify_one();
    let second_signal = tokio::time::timeout(Duration::from_secs(2), second_signal_rx)
        .await
        .expect("second attempt starts")
        .expect("signal received");
    tokio::time::timeout(Duration::from_secs(2), store.second_registered.notified())
        .await
        .expect("second attempt signal is registered before stale response");
    store.release_cancel.notify_one();
    assert_eq!(
        cancel.await.expect("cancel call completes").expect("cancel succeeds"),
        CancelOutcome::CancellationRequested
    );
    assert!(
        !second_signal.load(Ordering::Acquire),
        "attempt two must not receive attempt one's stale signal"
    );
    handler.release_second.notify_one();
    let final_record = tokio::time::timeout(Duration::from_secs(2), service.wait(accepted.id))
        .await
        .expect("task settles")
        .expect("wait succeeds");
    assert_eq!(final_record.state, TaskState::Succeeded);
    assert_eq!(final_record.attempt, 2);
    service.shutdown().await.expect("service shuts down");
}
