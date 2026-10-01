// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Gates establish event order. Timeouts are failure bounds, never ordering
//! evidence.

use std::sync::Arc;
use std::time::Duration;

use futures::poll;
use parking_lot::Mutex;
use tokio::sync::Semaphore;
use tokio::sync::oneshot;

use crate::handler::TaskContext;
use crate::handler::TaskHandler;
use crate::handler::TaskHandlerDescriptor;
use crate::handler::TaskRunOutcome;
use crate::handler::TaskRunResult;
use crate::model::AcceptOutcome;
use crate::model::OwnerEpoch;
use crate::model::RecoveryPage;
use crate::model::StoreCapabilities;
use crate::model::TaskCursor;
use crate::model::TaskId;
use crate::model::TaskOutput;
use crate::model::TaskPage;
use crate::model::TaskQuery;
use crate::model::TaskRecord;
use crate::model::TaskRequest;
use crate::model::TaskRunError;
use crate::model::TaskState;
use crate::model::TaskStateCounts;
use crate::model::TaskSummary;
use crate::model::TransitionCommand;
use crate::service::CancelOutcome;
use crate::service::TaskServiceError;
use crate::service::task_execution_service_builder::TaskExecutionServiceBuilder;
use crate::store::LegacyTaskStore as TaskStore;
use crate::store::MemoryTaskStore;
#[cfg(feature = "sqlite")]
use crate::store::SqliteTaskStore;
use crate::store::StoreError;
use crate::store::TaskFuture;

const DEADLINE: Duration = Duration::from_secs(5);

struct StepGate {
    entered: Mutex<Option<oneshot::Sender<()>>>,
    resume: Semaphore,
}

impl StepGate {
    /// Creates a one-step handshake; the returned receiver observes entry.
    fn new() -> (Arc<Self>, oneshot::Receiver<()>) {
        let (entered, receiver) = oneshot::channel();
        (
            Arc::new(Self {
                entered: Mutex::new(Some(entered)),
                resume: Semaphore::new(0),
            }),
            receiver,
        )
    }

    /// Signals entry, then waits for an explicit test continuation permit.
    async fn pause(&self) {
        let sender = self.entered.lock().take();
        if let Some(sender) = sender {
            sender.send(()).expect("test observes gate entry");
        }
        self.resume.acquire().await.expect("gate remains open").forget();
    }

    /// Allows exactly one paused operation to proceed.
    fn advance(&self) {
        self.resume.add_permits(1);
    }
}

/// Waits for a gate signal, failing with context if the tested step never
/// enters.
async fn entered(receiver: oneshot::Receiver<()>) {
    tokio::time::timeout(DEADLINE, receiver)
        .await
        .expect("step enters before deadline")
        .expect("entry signal");
}

/// Creates a bounded revision command for CAS tests without running a service.
fn transition(record: &TaskRecord, state: TaskState) -> TransitionCommand {
    TransitionCommand {
        id: record.id,
        expected_version: record.state_version,
        expected_attempt: record.attempt,
        state,
        retry_not_before_ms: None,
        output: None,
        assigned_resources: vec![],
        cancel_requested: false,
    }
}

/// Drives both CAS callers to a gate, then selects either winner explicitly.
async fn two_cas(store: Arc<dyn TaskStore>, second_wins: bool) {
    let outcome = store
        .accept(TaskId::generate(), TaskRequest::new("cas", "1", vec![]))
        .await
        .expect("accepted");
    let (AcceptOutcome::Accepted(record) | AcceptOutcome::Existing(record)) = outcome;
    let first_command = transition(&record, TaskState::Running);
    let second_command = transition(&record, TaskState::Cancelled);
    let (first_gate, first_entered) = StepGate::new();
    let (second_gate, second_entered) = StepGate::new();
    let first_store = Arc::clone(&store);
    let first_actor_gate = Arc::clone(&first_gate);
    let first = tokio::spawn(async move {
        first_actor_gate.pause().await;
        first_store.transition(first_command).await
    });
    let second_store = Arc::clone(&store);
    let second_actor_gate = Arc::clone(&second_gate);
    let second = tokio::spawn(async move {
        second_actor_gate.pause().await;
        second_store.transition(second_command).await
    });
    entered(first_entered).await;
    entered(second_entered).await;
    let (winner, loser, winning_state) = if second_wins {
        second_gate.advance();
        let winner = tokio::time::timeout(DEADLINE, second)
            .await
            .expect("winner finishes")
            .expect("actor joins");
        first_gate.advance();
        (
            winner,
            tokio::time::timeout(DEADLINE, first)
                .await
                .expect("loser finishes")
                .expect("actor joins"),
            TaskState::Cancelled,
        )
    } else {
        first_gate.advance();
        let winner = tokio::time::timeout(DEADLINE, first)
            .await
            .expect("winner finishes")
            .expect("actor joins");
        second_gate.advance();
        (
            winner,
            tokio::time::timeout(DEADLINE, second)
                .await
                .expect("loser finishes")
                .expect("actor joins"),
            TaskState::Running,
        )
    };
    let winner = winner.expect("first committed CAS wins");
    assert!(matches!(loser, Err(StoreError::Conflict)));
    assert_eq!(winner.state_version, 1);
    assert_eq!(winner.attempt, u32::from(!second_wins));
    assert_eq!(winner.state, winning_state);
    assert_eq!(
        store.get_summary(record.id).await.expect("summary").expect("retained"),
        winner
    );
}

#[tokio::test]
/// Tests both commit orders against the actual memory-store CAS boundary.
async fn test_two_cas_callers_have_one_winner_in_either_order() {
    for second_wins in [false, true] {
        two_cas(Arc::new(MemoryTaskStore::new(8)), second_wins).await;
    }
}

#[cfg(feature = "sqlite")]
#[tokio::test]
/// Tests both CAS schedules against real owned SQLite transactions.
async fn test_sqlite_two_cas_callers_have_one_winner_in_either_order() {
    for second_wins in [false, true] {
        let directory = std::env::temp_dir().join(format!("qubit-task-interleaving-{}", TaskId::generate()));
        std::fs::create_dir(&directory).expect("case-owned directory");
        let store = Arc::new(SqliteTaskStore::open(directory.join("cas.sqlite")).expect("SQLite opens"));
        let epoch = store.acquire_owner().await.expect("owner");
        two_cas(store.clone(), second_wins).await;
        store.release_owner(epoch).await.expect("release");
        drop(store);
        std::fs::remove_dir_all(directory).expect("remove only this test's directory");
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Event {
    AcceptEntered,
    AcceptCommitted,
    TerminalEntered,
    TerminalCommitted,
    RetryWriteEntered,
    RetryFault,
    OwnerReleased,
}

/// Injects gates at service SPI boundaries. This fixture proves service
/// ordering. SQLite's internal completion barrier requires a separate private
/// worker test.
struct GatedStore {
    inner: MemoryTaskStore,
    accept: Option<Arc<StepGate>>,
    finalization: Mutex<Option<Arc<StepGate>>>,
    retry_fault: bool,
    events: Mutex<Vec<Event>>,
}

impl GatedStore {
    /// Creates an isolated recoverable test double with observable owner
    /// release.
    fn new(accept: Option<Arc<StepGate>>, finalization: Option<Arc<StepGate>>, retry_fault: bool) -> Self {
        Self {
            inner: MemoryTaskStore::new(8),
            accept,
            finalization: Mutex::new(finalization),
            retry_fault,
            events: Mutex::new(vec![]),
        }
    }

    /// Records a completed or entered SPI stage for exact ordering assertions.
    fn event(&self, event: Event) {
        self.events.lock().push(event);
    }
}

impl TaskStore for GatedStore {
    fn capabilities(&self) -> StoreCapabilities {
        StoreCapabilities {
            persistent_history: true,
            restart_recovery: true,
        }
    }

    fn accept<'a>(&'a self, id: TaskId, request: TaskRequest) -> TaskFuture<'a, Result<AcceptOutcome, StoreError>> {
        Box::pin(async move {
            if let Some(gate) = &self.accept {
                self.event(Event::AcceptEntered);
                gate.pause().await;
            }
            let result = self.inner.accept(id, request).await;
            if result.is_ok() {
                self.event(Event::AcceptCommitted);
            }
            result
        })
    }

    fn transition<'a>(&'a self, command: TransitionCommand) -> TaskFuture<'a, Result<TaskSummary, StoreError>> {
        Box::pin(async move {
            let terminal = matches!(command.state, TaskState::Succeeded);
            let retry = matches!(command.state, TaskState::Queued) && command.expected_attempt > 0;
            if terminal || retry {
                let gate = self.finalization.lock().take();
                if let Some(gate) = gate {
                    self.event(if retry {
                        Event::RetryWriteEntered
                    } else {
                        Event::TerminalEntered
                    });
                    gate.pause().await;
                }
            }
            if retry && self.retry_fault {
                self.event(Event::RetryFault);
                return Err(StoreError::Failure("gated retry write failed".into()));
            }
            let result = self.inner.transition(command).await;
            if terminal && result.is_ok() {
                self.event(Event::TerminalCommitted);
            }
            result
        })
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
    fn get_summary<'a>(&'a self, id: TaskId) -> TaskFuture<'a, Result<Option<TaskSummary>, StoreError>> {
        self.inner.get_summary(id)
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
    fn has_unfinished_over_limit<'a>(&'a self, limit: usize) -> TaskFuture<'a, Result<bool, StoreError>> {
        self.inner.has_unfinished_over_limit(limit)
    }
    fn acquire_owner<'a>(&'a self) -> TaskFuture<'a, Result<OwnerEpoch, StoreError>> {
        Box::pin(async { Ok(OwnerEpoch(1)) })
    }
    fn scan_unfinished<'a>(&'a self, _cursor: Option<TaskCursor>) -> TaskFuture<'a, Result<RecoveryPage, StoreError>> {
        Box::pin(async {
            Ok(RecoveryPage {
                tasks: vec![],
                next: None,
            })
        })
    }
    fn release_owner<'a>(&'a self, _epoch: OwnerEpoch) -> TaskFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            self.event(Event::OwnerReleased);
            Ok(())
        })
    }
}

struct Handler {
    retry: bool,
}

impl TaskHandler for Handler {
    fn descriptor(&self) -> TaskHandlerDescriptor {
        TaskHandlerDescriptor {
            task_type: "interleaving".into(),
            version: "1".into(),
        }
    }
    fn run<'a>(&'a self, _payload: &'a [u8], _context: TaskContext) -> TaskFuture<'a, TaskRunResult> {
        Box::pin(async move {
            if self.retry {
                Err(TaskRunError {
                    category: "temporary".into(),
                    message: "retry".into(),
                    retryable: true,
                })
            } else {
                Ok(TaskRunOutcome::Succeeded(TaskOutput { summary: vec![42] }))
            }
        })
    }
}

#[tokio::test]
/// Cancelling the submit caller leaves admission owned by the service until its
/// write completes.
async fn test_cancelled_admission_caller_shutdown_releases_owner_after_write() {
    let (gate, started) = StepGate::new();
    let store = Arc::new(GatedStore::new(Some(gate.clone()), None, false));
    let service = TaskExecutionServiceBuilder::default()
        .runtime_handle(tokio::runtime::Handle::current())
        .store(store.clone())
        .build()
        .await
        .expect("service");
    let submitting_service = service.clone();
    let caller = tokio::spawn(async move {
        submitting_service
            .submit(TaskRequest::new("missing", "1", vec![]).with_idempotency_key("admission"))
            .await
    });
    entered(started).await;
    caller.abort();
    assert!(caller.await.expect_err("caller cancelled").is_cancelled());
    let closing = service.shutdown();
    tokio::pin!(closing);
    assert!(poll!(closing.as_mut()).is_pending());
    assert_eq!(*store.events.lock(), vec![Event::AcceptEntered]);
    gate.advance();
    tokio::time::timeout(DEADLINE, closing)
        .await
        .expect("shutdown drains admission")
        .expect("clean shutdown");
    let events = store.events.lock().clone();
    assert_eq!(
        events,
        vec![Event::AcceptEntered, Event::AcceptCommitted, Event::OwnerReleased]
    );
    assert!(
        store
            .inner
            .get_summary_by_idempotency_key("admission")
            .await
            .expect("lookup")
            .is_some()
    );
}

#[tokio::test]
/// An older terminal CAS retries after cancellation updates the running
/// revision.
async fn test_cancel_request_before_terminal_commit_preserves_finalizer_progress() {
    let (gate, started) = StepGate::new();
    let store = Arc::new(GatedStore::new(None, Some(gate.clone()), false));
    let service = TaskExecutionServiceBuilder::default()
        .runtime_handle(tokio::runtime::Handle::current())
        .store(store.clone())
        .register_handler(Arc::new(Handler { retry: false }))
        .expect("handler")
        .build()
        .await
        .expect("service");
    let record = service
        .submit(TaskRequest::new("interleaving", "1", vec![]).with_idempotency_key("terminal"))
        .await
        .expect("accepted");
    entered(started).await;
    assert_eq!(
        service.cancel(record.id).await.expect("cancel request"),
        CancelOutcome::CancellationRequested
    );
    let running = store
        .inner
        .get_summary(record.id)
        .await
        .expect("summary")
        .expect("retained");
    assert_eq!(running.state, TaskState::Running);
    assert!(running.cancel_requested);
    assert_eq!(running.state_version, 2);
    let closing = service.shutdown();
    tokio::pin!(closing);
    assert!(poll!(closing.as_mut()).is_pending());
    assert!(!store.events.lock().contains(&Event::OwnerReleased));
    gate.advance();
    tokio::time::timeout(DEADLINE, closing)
        .await
        .expect("shutdown drains finalizer")
        .expect("clean shutdown");
    let summary = store
        .inner
        .get_summary(record.id)
        .await
        .expect("summary")
        .expect("retained");
    assert_eq!(summary.state, TaskState::Succeeded);
    assert_eq!(summary.state_version, 3);
    assert_eq!(summary.attempt, 1);
    assert!(summary.cancel_requested);
    assert_eq!(
        *store.events.lock(),
        vec![
            Event::AcceptCommitted,
            Event::TerminalEntered,
            Event::TerminalCommitted,
            Event::OwnerReleased
        ]
    );
}

#[tokio::test]
/// The reverse schedule observes a committed terminal revision before
/// cancellation.
async fn test_terminal_commit_before_cancel_request_returns_already_terminal() {
    let (gate, started) = StepGate::new();
    let store = Arc::new(GatedStore::new(None, Some(gate.clone()), false));
    let service = TaskExecutionServiceBuilder::default()
        .runtime_handle(tokio::runtime::Handle::current())
        .store(store.clone())
        .register_handler(Arc::new(Handler { retry: false }))
        .expect("handler")
        .build()
        .await
        .expect("service");
    let record = service
        .submit(TaskRequest::new("interleaving", "1", vec![]).with_idempotency_key("terminal-first"))
        .await
        .expect("accepted");
    entered(started).await;
    gate.advance();
    let terminal = tokio::time::timeout(DEADLINE, service.wait(record.id))
        .await
        .expect("terminal commit")
        .expect("terminal summary");
    assert_eq!(terminal.state, TaskState::Succeeded);
    assert_eq!(
        service.cancel(record.id).await.expect("cancel after terminal"),
        CancelOutcome::AlreadyTerminal
    );
    assert_eq!(
        store
            .inner
            .get_summary(record.id)
            .await
            .expect("summary")
            .expect("retained"),
        terminal
    );
    tokio::time::timeout(DEADLINE, service.shutdown())
        .await
        .expect("shutdown drains")
        .expect("shutdown");
    assert_eq!(
        *store.events.lock(),
        vec![
            Event::AcceptCommitted,
            Event::TerminalEntered,
            Event::TerminalCommitted,
            Event::OwnerReleased
        ]
    );
}

#[tokio::test]
/// A fault after retry reservation never commits Queued or permits early owner
/// release.
async fn test_retry_reservation_store_fault_drains_before_owner_release() {
    let (gate, started) = StepGate::new();
    let store = Arc::new(GatedStore::new(None, Some(gate.clone()), true));
    let service = TaskExecutionServiceBuilder::default()
        .runtime_handle(tokio::runtime::Handle::current())
        .store(store.clone())
        .queue_capacity(1)
        .register_handler(Arc::new(Handler { retry: true }))
        .expect("handler")
        .build()
        .await
        .expect("service");
    let record = service
        .submit(TaskRequest::new("interleaving", "1", vec![]).with_idempotency_key("retry"))
        .await
        .expect("accepted");
    entered(started).await;
    assert_eq!(
        store
            .inner
            .get_summary(record.id)
            .await
            .expect("summary")
            .expect("retained")
            .state,
        TaskState::Running
    );
    let closing = service.shutdown();
    tokio::pin!(closing);
    assert!(poll!(closing.as_mut()).is_pending());
    assert!(!store.events.lock().contains(&Event::OwnerReleased));
    gate.advance();
    assert!(
        matches!(tokio::time::timeout(DEADLINE, closing).await.expect("fault shutdown drains"), Err(TaskServiceError::StoreUnavailable(message)) if message.contains("gated retry write failed"))
    );
    assert!(matches!(
        tokio::time::timeout(DEADLINE, service.wait(record.id))
            .await
            .expect("fault wakes waiter"),
        Err(TaskServiceError::StoreUnavailable(_))
    ));
    let summary = store
        .inner
        .get_summary(record.id)
        .await
        .expect("summary")
        .expect("retained");
    assert_eq!(summary.state, TaskState::Running);
    assert_eq!(summary.state_version, 1);
    assert_eq!(summary.attempt, 1);
    assert_eq!(
        *store.events.lock(),
        vec![
            Event::AcceptCommitted,
            Event::RetryWriteEntered,
            Event::RetryFault,
            Event::OwnerReleased
        ]
    );
}
