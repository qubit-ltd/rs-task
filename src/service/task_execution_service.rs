// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
// qubit-style: allow multiple-public-types
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use futures::FutureExt;
use futures::future::select;
use parking_lot::Mutex;
use tokio::pin;
use tokio::runtime;
use tokio::sync;
use tokio::sync::Notify;
use tokio::sync::oneshot;
use tokio::task;
use tokio::time;

use super::admission_budget::AdmissionBudget;
use super::admission_budget::AdmissionBudgetError;
use super::admission_budget::AdmissionReservation;
use super::admission_gate::AdmissionGate;
use super::local_task_handle::LocalTaskHandle;
use super::local_task_outcome::LocalTaskOutcome;
use super::local_task_outcome::adapt_local_outcome;
use super::local_task_result_error::LocalTaskResultError;
use super::retry_policy::RetryPolicy;
use super::scheduler_queue::SchedulerQueue;
#[cfg(feature = "event-bus")]
use super::task_event_notification_stats::TaskEventNotificationStats;
#[cfg(feature = "event-bus")]
use super::task_event_publisher::TaskEventPublisher;
use super::task_execution_service_builder::TaskExecutionServiceBuilder;
use super::task_execution_service_builder::TaskServiceBuildError;
use super::task_wait_registry::TaskWaitRegistry;
use crate::engine::EngineError;
use crate::engine::ExecutionOutcome;
use crate::engine::TaskExecutionEngine;
#[cfg(feature = "event-bus")]
use crate::event::TaskEvent;
use crate::handler::LocalTaskHandler;
use crate::handler::TaskContext;
use crate::handler::TaskHandler;
use crate::handler::TaskHandlerDescriptor;
use crate::handler::TaskHandlerRegistry;
use crate::handler::TaskRunOutcome;
use crate::model::AcceptOutcome;
use crate::model::MAX_IDEMPOTENCY_KEY_BYTES;
use crate::model::MAX_TASK_DIAGNOSTIC_CATEGORY_BYTES;
use crate::model::MAX_TASK_DIAGNOSTIC_MESSAGE_BYTES;
use crate::model::MAX_TASK_OUTPUT_SUMMARY_BYTES;
use crate::model::OwnerEpoch;
use crate::model::ResourceCapacity;
use crate::model::ResourceRequest;
use crate::model::StoreCapabilities;
use crate::model::TaskId;
use crate::model::TaskOutput;
use crate::model::TaskPage;
use crate::model::TaskQuery;
use crate::model::TaskRecord;
use crate::model::TaskRequest;
use crate::model::TaskState;
use crate::model::TaskStateCounts;
use crate::model::TaskStateKind;
use crate::model::TaskStats;
use crate::model::TaskSummary;
use crate::model::TransitionCommand;
use crate::model::checked_page_size;
use crate::scheduling::QueueSnapshot;
use crate::scheduling::QueuedTask;
use crate::scheduling::SchedulingPolicy;
use crate::store::StoreError;
use crate::store::TaskStore;

/// Effective store capabilities and local-closure support.
///
/// # Examples
///
/// ```
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use qubit_task::TaskExecutionService;
///
/// let service = TaskExecutionService::in_memory().await?;
/// assert!(service.capabilities().submit_local);
/// service.shutdown().await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaskServiceCapabilities {
    /// Capabilities declared by the selected store.
    pub store: StoreCapabilities,
    /// Whether local in-process handlers can be submitted.
    pub submit_local: bool,
}

/// Failure reported by a service operation.
///
/// # Examples
///
/// ```
/// use qubit_task::service::TaskServiceError;
///
/// let error = TaskServiceError::QueueFull;
/// assert_eq!(error.to_string(), "task queue is full");
/// ```
#[derive(Debug, thiserror::Error)]
#[must_use]
pub enum TaskServiceError {
    /// The selected store failed an operation.
    #[error(transparent)]
    Store(
        /// Underlying storage failure.
        #[from]
        StoreError,
    ),
    /// The configured queue has no remaining waiting capacity.
    #[error("task queue is full")]
    QueueFull,
    /// The request payload would exceed the configured in-flight byte budget.
    #[error(
        "in-flight task payload budget exceeded: requested {requested_bytes} bytes, {available_bytes} bytes available"
    )]
    PayloadBudgetExceeded {
        /// Payload bytes in the rejected submission.
        requested_bytes: usize,
        /// Payload bytes remaining when the submission was checked.
        available_bytes: usize,
    },
    /// The configured number of detached admission workers is already in
    /// flight.
    #[error("in-flight task submission limit reached ({limit})")]
    SubmissionLimitExceeded {
        /// Maximum number of concurrent admission workers.
        limit: usize,
    },
    /// The request exceeds available configured capacity.
    #[error("task request cannot be satisfied by configured resources")]
    Unsatisfiable,
    /// The request contains invalid metadata or an oversized payload.
    #[error("invalid task request: {0}")]
    InvalidRequest(
        /// Validation diagnostic describing the rejected request field.
        String,
    ),
    /// The requested task is blocked pending intervention.
    #[error("task is blocked and requires intervention")]
    Blocked,
    /// The expected record revision exists, but its lifecycle is not blocked.
    #[error("task is not blocked (current state: {actual:?})")]
    NotBlocked {
        /// Lifecycle state observed when the operation was rejected.
        actual: TaskStateKind,
    },
    /// The task used all configured execution attempts and cannot be requeued.
    #[error("task exhausted its execution attempt budget ({attempts}/{limit})")]
    AttemptsExhausted {
        /// Number of attempts already started.
        attempts: u32,
        /// Maximum attempts configured for the service.
        limit: u32,
    },
    /// New task submissions have been stopped.
    #[error("task execution service is shutting down")]
    ShuttingDown,
    /// The caller's shutdown deadline expired while accepted work was draining.
    #[error("task execution service did not shut down before the deadline")]
    ShutdownTimedOut,
    /// A persistence failure suspended task acceptance and scheduling.
    #[error("task execution service is paused after a task store failure: {0}")]
    StoreUnavailable(
        /// First store failure retained by the service.
        String,
    ),
    /// The scheduler or execution engine cannot accept or start more work.
    #[error("task execution scheduler is unavailable: {0}")]
    SchedulerUnavailable(
        /// Scheduler or engine failure retained by the service.
        String,
    ),
    /// The task notification publisher failed while draining during shutdown.
    #[error("task notification publisher failed to close: {0}")]
    NotificationClose(
        /// Notification publisher close or worker failure diagnostic.
        String,
    ),
    /// No handler matches the submitted type and exact version.
    #[error("no handler registered for `{task_type}` version `{version}`")]
    MissingHandler {
        /// Task type requested by the submitted record.
        task_type: String,
        /// Exact version requested by the submitted record.
        version: String,
    },
    /// Reconstructable storage cannot accept a local closure.
    #[error("local closure submission is unavailable with a restart-recoverable store")]
    UnsupportedCapability,
}

/// Outcome of a cancellation request.
///
/// # Examples
///
/// ```
/// use qubit_task::service::CancelOutcome;
///
/// let outcome = CancelOutcome::AlreadyTerminal;
/// assert!(matches!(outcome, CancelOutcome::AlreadyTerminal));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub enum CancelOutcome {
    /// The task was cancelled while it was queued.
    CancelledBeforeStart,
    /// Cooperative cancellation was signalled to a running handler.
    CancellationRequested,
    /// The task had already reached a terminal state.
    AlreadyTerminal,
}

/// Components and synchronization state shared by service handle clones.
pub(crate) struct ServiceCore {
    /// Authoritative lifecycle and request store.
    pub(crate) store: Arc<dyn TaskStore>,
    /// Backend that atomically reserves resources and starts handlers.
    pub(crate) engine: Arc<dyn TaskExecutionEngine>,
    /// Strategy used to order scheduler candidates.
    pub(crate) policy: Arc<dyn SchedulingPolicy>,
    /// Runtime used for service-owned asynchronous workers.
    pub(crate) runtime_handle: runtime::Handle,
    /// Exact-version handler registry.
    pub(crate) handlers: TaskHandlerRegistry,
    /// Maximum accepted queue entries.
    pub(crate) queue_capacity: usize,
    /// Maximum candidates examined by each scheduler pass.
    pub(crate) scan_budget: usize,
    /// Maximum attempts allowed for each task.
    pub(crate) max_attempts: u32,
    /// Delay schedule used for retryable failures.
    pub(crate) retry_policy: RetryPolicy,
    /// Concurrent execution slots.
    pub(crate) running_slots: Arc<sync::Semaphore>,
    /// Ready and delayed task queues.
    pub(crate) queue: Mutex<SchedulerQueue>,
    /// Number of accepted tasks occupying queue capacity.
    pub(crate) queue_count: AtomicUsize,
    /// Process-local handlers not persisted in the store.
    pub(crate) local_handlers: Mutex<HashMap<TaskId, Arc<dyn TaskHandler>>>,
    /// Result finalization senders for process-local task handles.
    pub(crate) local_finalizations: Mutex<HashMap<TaskId, oneshot::Sender<Result<TaskState, LocalTaskResultError>>>>,
    /// Cooperative cancellation signals indexed by task ID.
    pub(crate) cancellations: Mutex<HashMap<TaskId, RunningCancellation>>,
    /// Wakes the scheduler and shutdown coordinator after state changes.
    pub(crate) changed: Notify,
    /// Per-task notification registry used by waiters.
    pub(super) wait_registry: Arc<TaskWaitRegistry>,
    /// Serializes lifecycle event publication across transitions.
    pub(crate) transition_event_lock: sync::RwLock<()>,
    /// Prevents new admissions after shutdown starts.
    pub(super) admission: AdmissionGate,
    /// Bounds detached admission workers and payload retention.
    pub(super) admission_budget: Arc<AdmissionBudget>,
    /// Exclusive recoverable-store ownership epoch, if supported.
    pub(crate) owner: Option<OwnerEpoch>,
    /// First latched store failure suspending service progress.
    pub(crate) store_fault: Mutex<Option<String>>,
    /// First latched scheduler failure suspending service progress.
    pub(crate) scheduler_fault: Mutex<Option<String>>,
    /// Running attempts whose completion has not been finalized.
    pub(crate) attempts_in_flight: AtomicUsize,
    /// Wakes shutdown waiters when attempt count changes.
    pub(crate) attempts_changed: Notify,
    /// Whether the scheduler worker has exited.
    pub(crate) scheduler_finished: AtomicBool,
    /// Wakes shutdown waiters when the scheduler exits.
    pub(crate) scheduler_finished_notify: Notify,
    /// Optional bounded lifecycle event publisher.
    #[cfg(feature = "event-bus")]
    pub(super) event_bus: Option<TaskEventPublisher>,
}

/// Signal belonging to one specific execution attempt of a task.
pub(crate) struct RunningCancellation {
    /// Execution generation owning this cancellation signal.
    attempt: u32,
    /// Shared signal observed by the handler and engine.
    signal: Arc<AtomicBool>,
}

/// Tracks the lifetime of all public service handles and their admission
/// workers.
struct ServiceHandleLease {
    /// Weak reference used to start shutdown after the final handle is dropped.
    core: std::sync::Weak<ServiceCore>,
}

impl Drop for ServiceHandleLease {
    /// Starts asynchronous service shutdown when the last handle lease is gone.
    fn drop(&mut self) {
        if let Some(core) = self.core.upgrade() {
            begin_shutdown_core(core);
        }
    }
}

/// Single service facade over volatile or restart-recoverable components.
///
/// The service owns admission and scheduling for its components. Dropping the
/// last handle starts an asynchronous drain; call [`shutdown`](Self::shutdown)
/// when the caller must observe its result. Keep an injected runtime alive
/// until that drain finishes.
///
/// # Examples
///
/// ```
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     use qubit_task::TaskExecutionService;
///
///     let service = TaskExecutionService::in_memory().await?;
///     let capabilities = service.capabilities();
///     assert!(!capabilities.store.restart_recovery);
///     service.shutdown().await?;
///     Ok(())
/// }
/// ```
#[derive(Clone)]
pub struct TaskExecutionService {
    /// Starts asynchronous shutdown when the final public handle is dropped.
    _lease: Arc<ServiceHandleLease>,
    /// Shared components and state used by every handle clone.
    pub(crate) core: Arc<ServiceCore>,
}

impl TaskExecutionService {
    /// Builds an explicitly volatile, single-process service.
    ///
    /// # Returns
    ///
    /// A service backed by bounded in-memory history and local components.
    ///
    /// # Errors
    ///
    /// Returns a build error if the default service cannot be assembled.
    pub async fn in_memory() -> Result<Self, TaskServiceBuildError> {
        TaskExecutionServiceBuilder::in_memory().build().await
    }

    /// Reports the selected store's history and recovery guarantees.
    ///
    /// # Returns
    ///
    /// The selected store capabilities and local closure availability.
    #[must_use]
    pub fn capabilities(&self) -> TaskServiceCapabilities {
        let store = self.core.store.capabilities();
        TaskServiceCapabilities {
            store,
            submit_local: !store.restart_recovery,
        }
    }

    /// Returns the diagnostic that suspended storage-dependent progress, if
    /// any.
    ///
    /// # Returns
    ///
    /// The first latched store diagnostic, or `None` if storage remains
    /// available.
    #[must_use]
    pub fn last_store_error(&self) -> Option<String> {
        self.core.store_fault.lock().clone()
    }

    /// Returns the diagnostic from a scheduler panic, if one occurred.
    ///
    /// # Returns
    ///
    /// The first scheduler diagnostic, or `None` if scheduling remains
    /// available.
    #[must_use]
    pub fn last_scheduler_error(&self) -> Option<String> {
        self.core.scheduler_fault.lock().clone()
    }

    /// Returns lifecycle notification admission counters when a bus is
    /// configured.
    ///
    /// # Returns
    ///
    /// A counter snapshot when event publication is configured.
    #[cfg(feature = "event-bus")]
    #[must_use]
    pub fn notification_stats(&self) -> Option<TaskEventNotificationStats> {
        self.core.event_bus.as_ref().map(TaskEventPublisher::stats)
    }

    /// Accepts a reconstructable request and returns its stable task record.
    ///
    /// The request must include a non-empty idempotency key that the caller
    /// created and retained before submission. Repeating the same request with
    /// the same key resolves to the accepted record; reusing the key for a
    /// different request returns an idempotency conflict.
    ///
    /// # Parameters
    ///
    /// * `request` - Bounded request with a caller-retained idempotency key.
    ///
    /// # Returns
    ///
    /// The accepted record or an identical record already retained.
    ///
    /// # Errors
    ///
    /// Returns validation, idempotency, capacity, shutdown, or store errors.
    pub async fn submit(&self, request: TaskRequest) -> Result<TaskRecord, TaskServiceError> {
        let reservation = self.reserve_admission(request.payload.len())?;
        let service = self.clone();
        await_admission(
            self.core
                .runtime_handle
                .spawn(async move { service.submit_admitted(request, reservation).await }),
        )
        .await
    }

    /// Submits a process-local closure when the selected store cannot promise
    /// restart recovery.
    ///
    /// # Type Parameters
    ///
    /// * `F` - One-shot closure type.
    /// * `R` - Process-local success value.
    /// * `E` - Process-local application error.
    ///
    /// # Parameters
    ///
    /// * `task` - One-shot closure returning a typed local outcome.
    ///
    /// # Returns
    ///
    /// A handle that yields the closure's process-local success or error value
    /// after persisted finalization.
    ///
    /// # Errors
    ///
    /// Returns an error if closure submission is unsupported or service
    /// admission, validation, or persistence fails.
    pub async fn submit_local<F, R, E>(&self, task: F) -> Result<LocalTaskHandle<R, E>, TaskServiceError>
    where
        F: FnOnce(TaskContext) -> LocalTaskOutcome<R, E> + Send + 'static,
        R: Send + 'static,
        E: std::fmt::Display + Send + 'static,
    {
        let reservation = self.reserve_admission(0)?;
        let service = self.clone();
        await_admission(
            self.core
                .runtime_handle
                .spawn(async move { service.submit_local_admitted(task, reservation).await }),
        )
        .await
    }

    /// Loads the complete retained task, including its payload.
    ///
    /// # Parameters
    ///
    /// * `id` - Stable task identifier.
    ///
    /// # Returns
    ///
    /// The record with payload when retained, or `None` otherwise.
    ///
    /// # Errors
    ///
    /// Returns a service error when the store read fails.
    pub async fn get(&self, id: TaskId) -> Result<Option<TaskRecord>, TaskServiceError> {
        self.core
            .store
            .get(id)
            .await
            .map_err(|error| self.handle_store_error(error))
    }

    /// Loads lifecycle metadata without retrieving the potentially large
    /// payload.
    ///
    /// # Parameters
    ///
    /// * `id` - Stable task identifier.
    ///
    /// # Returns
    ///
    /// The payload-free summary when the task is retained, or `None` otherwise.
    ///
    /// # Errors
    ///
    /// Returns a service error when the store cannot read the summary.
    pub async fn get_summary(&self, id: TaskId) -> Result<Option<TaskSummary>, TaskServiceError> {
        self.core
            .store
            .get_summary(id)
            .await
            .map_err(|error| self.handle_store_error(error))
    }

    /// Finds a retained task by the caller-supplied idempotency key.
    ///
    /// A missing record only means that the key is not committed at the time
    /// of this lookup. A submission worker may still be accepting it; retry
    /// `submit` with the same key and identical request to recover its record.
    ///
    /// # Parameters
    ///
    /// * `key` - Caller-retained idempotency key.
    ///
    /// # Returns
    ///
    /// The matching retained task, or `None` when not yet committed or absent.
    ///
    /// # Errors
    ///
    /// Returns `InvalidRequest` for an empty or oversized key, or a store
    /// error if lookup fails.
    pub async fn get_by_idempotency_key(&self, key: &str) -> Result<Option<TaskRecord>, TaskServiceError> {
        if key.is_empty() || key.len() > MAX_IDEMPOTENCY_KEY_BYTES {
            return Err(TaskServiceError::InvalidRequest(
                "idempotency key must contain between 1 and 256 bytes".into(),
            ));
        }
        self.core
            .store
            .get_by_idempotency_key(key)
            .await
            .map_err(|error| self.handle_store_error(error))
    }

    /// Returns a bounded page of retained history.
    ///
    /// # Parameters
    ///
    /// * `query` - State, correlation, cursor, and page-size filters.
    ///
    /// # Returns
    ///
    /// A bounded page of payload-free task summaries.
    ///
    /// # Errors
    ///
    /// Returns `InvalidRequest` for an invalid page size or a store error.
    pub async fn list(&self, query: TaskQuery) -> Result<TaskPage, TaskServiceError> {
        checked_page_size(query.limit)?;
        self.core
            .store
            .list(query)
            .await
            .map_err(|error| self.handle_store_error(error))
    }

    /// Deletes a bounded number of terminal records accepted before a cutoff.
    ///
    /// # Parameters
    ///
    /// * `accepted_before_ms` - Exclusive Unix epoch millisecond cutoff.
    /// * `max_rows` - Maximum number of terminal records to delete in this
    ///   call.
    ///
    /// # Returns
    ///
    /// The number of records deleted by the selected store.
    ///
    /// # Errors
    ///
    /// Returns `ShuttingDown` after service shutdown starts, `Store` when the
    /// store rejects pruning or encounters an error, or `StoreUnavailable` if
    /// a previous store failure suspended the service.
    pub async fn prune_terminal_before(
        &self,
        accepted_before_ms: u64,
        max_rows: NonZeroUsize,
    ) -> Result<usize, TaskServiceError> {
        if let Some(error) = self.last_store_error() {
            return Err(TaskServiceError::StoreUnavailable(error));
        }
        let _permit = self.core.admission.enter()?;
        self.core
            .store
            .prune_terminal_before(accepted_before_ms, max_rows)
            .await
            .map_err(|error| self.handle_store_error(error))
    }

    /// Counts visible task states and reports current resource use.
    ///
    /// # Returns
    ///
    /// Current retained state counts and engine resource usage.
    ///
    /// # Errors
    ///
    /// Returns a store error if state counts cannot be read.
    pub async fn stats(&self) -> Result<TaskStats, TaskServiceError> {
        task_stats(&self.core).await.map_err(|error| match error {
            TaskServiceError::Store(store_error) => self.handle_store_error(store_error),
            other => other,
        })
    }

    /// Cancels queued or blocked work, or persists a cooperative cancellation
    /// request for running work before signalling its local handler.
    /// A running handler decides whether to acknowledge cancellation; a
    /// concurrent terminal transition returns `AlreadyTerminal`.
    ///
    /// # Parameters
    ///
    /// * `id` - Stable task identity to cancel.
    ///
    /// # Returns
    ///
    /// Whether cancellation completed before start, was requested, or the task
    /// was already terminal.
    ///
    /// # Errors
    ///
    /// Returns `NotFound`, shutdown, store, or scheduler errors when the
    /// request cannot be applied.
    pub async fn cancel(&self, id: TaskId) -> Result<CancelOutcome, TaskServiceError> {
        let _permit = self.core.admission.enter()?;
        let mut record = self
            .core
            .store
            .get_summary(id)
            .await
            .map_err(|error| self.handle_store_error(error))?
            .ok_or(StoreError::NotFound)?;
        loop {
            if record.state.is_terminal() {
                return Ok(CancelOutcome::AlreadyTerminal);
            }
            let queued = matches!(record.state, TaskState::Queued);
            let blocked = matches!(record.state, TaskState::Blocked { .. });
            let updated = transition(
                &self.core,
                &record,
                if queued || blocked {
                    TaskState::Cancelled
                } else {
                    TaskState::Running
                },
                None,
                if queued || blocked {
                    Vec::new()
                } else {
                    record.assigned_resources.clone()
                },
                !queued && !blocked,
            )
            .await;
            match updated {
                Ok(updated) => {
                    let local_sender = if queued || blocked {
                        self.core.local_finalizations.lock().remove(&id)
                    } else {
                        None
                    };
                    if queued {
                        let removed = self.core.queue.lock().remove(id);
                        if removed {
                            self.release_queue_slot();
                        }
                    }
                    if queued || blocked {
                        self.core.local_handlers.lock().remove(&id);
                    } else if let Some(current) = self.core.cancellations.lock().get(&id)
                        && current.attempt == updated.attempt
                    {
                        current.signal.store(true, Ordering::Release);
                    }
                    self.core.changed.notify_waiters();
                    if let Some(sender) = local_sender {
                        let _ = sender.send(Ok(TaskState::Cancelled));
                    }
                    return Ok(if queued || blocked {
                        CancelOutcome::CancelledBeforeStart
                    } else {
                        CancelOutcome::CancellationRequested
                    });
                }
                Err(StoreError::Conflict) => {
                    let latest = self
                        .core
                        .store
                        .get_summary(id)
                        .await
                        .map_err(|error| self.handle_store_error(error))?
                        .ok_or(StoreError::NotFound)?;
                    if latest.state.is_terminal() {
                        return Ok(CancelOutcome::AlreadyTerminal);
                    }
                    if latest.state_version <= record.state_version {
                        return Err(StoreError::Conflict.into());
                    }
                    record = latest;
                }
                Err(error) => return Err(self.handle_store_error(error)),
            }
        }
    }

    /// Requeues a blocked task after external intervention.
    ///
    /// # Parameters
    ///
    /// * `id` - Stable identity of the blocked task.
    ///
    /// # Returns
    ///
    /// The updated queued task summary.
    ///
    /// # Errors
    ///
    /// Returns `Blocked`, `AttemptsExhausted`, shutdown, capacity, or store
    /// errors if it cannot be requeued.
    pub async fn retry_blocked(&self, id: TaskId) -> Result<TaskSummary, TaskServiceError> {
        let service = self.clone();
        await_admission(
            self.core
                .runtime_handle
                .spawn(async move { service.retry_blocked_admitted(id).await }),
        )
        .await
    }

    /// Cancels an operator-selected blocked task if its state revision is
    /// unchanged.
    ///
    /// # Parameters
    ///
    /// * `id` - Stable identifier of the blocked task.
    /// * `expected_version` - State version observed during operator review.
    ///
    /// # Returns
    ///
    /// The committed cancelled summary.
    ///
    /// # Errors
    ///
    /// Returns `Store(Conflict)` if the record changed after review,
    /// `NotBlocked` if the matching revision is not blocked, or a store or
    /// shutdown error if the operation cannot be completed.
    pub async fn abandon_blocked(&self, id: TaskId, expected_version: u64) -> Result<TaskSummary, TaskServiceError> {
        if let Some(error) = self.last_store_error() {
            return Err(TaskServiceError::StoreUnavailable(error));
        }
        let _permit = self.core.admission.enter()?;
        let record = self
            .core
            .store
            .get_summary(id)
            .await
            .map_err(|error| self.handle_store_error(error))?
            .ok_or(StoreError::NotFound)?;
        if record.state_version != expected_version {
            return Err(StoreError::Conflict.into());
        }
        if !matches!(record.state, TaskState::Blocked { .. }) {
            return Err(TaskServiceError::NotBlocked {
                actual: record.state.kind(),
            });
        }
        let updated = self
            .core
            .store
            .abandon_blocked(id, expected_version)
            .await
            .map_err(|error| self.handle_store_error(error))?;
        publish_record(&self.core, &updated);
        self.core.changed.notify_waiters();
        self.core.wait_registry.notify(id);
        Ok(updated)
    }

    /// Resolves when the task becomes terminal; returns an error if it becomes
    /// blocked.
    ///
    /// # Parameters
    ///
    /// * `id` - Stable task identity to observe.
    ///
    /// # Returns
    ///
    /// The terminal task summary.
    ///
    /// # Errors
    ///
    /// Returns `NotFound`, `Blocked`, `StoreUnavailable`, or
    /// `SchedulerUnavailable` while waiting.
    pub async fn wait(&self, id: TaskId) -> Result<TaskSummary, TaskServiceError> {
        let subscription = self.core.wait_registry.subscribe(id);
        loop {
            let notified = subscription.notified();
            pin!(notified);
            notified.as_mut().enable();
            if let Some(error) = self.last_store_error() {
                return Err(TaskServiceError::StoreUnavailable(error));
            }
            if let Some(error) = self.last_scheduler_error() {
                return Err(TaskServiceError::SchedulerUnavailable(error));
            }
            let record = self
                .core
                .store
                .get_summary(id)
                .await
                .map_err(|error| self.handle_store_error(error))?
                .ok_or(StoreError::NotFound)?;
            if record.state.is_terminal() {
                return Ok(record);
            }
            if matches!(record.state, TaskState::Blocked { .. }) {
                return Err(TaskServiceError::Blocked);
            }
            notified.await;
        }
    }

    /// Stops accepting new work and waits for all accepted work to settle.
    ///
    /// # Returns
    ///
    /// Success after accepted work drains and store ownership is released.
    ///
    /// # Errors
    ///
    /// Returns the shared shutdown failure, if draining or cleanup fails.
    pub async fn shutdown(&self) -> Result<(), TaskServiceError> {
        begin_shutdown_core(Arc::clone(&self.core));
        self.core.admission.wait_closed().await
    }

    /// Stops accepting work and waits until `deadline`; the shared shutdown
    /// coordinator continues draining if this caller times out.
    ///
    /// # Parameters
    ///
    /// * `deadline` - Absolute Tokio instant by which this caller must finish.
    ///
    /// # Returns
    ///
    /// Success when shared shutdown finishes before the deadline.
    ///
    /// # Errors
    ///
    /// Returns `ShutdownTimedOut` when the deadline expires, otherwise the
    /// shared shutdown error if draining fails.
    pub async fn shutdown_until(&self, deadline: time::Instant) -> Result<(), TaskServiceError> {
        begin_shutdown_core(Arc::clone(&self.core));
        time::timeout_at(deadline, self.core.admission.wait_closed())
            .await
            .map_err(|_| TaskServiceError::ShutdownTimedOut)?
    }

    /// Finishes request acceptance in a background worker that holds an
    /// admission permit even if the caller is cancelled.
    ///
    /// # Parameters
    ///
    /// * `request` - Validated reconstructable task request.
    /// * `reservation` - In-flight worker and payload budget reservation.
    ///
    /// # Returns
    ///
    /// The accepted record or identical retained record.
    ///
    /// # Errors
    ///
    /// Returns admission, validation, store, or queue capacity errors.
    async fn submit_admitted(
        &self,
        request: TaskRequest,
        reservation: AdmissionReservation,
    ) -> Result<TaskRecord, TaskServiceError> {
        if let Some(error) = self.last_scheduler_error() {
            return Err(TaskServiceError::SchedulerUnavailable(error));
        }
        if let Some(error) = self.last_store_error() {
            return Err(TaskServiceError::StoreUnavailable(error));
        }
        let _permit = self.core.admission.enter()?;
        let capacity = self.core.engine.capacity().capacity;
        validate_request(&request, &capacity)?;
        let idempotency_key = request.idempotency_key.as_deref().ok_or_else(|| {
            TaskServiceError::InvalidRequest("task submission requires a non-empty idempotency key".into())
        })?;
        if idempotency_key.is_empty() {
            return Err(TaskServiceError::InvalidRequest(
                "task submission requires a non-empty idempotency key".into(),
            ));
        }
        if let Some(record) = self
            .core
            .store
            .get_by_idempotency_key(idempotency_key)
            .await
            .map_err(|error| self.handle_store_error(error))?
        {
            return if record.request == request {
                Ok(record)
            } else {
                Err(StoreError::IdempotencyConflict.into())
            };
        }
        if self.core.queue_count.load(Ordering::Acquire) >= self.core.queue_capacity {
            if let Some(record) = self
                .core
                .store
                .get_by_idempotency_key(idempotency_key)
                .await
                .map_err(|error| self.handle_store_error(error))?
            {
                return if record.request == request {
                    Ok(record)
                } else {
                    Err(StoreError::IdempotencyConflict.into())
                };
            }
            return Err(TaskServiceError::QueueFull);
        }
        self.reserve_queue_slot()?;
        let resources = request.resources.clone();
        let outcome = self.core.store.accept(TaskId::generate(), request).await;
        drop(reservation);
        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(error) => {
                self.release_queue_slot();
                return Err(self.handle_store_error(error));
            }
        };
        match outcome {
            AcceptOutcome::Accepted(record) => {
                self.core.queue.lock().push(QueuedTask {
                    id: record.id,
                    resources,
                    retry_not_before_ms: None,
                    bypasses: 0,
                });
                self.core.changed.notify_one();
                publish_record(&self.core, &record.summary());
                self.core.wait_registry.notify(record.id);
                Ok(record)
            }
            AcceptOutcome::Existing(record) => {
                self.release_queue_slot();
                Ok(record)
            }
        }
    }

    /// Registers a local closure and retains it through acceptance and queue
    /// publication.
    ///
    /// # Type Parameters
    ///
    /// * `F` - One-shot closure type.
    /// * `R` - Process-local success value.
    /// * `E` - Process-local application error.
    ///
    /// # Parameters
    ///
    /// * `task` - Closure to run for the accepted local task.
    /// * `reservation` - In-flight admission reservation.
    ///
    /// # Returns
    ///
    /// A handle for the closure's typed result and persisted final state.
    ///
    /// # Errors
    ///
    /// Returns an error if local execution is unsupported or acceptance fails.
    async fn submit_local_admitted<F, R, E>(
        &self,
        task: F,
        reservation: AdmissionReservation,
    ) -> Result<LocalTaskHandle<R, E>, TaskServiceError>
    where
        F: FnOnce(TaskContext) -> LocalTaskOutcome<R, E> + Send + 'static,
        R: Send + 'static,
        E: std::fmt::Display + Send + 'static,
    {
        if let Some(error) = self.last_scheduler_error() {
            return Err(TaskServiceError::SchedulerUnavailable(error));
        }
        if self.core.store.capabilities().restart_recovery {
            return Err(TaskServiceError::UnsupportedCapability);
        }
        if let Some(error) = self.last_store_error() {
            return Err(TaskServiceError::StoreUnavailable(error));
        }
        let _permit = self.core.admission.enter()?;
        let id = TaskId::generate();
        let descriptor = TaskHandlerDescriptor {
            task_type: format!("local:{id}"),
            version: "1".into(),
        };
        let (typed_sender, typed_receiver) = oneshot::channel();
        let (final_sender, final_receiver) = oneshot::channel();
        let handler: Arc<dyn TaskHandler> = Arc::new(LocalTaskHandler::new(
            descriptor.clone(),
            adapt_local_outcome(task, typed_sender),
        ));
        let request = TaskRequest {
            task_type: descriptor.task_type,
            handler_version: descriptor.version,
            payload: Vec::new(),
            resources: ResourceRequest {
                cpu_slots: 1,
                ..Default::default()
            },
            correlation_key: None,
            idempotency_key: None,
            metadata: Default::default(),
        };
        let capacity = self.core.engine.capacity().capacity;
        validate_request(&request, &capacity)?;
        self.reserve_queue_slot()?;
        {
            let fault = self.core.store_fault.lock();
            if let Some(error) = fault.as_ref() {
                self.release_queue_slot();
                return Err(TaskServiceError::StoreUnavailable(error.clone()));
            }
            self.core.local_finalizations.lock().insert(id, final_sender);
        }
        let resources = request.resources.clone();
        let outcome = self.core.store.accept(id, request).await;
        drop(reservation);
        match outcome {
            Ok(AcceptOutcome::Accepted(record)) => {
                let finalizations = self.core.local_finalizations.lock();
                if !finalizations.contains_key(&id) {
                    self.release_queue_slot();
                    return Ok(LocalTaskHandle::new(record.id, typed_receiver, final_receiver));
                }
                self.core.local_handlers.lock().insert(id, handler);
                self.core.queue.lock().push(QueuedTask {
                    id,
                    resources,
                    retry_not_before_ms: None,
                    bypasses: 0,
                });
                drop(finalizations);
                self.core.changed.notify_one();
                publish_record(&self.core, &record.summary());
                self.core.wait_registry.notify(record.id);
                Ok(LocalTaskHandle::new(record.id, typed_receiver, final_receiver))
            }
            Ok(AcceptOutcome::Existing(record)) => {
                self.core.local_finalizations.lock().remove(&id);
                self.release_queue_slot();
                Err(TaskServiceError::InvalidRequest(format!(
                    "local task id {} was already accepted",
                    record.id
                )))
            }
            Err(error) => {
                self.core.local_finalizations.lock().remove(&id);
                self.release_queue_slot();
                Err(self.handle_store_error(error))
            }
        }
    }

    /// Converts a store error and latches operational failures while preserving
    /// ordinary conflicts and not-found results for the calling operation.
    ///
    /// # Parameters
    ///
    /// * `error` - Store error returned by a service operation.
    ///
    /// # Returns
    ///
    /// The corresponding public service error.
    fn handle_store_error(&self, error: StoreError) -> TaskServiceError {
        if matches!(error, StoreError::Failure(_)) {
            record_store_fault(&self.core, error.to_string());
        }
        error.into()
    }

    /// Attempts to reserve bounded admission capacity before detaching a
    /// worker.
    ///
    /// # Parameters
    ///
    /// * `payload_bytes` - Request bytes retained by the worker.
    ///
    /// # Returns
    ///
    /// A reservation released when admission completes.
    ///
    /// # Errors
    ///
    /// Returns the configured payload or submission limit error.
    fn reserve_admission(&self, payload_bytes: usize) -> Result<AdmissionReservation, TaskServiceError> {
        self.core
            .admission_budget
            .try_reserve(payload_bytes)
            .map_err(|error| match error {
                AdmissionBudgetError::PayloadBytesExceeded { requested, available } => {
                    TaskServiceError::PayloadBudgetExceeded {
                        requested_bytes: requested,
                        available_bytes: available,
                    }
                }
                AdmissionBudgetError::SubmissionLimitExceeded { limit } => {
                    TaskServiceError::SubmissionLimitExceeded { limit }
                }
            })
    }

    /// Atomically reserves one waiting-queue position when capacity remains.
    ///
    /// # Returns
    ///
    /// Success after reserving one queue position.
    ///
    /// # Errors
    ///
    /// Returns `QueueFull` when no queue capacity remains.
    fn reserve_queue_slot(&self) -> Result<(), TaskServiceError> {
        self.core
            .queue_count
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                (count < self.core.queue_capacity).then_some(count + 1)
            })
            .map(|_| ())
            .map_err(|_| TaskServiceError::QueueFull)
    }

    /// Releases one previously reserved waiting-queue position.
    fn release_queue_slot(&self) {
        let _ = self
            .core
            .queue_count
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                Some(count.saturating_sub(1))
            });
    }

    /// Requeues a blocked task while holding a permit through persistence and
    /// queue publication.
    ///
    /// # Parameters
    ///
    /// * `id` - Stable identity of the blocked task.
    ///
    /// # Returns
    ///
    /// The committed queued summary.
    ///
    /// # Errors
    ///
    /// Returns a service error when the task is unavailable, exhausted, the
    /// queue is full, shutdown has started, or persistence fails.
    async fn retry_blocked_admitted(&self, id: TaskId) -> Result<TaskSummary, TaskServiceError> {
        if let Some(error) = self.last_scheduler_error() {
            return Err(TaskServiceError::SchedulerUnavailable(error));
        }
        if let Some(error) = self.last_store_error() {
            return Err(TaskServiceError::StoreUnavailable(error));
        }
        let _permit = self.core.admission.enter()?;
        let record = self
            .core
            .store
            .get_summary(id)
            .await
            .map_err(|error| self.handle_store_error(error))?
            .ok_or(StoreError::NotFound)?;
        if !matches!(record.state, TaskState::Blocked { .. }) {
            return Err(TaskServiceError::Blocked);
        }
        if record.attempt >= self.core.max_attempts {
            return Err(TaskServiceError::AttemptsExhausted {
                attempts: record.attempt,
                limit: self.core.max_attempts,
            });
        }
        self.reserve_queue_slot()?;
        let updated = match transition(&self.core, &record, TaskState::Queued, None, Vec::new(), false).await {
            Ok(updated) => updated,
            Err(error) => {
                self.core.queue_count.fetch_sub(1, Ordering::AcqRel);
                return Err(self.handle_store_error(error));
            }
        };
        self.core.queue.lock().push(QueuedTask {
            id,
            resources: updated.request.resources.clone(),
            retry_not_before_ms: None,
            bypasses: 0,
        });
        self.core.changed.notify_one();
        Ok(updated)
    }
}

/// Closes admission once and starts the shared asynchronous drain coordinator.
///
/// # Parameters
///
/// * `core` - Shared service state to close and drain.
fn begin_shutdown_core(core: Arc<ServiceCore>) {
    if core.admission.close() {
        core.changed.notify_waiters();
        let runtime_handle = core.runtime_handle.clone();
        runtime_handle.spawn(async move {
            let primary = coordinate_shutdown(Arc::clone(&core)).await;
            let notification = close_notification_publisher(&core).await;
            let result = combine_shutdown_results(primary, notification);
            core.admission.finish_close(result);
            core.changed.notify_waiters();
        });
    }
}

/// Drains accepted work and releases store ownership after admission becomes
/// idle.
///
/// # Parameters
///
/// * `core` - Service state whose accepted work must settle.
///
/// # Returns
///
/// Success after workers stop and ownership is released.
///
/// # Errors
///
/// Returns a latched service or store cleanup error.
async fn coordinate_shutdown(core: Arc<ServiceCore>) -> Result<(), TaskServiceError> {
    core.admission.wait_idle().await;
    let initial_store_fault = { core.store_fault.lock().clone() };
    if let Some(error) = initial_store_fault {
        return finish_failed_shutdown(&core, TaskServiceError::StoreUnavailable(error)).await;
    }
    let scheduler_fault = { core.scheduler_fault.lock().clone() };
    if let Some(error) = scheduler_fault {
        wait_scheduler_finished(&core).await;
        wait_for_attempts(&core).await;
        if let Some(epoch) = core.owner
            && let Err(release_error) = core.store.release_owner(epoch).await
        {
            return Err(TaskServiceError::SchedulerUnavailable(format!(
                "{error}; owner release failed: {release_error}"
            )));
        }
        return Err(TaskServiceError::SchedulerUnavailable(error));
    }
    let transition_guard = loop {
        let notified = core.changed.notified();
        pin!(notified);
        notified.as_mut().enable();
        let store_fault = { core.store_fault.lock().clone() };
        if let Some(error) = store_fault {
            return finish_failed_shutdown(&core, TaskServiceError::StoreUnavailable(error)).await;
        }
        let scheduler_fault = { core.scheduler_fault.lock().clone() };
        if let Some(error) = scheduler_fault {
            wait_scheduler_finished(&core).await;
            wait_for_attempts(&core).await;
            if let Some(epoch) = core.owner
                && let Err(store_error) = core.store.release_owner(epoch).await
            {
                record_store_fault(&core, store_error.to_string());
                return Err(TaskServiceError::Store(store_error));
            }
            return Err(TaskServiceError::SchedulerUnavailable(error));
        }
        let stats_result = task_stats(&core).await.inspect_err(|error| {
            if let TaskServiceError::Store(store_error) = error
                && matches!(store_error, StoreError::Failure(_))
            {
                record_store_fault(&core, store_error.to_string());
            }
        });
        let stats = match stats_result {
            Ok(stats) => stats,
            Err(_error) if core.store_fault.lock().is_some() => continue,
            Err(error) => return Err(error),
        };
        if stats.queued == 0 && stats.running == 0 {
            wait_scheduler_finished(&core).await;
            wait_for_attempts(&core).await;
            let transition_guard = core.transition_event_lock.write().await;
            let settled_result = task_stats(&core).await.inspect_err(|error| {
                if let TaskServiceError::Store(store_error) = error
                    && matches!(store_error, StoreError::Failure(_))
                {
                    record_store_fault(&core, store_error.to_string());
                }
            });
            let settled_stats = match settled_result {
                Ok(stats) => stats,
                Err(_error) if core.store_fault.lock().is_some() => {
                    drop(transition_guard);
                    continue;
                }
                Err(error) => return Err(error),
            };
            let scheduler_fault = { core.scheduler_fault.lock().clone() };
            if let Some(error) = scheduler_fault {
                drop(transition_guard);
                wait_scheduler_finished(&core).await;
                wait_for_attempts(&core).await;
                if let Some(epoch) = core.owner
                    && let Err(store_error) = core.store.release_owner(epoch).await
                {
                    record_store_fault(&core, store_error.to_string());
                    return Err(TaskServiceError::Store(store_error));
                }
                return Err(TaskServiceError::SchedulerUnavailable(error));
            }
            if settled_stats.queued == 0 && settled_stats.running == 0 {
                break transition_guard;
            }
        }
        notified.await;
    };
    if let Some(epoch) = core.owner
        && let Err(error) = core.store.release_owner(epoch).await
    {
        record_store_fault(&core, error.to_string());
        wait_scheduler_finished(&core).await;
        wait_for_attempts(&core).await;
        return Err(TaskServiceError::StoreUnavailable(error.to_string()));
    }
    drop(transition_guard);
    Ok(())
}

/// Waits until every activated attempt has completed service finalization.
///
/// # Parameters
///
/// * `core` - Service state containing the active attempt counter.
async fn wait_for_attempts(core: &ServiceCore) {
    loop {
        let notified = core.attempts_changed.notified();
        pin!(notified);
        notified.as_mut().enable();
        if core.attempts_in_flight.load(Ordering::Acquire) == 0 {
            return;
        }
        notified.await;
    }
}

/// Waits for the scheduler worker to publish that it has exited.
///
/// # Parameters
///
/// * `core` - Service state containing the scheduler completion signal.
async fn wait_scheduler_finished(core: &ServiceCore) {
    loop {
        let notified = core.scheduler_finished_notify.notified();
        pin!(notified);
        notified.as_mut().enable();
        if core.scheduler_finished.load(Ordering::Acquire) {
            return;
        }
        notified.await;
    }
}

/// Finishes shutdown after a service fault and attempts owner cleanup.
///
/// # Parameters
///
/// * `core` - Service state whose workers must stop.
/// * `primary` - Fault that caused shutdown.
///
/// # Returns
///
/// The primary failure after all workers stop.
///
/// # Errors
///
/// Returns the primary service failure, enriched when owner release also fails.
async fn finish_failed_shutdown(core: &Arc<ServiceCore>, primary: TaskServiceError) -> Result<(), TaskServiceError> {
    wait_scheduler_finished(core).await;
    wait_for_attempts(core).await;
    if let Some(epoch) = core.owner
        && let Err(release_error) = core.store.release_owner(epoch).await
    {
        let diagnostic = core.store_fault.lock().clone().unwrap_or_else(|| primary.to_string());
        record_store_fault(core, format!("{diagnostic}; owner release failed: {release_error}"));
        return Err(TaskServiceError::StoreUnavailable(format!(
            "{diagnostic}; owner release failed: {release_error}"
        )));
    }
    Err(primary)
}

impl TaskExecutionService {
    /// Starts the background scheduler and wraps its shared service state.
    ///
    /// # Parameters
    ///
    /// * `core` - Fully assembled component and service state.
    ///
    /// # Returns
    ///
    /// A public service handle with its scheduler worker started.
    pub(crate) fn start(core: ServiceCore) -> Self {
        let core = Arc::new(core);
        let service = Self {
            _lease: Arc::new(ServiceHandleLease {
                core: Arc::downgrade(&core),
            }),
            core,
        };
        let weak = Arc::downgrade(&service.core);
        let supervisor_weak = weak.clone();
        service.core.runtime_handle.spawn(async move {
            let result = std::panic::AssertUnwindSafe(scheduler_loop(weak)).catch_unwind().await;
            if let Some(core) = supervisor_weak.upgrade() {
                if let Err(payload) = result {
                    record_scheduler_fault(&core, panic_message(payload));
                }
                core.scheduler_finished.store(true, Ordering::Release);
                core.scheduler_finished_notify.notify_waiters();
            }
        });
        service
    }
}

/// Restores unprocessed tasks when a scheduler round exits early.
struct QueueWindowGuard {
    /// Shared queue to which unfinished tasks are restored.
    core: Arc<ServiceCore>,
    /// Scheduler candidates not yet started or otherwise consumed.
    tasks: Option<Vec<QueuedTask>>,
}

impl QueueWindowGuard {
    /// Owns one bounded scheduler window until it is restored.
    ///
    /// # Parameters
    ///
    /// * `core` - Service state owning the shared queue.
    /// * `tasks` - Candidate window removed from that queue.
    ///
    /// # Returns
    ///
    /// A guard that restores the window unless explicitly consumed.
    fn new(core: Arc<ServiceCore>, tasks: Vec<QueuedTask>) -> Self {
        Self {
            core,
            tasks: Some(tasks),
        }
    }

    /// Borrows the current window for policy ordering and execution.
    ///
    /// # Returns
    ///
    /// Mutable access to candidates held by this guard.
    fn tasks_mut(&mut self) -> &mut Vec<QueuedTask> {
        self.tasks.as_mut().expect("scheduler window is active")
    }

    /// Returns all unprocessed work to the front of the shared queue.
    fn restore(&mut self) {
        if let Some(tasks) = self.tasks.take() {
            self.core.queue.lock().restore_front(tasks);
        }
    }
}

impl Drop for QueueWindowGuard {
    /// Restores the window if scheduler control exits early.
    fn drop(&mut self) {
        self.restore();
    }
}

/// Selects queued work, reserves resources, and starts eligible task attempts.
///
/// # Parameters
///
/// * `core_ref` - Weak shared state upgraded for each scheduling pass.
async fn scheduler_loop(core_ref: std::sync::Weak<ServiceCore>) {
    loop {
        let Some(core) = core_ref.upgrade() else {
            return;
        };
        if core.store_fault.lock().is_some() {
            return;
        }
        let now = now_ms();
        let window_tasks = core.queue.lock().take_window(core.scan_budget, now);
        let mut window = QueueWindowGuard::new(Arc::clone(&core), window_tasks);
        let queue = window.tasks_mut();
        if queue.is_empty() {
            if core.admission.is_closing() && core.admission.is_idle() && core.queue.lock().is_empty() {
                match task_stats(&core).await {
                    Ok(stats) if stats.queued == 0 && stats.running == 0 => return,
                    Err(error) => {
                        if let TaskServiceError::Store(store_error) = error {
                            record_store_fault(&core, store_error.to_string());
                        }
                        return;
                    }
                    _ => {}
                }
            }
            let notified = core.changed.notified();
            pin!(notified);
            notified.as_mut().enable();
            let deadline = core.queue.lock().next_deadline();
            let wait = deadline.map(|value| std::time::Duration::from_millis(value.saturating_sub(now_ms())));
            if let Some(wait) = wait {
                let _ = select(Box::pin(notified), Box::pin(time::sleep(wait))).await;
            } else {
                notified.await;
            }
            continue;
        }
        let order = core.policy.order(
            &QueueSnapshot {
                tasks: queue.to_vec(),
                scan_budget: core.scan_budget,
            },
            &core.engine.capacity(),
        );
        let original_positions = queue
            .iter()
            .enumerate()
            .map(|(position, task)| (task.id, position))
            .collect::<HashMap<_, _>>();
        let mut activated_positions = Vec::new();
        let mut started = false;
        for id in order {
            if core.store_fault.lock().is_some() {
                return;
            }
            let Some(index) = queue.iter().position(|task| task.id == id) else {
                continue;
            };
            let mut task = queue.remove(index);
            let record = match core.store.get_summary(id).await {
                Ok(Some(record)) => record,
                Ok(None) => {
                    release_core_queue_slot(&core);
                    core.local_handlers.lock().remove(&id);
                    continue;
                }
                Err(error) => {
                    queue.push(task);
                    pause_on_store_fault(&core, error);
                    return;
                }
            };
            if core.store_fault.lock().is_some() {
                queue.push(task);
                return;
            }
            if matches!(record.state, TaskState::Queued)
                && record.retry_not_before_ms.is_some_and(|deadline| deadline > now_ms())
            {
                let deadline = record.retry_not_before_ms.expect("deadline checked above");
                task.retry_not_before_ms = Some(deadline);
                queue.push(task);
                continue;
            }
            if !matches!(record.state, TaskState::Queued) {
                release_core_queue_slot(&core);
                core.local_handlers.lock().remove(&id);
                if record.state.is_terminal() || matches!(record.state, TaskState::Blocked { .. }) {
                    finalize_local(&core, id, Ok(record.state));
                }
                continue;
            }
            let handler = core.local_handlers.lock().get(&id).cloned().or_else(|| {
                core.handlers
                    .resolve(&record.request.task_type, &record.request.handler_version)
            });
            let Some(handler) = handler else {
                match mark_blocked(
                    &core,
                    &record,
                    format!(
                        "missing handler {}@{}",
                        record.request.task_type, record.request.handler_version
                    ),
                )
                .await
                {
                    Ok(()) | Err(StoreError::NotFound) => release_core_queue_slot(&core),
                    Err(StoreError::Conflict) => queue.push(task),
                    Err(error) => {
                        queue.push(task);
                        pause_on_store_fault(&core, error);
                        return;
                    }
                }
                continue;
            };
            let Ok(running_permit) = core.running_slots.clone().try_acquire_owned() else {
                queue.push(task);
                break;
            };
            let prepared = match core.engine.prepare(id, record.request.resources.clone()).await {
                Ok(value) => value,
                Err(EngineError::TemporarilyUnavailable) => {
                    queue.push(task);
                    continue;
                }
                Err(EngineError::Unsatisfiable) => {
                    match mark_blocked(&core, &record, "resource request is unsatisfiable".into()).await {
                        Ok(()) | Err(StoreError::NotFound) => release_core_queue_slot(&core),
                        Err(StoreError::Conflict) => queue.push(task),
                        Err(error) => {
                            queue.push(task);
                            pause_on_store_fault(&core, error);
                            return;
                        }
                    }
                    continue;
                }
                Err(EngineError::Closed) => {
                    queue.push(task);
                    record_scheduler_fault(&core, "execution engine closed during prepare".into());
                    return;
                }
            };
            if core.store_fault.lock().is_some() {
                queue.push(task);
                return;
            }
            let current = match core.store.get(id).await {
                Ok(Some(current))
                    if current.state_version == record.state_version
                        && current.attempt == record.attempt
                        && matches!(current.state, TaskState::Queued) =>
                {
                    current
                }
                Ok(Some(_)) | Ok(None) => {
                    release_core_queue_slot(&core);
                    continue;
                }
                Err(error) => {
                    pause_on_store_fault(&core, error);
                    return;
                }
            };
            let assigned = prepared.assigned_resources().to_vec();
            let running = match transition(&core, &record, TaskState::Running, None, assigned.clone(), false).await {
                Ok(value) => value,
                Err(StoreError::Conflict | StoreError::NotFound) => {
                    release_core_queue_slot(&core);
                    continue;
                }
                Err(error) => {
                    queue.push(task);
                    pause_on_store_fault(&core, error);
                    return;
                }
            };
            let mut running_record = current.clone();
            running_record.state = running.state.clone();
            running_record.state_version = running.state_version;
            running_record.attempt = running.attempt;
            running_record.started_at_ms = running.started_at_ms;
            running_record.assigned_resources = running.assigned_resources.clone();
            release_core_queue_slot(&core);
            if core.store_fault.lock().is_some() {
                return;
            }
            match core.store.get_summary(id).await {
                Ok(Some(latest))
                    if latest.state_version == running.state_version
                        && latest.attempt == running.attempt
                        && matches!(latest.state, TaskState::Running)
                        && !latest.cancel_requested => {}
                Ok(Some(latest)) if latest.attempt == running.attempt && matches!(latest.state, TaskState::Running) => {
                    match transition(&core, &latest, TaskState::Cancelled, None, Vec::new(), false).await {
                        Ok(cancelled) => finalize_local(&core, id, Ok(cancelled.state)),
                        Err(error) => pause_on_store_fault(&core, error),
                    }
                    continue;
                }
                Ok(Some(_)) | Ok(None) => continue,
                Err(error) => {
                    pause_on_store_fault(&core, error);
                    return;
                }
            }
            core.local_handlers.lock().remove(&id);
            let cancelled = Arc::new(AtomicBool::new(false));
            let context = TaskContext::new(id, running.attempt, assigned, cancelled);
            match core
                .engine
                .activate(prepared, handler, current.request.payload.clone(), context)
                .await
            {
                Ok(handle) => {
                    let cancellation_signal = Arc::clone(&handle.cancelled);
                    core.cancellations.lock().insert(
                        id,
                        RunningCancellation {
                            attempt: running.attempt,
                            signal: Arc::clone(&cancellation_signal),
                        },
                    );
                    core.attempts_in_flight.fetch_add(1, Ordering::AcqRel);
                    let weak = Arc::downgrade(&core);
                    core.runtime_handle
                        .spawn(finish_attempt(weak, running_record, handle.receiver, running_permit));
                    activated_positions.push(original_positions[&id]);
                    started = true;
                    match core.store.get_summary(id).await {
                        Ok(Some(record))
                            if record.attempt == running.attempt
                                && matches!(record.state, TaskState::Running)
                                && record.cancel_requested =>
                        {
                            cancellation_signal.store(true, Ordering::Release);
                        }
                        Ok(Some(_)) | Ok(None) => {}
                        Err(error) => {
                            core.cancellations.lock().remove(&id);
                            pause_on_store_fault(&core, error);
                            return;
                        }
                    }
                }
                Err(error) => {
                    let reason = format!("engine activation failed: {error}");
                    let mut latest = running_record.summary();
                    loop {
                        match mark_blocked(&core, &latest, reason.clone()).await {
                            Ok(()) => break,
                            Err(StoreError::Conflict) => match core.store.get_summary(id).await {
                                Ok(Some(record))
                                    if record.attempt == latest.attempt
                                        && matches!(record.state, TaskState::Running) =>
                                {
                                    latest = record;
                                }
                                Ok(_) => break,
                                Err(error) => {
                                    pause_on_store_fault(&core, error);
                                    return;
                                }
                            },
                            Err(error) => {
                                pause_on_store_fault(&core, error);
                                return;
                            }
                        }
                    }
                }
            }
        }
        for item in queue.iter_mut() {
            let Some(position) = original_positions.get(&item.id) else {
                continue;
            };
            if activated_positions
                .iter()
                .any(|started_position| started_position > position)
            {
                item.bypasses = item.bypasses.saturating_add(1);
            }
        }
        {
            window.restore();
        }
        if !started {
            let wait = core
                .queue
                .lock()
                .next_deadline()
                .map(|deadline| std::time::Duration::from_millis(deadline.saturating_sub(now_ms())));
            let notified = core.changed.notified();
            pin!(notified);
            notified.as_mut().enable();
            let sleep_for = wait.unwrap_or(std::time::Duration::from_millis(40));
            let notified = Box::pin(notified);
            let timer = Box::pin(time::sleep(sleep_for));
            let _ = select(notified, timer).await;
        } else {
            core.changed.notify_waiters();
        }
    }
}

/// Persists an execution result, retry decision, and local-handle completion.
///
/// # Parameters
///
/// * `core_ref` - Weak service reference held through attempt completion.
/// * `running` - Record snapshot committed before handler activation.
/// * `receiver` - Completion result channel returned by the engine.
/// * `_running_permit` - Slot retained until finalization exits.
async fn finish_attempt(
    core_ref: std::sync::Weak<ServiceCore>,
    running: TaskRecord,
    receiver: sync::oneshot::Receiver<ExecutionOutcome>,
    _running_permit: sync::OwnedSemaphorePermit,
) {
    let _attempt_guard = AttemptInFlightGuard {
        core_ref: core_ref.clone(),
    };
    let outcome = receiver
        .await
        .unwrap_or_else(|_| ExecutionOutcome::WorkerStopped("execution worker stopped".into()));
    let Some(core) = core_ref.upgrade() else {
        return;
    };
    {
        let mut cancellations = core.cancellations.lock();
        if cancellations
            .get(&running.id)
            .is_some_and(|current| current.attempt == running.attempt)
        {
            cancellations.remove(&running.id);
        }
    }
    let state = match &outcome {
        ExecutionOutcome::Panicked(message) => TaskState::Panicked {
            message: truncate_utf8(message, MAX_TASK_DIAGNOSTIC_MESSAGE_BYTES),
        },
        ExecutionOutcome::WorkerStopped(_) if running.attempt < core.max_attempts => TaskState::Queued,
        ExecutionOutcome::WorkerStopped(_) => TaskState::Blocked {
            reason: "execution worker stopped after retry limit".into(),
        },
        ExecutionOutcome::Returned(Ok(TaskRunOutcome::Succeeded(_))) => TaskState::Succeeded,
        ExecutionOutcome::Returned(Ok(TaskRunOutcome::Cancelled)) => TaskState::Cancelled,
        ExecutionOutcome::Returned(Err(error)) if error.retryable && running.attempt < core.max_attempts => {
            TaskState::Queued
        }
        ExecutionOutcome::Returned(Err(error)) if error.retryable => TaskState::Blocked {
            reason: truncate_utf8(
                &format!("retry limit reached: {}", error.message),
                MAX_TASK_DIAGNOSTIC_MESSAGE_BYTES,
            ),
        },
        ExecutionOutcome::Returned(Err(error)) => TaskState::Failed {
            category: truncate_utf8(&error.category, MAX_TASK_DIAGNOSTIC_CATEGORY_BYTES),
            message: truncate_utf8(&error.message, MAX_TASK_DIAGNOSTIC_MESSAGE_BYTES),
        },
    };
    let output = match outcome {
        ExecutionOutcome::Returned(Ok(TaskRunOutcome::Succeeded(output))) => Some(output),
        _ => None,
    };
    let mut final_state = if output
        .as_ref()
        .is_some_and(|value| value.summary.len() > MAX_TASK_OUTPUT_SUMMARY_BYTES)
    {
        TaskState::Failed {
            category: "output_too_large".into(),
            message: "task output summary exceeded the 65536-byte limit".into(),
        }
    } else {
        state
    };
    let output = output.filter(|value| value.summary.len() <= MAX_TASK_OUTPUT_SUMMARY_BYTES);
    let mut retry_deadline = if matches!(final_state, TaskState::Queued) {
        Some(retry_deadline_ms(now_ms(), core.retry_policy, running.attempt))
    } else {
        None
    };
    let mut retry_slot_reserved = false;
    if matches!(final_state, TaskState::Queued) {
        retry_slot_reserved = try_reserve_core_queue_slot(&core);
        if !retry_slot_reserved {
            retry_deadline = None;
            final_state = TaskState::Blocked {
                reason: "retry queue is full; call retry_blocked when capacity is available".into(),
            };
        }
    }
    loop {
        let latest = match core.store.get_summary(running.id).await {
            Ok(Some(record)) if matches!(record.state, TaskState::Running) && record.attempt == running.attempt => {
                record
            }
            Ok(None) => break,
            Ok(_) => break,
            Err(error) => {
                pause_on_store_fault(&core, error);
                break;
            }
        };
        match transition_with_deadline(
            &core,
            &latest,
            final_state.clone(),
            retry_deadline,
            output.clone(),
            latest.assigned_resources.clone(),
            latest.cancel_requested,
        )
        .await
        {
            Ok(updated) => {
                if matches!(final_state, TaskState::Queued) {
                    core.queue.lock().push(QueuedTask {
                        id: updated.id,
                        resources: updated.request.resources.clone(),
                        retry_not_before_ms: updated.retry_not_before_ms,
                        bypasses: 0,
                    });
                }
                core.changed.notify_waiters();
                if !matches!(updated.state, TaskState::Queued | TaskState::Running) {
                    finalize_local(&core, updated.id, Ok(updated.state));
                }
                return;
            }
            Err(StoreError::Conflict) => continue,
            Err(error) => {
                pause_on_store_fault(&core, error);
                break;
            }
        }
    }
    if retry_slot_reserved {
        release_core_queue_slot(&core);
    }
}

/// Decrements the tracked execution-attempt count when finalization exits.
struct AttemptInFlightGuard {
    /// Service whose active-attempt count this guard owns.
    core_ref: std::sync::Weak<ServiceCore>,
}

impl Drop for AttemptInFlightGuard {
    /// Wakes scheduler-failure shutdown when the last attempt finalizes.
    fn drop(&mut self) {
        if let Some(core) = self.core_ref.upgrade() {
            core.attempts_in_flight.fetch_sub(1, Ordering::AcqRel);
            core.attempts_changed.notify_waiters();
        }
    }
}

/// Persists a blocked state and completes any local handle waiting on it.
///
/// # Parameters
///
/// * `core` - Service state containing storage and local result channels.
/// * `record` - Snapshot whose version and attempt guard the transition.
/// * `reason` - Operator-readable explanation for the blocked state.
///
/// # Returns
///
/// Success after the blocked transition is persisted.
///
/// # Errors
///
/// Returns the store error if the transition cannot be committed.
async fn mark_blocked(core: &ServiceCore, record: &TaskSummary, reason: String) -> Result<(), StoreError> {
    let updated = transition(
        core,
        record,
        TaskState::Blocked {
            reason: truncate_utf8(&reason, MAX_TASK_DIAGNOSTIC_MESSAGE_BYTES),
        },
        None,
        Vec::new(),
        record.cancel_requested,
    )
    .await?;
    core.changed.notify_waiters();
    finalize_local(core, updated.id, Ok(updated.state));
    Ok(())
}

/// Sends final state or infrastructure failure to a process-local task handle.
///
/// # Parameters
///
/// * `core` - Service state owning local finalization senders.
/// * `id` - Task whose handle should receive the result.
/// * `result` - Final lifecycle state or infrastructure failure.
fn finalize_local(core: &ServiceCore, id: TaskId, result: Result<TaskState, LocalTaskResultError>) {
    let sender = core.local_finalizations.lock().remove(&id);
    if let Some(sender) = sender {
        let _ = sender.send(result);
    }
}

/// Latches a worker-side store error and suspends service admission.
///
/// # Parameters
///
/// * `core` - Service state to suspend.
/// * `error` - Store failure observed by a worker.
fn pause_on_store_fault(core: &Arc<ServiceCore>, error: StoreError) {
    record_store_fault(core, error.to_string());
}

/// Records the first storage failure and closes admission for all waiters.
///
/// # Parameters
///
/// * `core` - Service state to suspend.
/// * `diagnostic` - Error message retained for later callers.
fn record_store_fault(core: &Arc<ServiceCore>, diagnostic: String) {
    let (diagnostic, finalizations) = {
        let mut fault = core.store_fault.lock();
        let diagnostic = fault.get_or_insert(diagnostic).clone();
        let finalizations = core
            .local_finalizations
            .lock()
            .drain()
            .map(|(_, sender)| sender)
            .collect::<Vec<_>>();
        (diagnostic, finalizations)
    };
    core.local_handlers.lock().clear();
    for sender in finalizations {
        let _ = sender.send(Err(LocalTaskResultError::StoreUnavailable(diagnostic.clone())));
    }
    begin_shutdown_core(Arc::clone(core));
    core.changed.notify_waiters();
    core.wait_registry.notify_all();
}

/// Records the first scheduler panic and starts the shared shutdown
/// coordinator.
///
/// # Parameters
///
/// * `core` - Service state whose scheduler failure is retained.
/// * `diagnostic` - Panic or scheduler failure message.
fn record_scheduler_fault(core: &Arc<ServiceCore>, diagnostic: String) {
    let (diagnostic, finalizations) = {
        let mut fault = core.scheduler_fault.lock();
        let diagnostic = fault.get_or_insert(diagnostic).clone();
        let finalizations = core
            .local_finalizations
            .lock()
            .drain()
            .map(|(_, sender)| sender)
            .collect::<Vec<_>>();
        (diagnostic, finalizations)
    };
    core.local_handlers.lock().clear();
    for sender in finalizations {
        let _ = sender.send(Err(LocalTaskResultError::Infrastructure(diagnostic.clone())));
    }
    core.changed.notify_waiters();
    core.wait_registry.notify_all();
    begin_shutdown_core(Arc::clone(core));
}

/// Extracts a useful diagnostic from a caught scheduler panic payload.
///
/// # Parameters
///
/// * `payload` - Panic payload returned by `catch_unwind`.
///
/// # Returns
///
/// A string message or a stable fallback for non-string payloads.
fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else if let Some(message) = payload.downcast_ref::<&'static str>() {
        (*message).to_owned()
    } else {
        "scheduler panicked with a non-string payload".into()
    }
}

/// Reads store state counts and the current execution resource snapshot.
///
/// # Parameters
///
/// * `core` - Service state whose store and engine are observed.
///
/// # Returns
///
/// State counts and current resource reservations.
///
/// # Errors
///
/// Returns a service error if the store count query fails.
async fn task_stats(core: &ServiceCore) -> Result<TaskStats, TaskServiceError> {
    let TaskStateCounts {
        queued,
        running,
        blocked,
        terminal,
    } = core.store.count_states().await.map_err(TaskServiceError::Store)?;
    Ok(TaskStats {
        queued,
        running,
        blocked,
        terminal,
        resources: core.engine.capacity(),
    })
}

/// Stops and drains the service-owned notification worker before shutdown
/// publishes its shared result.
///
/// With the `event-bus` feature, this waits on the publisher through
/// `spawn_blocking`, bounded by the configured timeout. A timeout is reported
/// through `NotificationClose`; the worker continues processing accepted
/// notifications. The injected `EventBus` remains application owned.
///
/// # Parameters
///
/// * `core` - Service state containing the optional publisher.
///
/// # Returns
///
/// Success when no publisher exists or it has stopped.
///
/// # Errors
///
/// Returns `NotificationClose` when the worker times out, fails to join, or
/// panics.
async fn close_notification_publisher(core: &Arc<ServiceCore>) -> Result<(), TaskServiceError> {
    #[cfg(feature = "event-bus")]
    if let Some(publisher) = &core.event_bus {
        publisher
            .close(&core.runtime_handle)
            .await
            .map_err(|error| TaskServiceError::NotificationClose(error.to_string()))?;
    }
    #[cfg(not(feature = "event-bus"))]
    let _ = core;
    Ok(())
}

/// Combines service convergence and notification worker shutdown results.
///
/// A notification close failure becomes the close result when task
/// convergence succeeded. If both fail, the service error stays primary and
/// the notification error is appended to its diagnostic.
///
/// # Parameters
///
/// * `primary` - Result of draining service work and releasing ownership.
/// * `notification` - Result of closing the optional notification worker.
///
/// # Returns
///
/// The combined shutdown result, preserving the service error as primary.
///
/// # Errors
///
/// Returns the service failure, the notification close failure, or a combined
/// diagnostic when both operations fail.
fn combine_shutdown_results(
    primary: Result<(), TaskServiceError>,
    notification: Result<(), TaskServiceError>,
) -> Result<(), TaskServiceError> {
    match (primary, notification) {
        (Ok(()), result) => result,
        (Err(primary), Ok(())) => Err(primary),
        (Err(TaskServiceError::StoreUnavailable(store)), Err(TaskServiceError::NotificationClose(close))) => Err(
            TaskServiceError::StoreUnavailable(format!("{store}; notification close failed: {close}")),
        ),
        (Err(TaskServiceError::SchedulerUnavailable(scheduler)), Err(TaskServiceError::NotificationClose(close))) => {
            Err(TaskServiceError::SchedulerUnavailable(format!(
                "{scheduler}; notification close failed: {close}"
            )))
        }
        (Err(primary), Err(close)) => Err(TaskServiceError::StoreUnavailable(format!(
            "{primary}; notification close failed: {close}"
        ))),
    }
}

/// Awaits an admission worker and maps task-join failures into service errors.
///
/// # Type Parameters
///
/// * `T` - Value produced by the admission worker.
///
/// # Parameters
///
/// * `handle` - Detached worker handle retained after caller cancellation.
///
/// # Returns
///
/// The worker's value after it completes.
///
/// # Errors
///
/// Returns the worker's service error or a diagnostic if the task join fails.
async fn await_admission<T>(handle: task::JoinHandle<Result<T, TaskServiceError>>) -> Result<T, TaskServiceError> {
    handle
        .await
        .map_err(|error| TaskServiceError::StoreUnavailable(format!("task admission worker stopped: {error}")))?
}

/// Enqueues a best-effort lifecycle event when event-bus support is enabled.
///
/// # Parameters
///
/// * `core` - Service state containing the optional event publisher.
/// * `record` - Payload-free lifecycle snapshot to publish.
fn publish_record(core: &ServiceCore, record: &TaskSummary) {
    #[cfg(feature = "event-bus")]
    if let Some(bus) = &core.event_bus {
        bus.enqueue(TaskEvent::from(record));
    }
    #[cfg(not(feature = "event-bus"))]
    let _ = (core, record);
}

/// Applies a version-checked store transition and publishes its new revision.
///
/// # Parameters
///
/// * `core` - Service state owning the store and event lock.
/// * `record` - Snapshot whose version and attempt guard the update.
/// * `state` - New lifecycle state.
/// * `output` - Optional bounded task result summary.
/// * `assigned_resources` - Resources assigned to the new state.
/// * `cancel_requested` - Whether cooperative cancellation is pending.
///
/// # Returns
///
/// The committed task summary.
///
/// # Errors
///
/// Returns the store error if the expected revision cannot be committed.
async fn transition(
    core: &ServiceCore,
    record: &TaskSummary,
    state: TaskState,
    output: Option<TaskOutput>,
    assigned_resources: Vec<String>,
    cancel_requested: bool,
) -> Result<TaskSummary, StoreError> {
    transition_with_deadline(core, record, state, None, output, assigned_resources, cancel_requested).await
}

/// Applies a transition with an optional retry deadline and publishes it.
///
/// # Parameters
///
/// * `core` - Service state owning the store and event lock.
/// * `record` - Snapshot whose version and attempt guard the update.
/// * `state` - New lifecycle state.
/// * `retry_not_before_ms` - Optional earliest retry timestamp.
/// * `output` - Optional bounded task result summary.
/// * `assigned_resources` - Resources assigned to the new state.
/// * `cancel_requested` - Whether cooperative cancellation is pending.
///
/// # Returns
///
/// The committed task summary.
///
/// # Errors
///
/// Returns the store error if the expected revision cannot be committed.
async fn transition_with_deadline(
    core: &ServiceCore,
    record: &TaskSummary,
    state: TaskState,
    retry_not_before_ms: Option<u64>,
    output: Option<TaskOutput>,
    assigned_resources: Vec<String>,
    cancel_requested: bool,
) -> Result<TaskSummary, StoreError> {
    let _guard = core.transition_event_lock.read().await;
    let updated = core
        .store
        .transition(TransitionCommand {
            id: record.id,
            expected_version: record.state_version,
            expected_attempt: record.attempt,
            state,
            retry_not_before_ms,
            output,
            assigned_resources,
            cancel_requested,
        })
        .await?;
    publish_record(core, &updated);
    core.wait_registry.notify(updated.id);
    Ok(updated)
}

/// Reads the current Unix epoch time in milliseconds, saturating to `u64`.
///
/// # Returns
///
/// Current epoch milliseconds, or zero if the clock predates the epoch.
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis().min(u64::MAX as u128) as u64)
}

/// Computes the next retry timestamp using a saturating addition.
///
/// # Parameters
///
/// * `now_ms` - Current Unix epoch time in milliseconds.
/// * `policy` - Retry delay policy.
/// * `attempt` - One-based attempt number that just failed.
///
/// # Returns
///
/// The earliest next attempt timestamp, saturated at `u64::MAX`.
fn retry_deadline_ms(now_ms: u64, policy: RetryPolicy, attempt: u32) -> u64 {
    now_ms.saturating_add(policy.delay_for_attempt(attempt).as_millis().min(u64::MAX as u128) as u64)
}

/// Validates request limits and whether configured resources can satisfy it.
///
/// # Parameters
///
/// * `request` - Request metadata and resource demand.
/// * `capacity` - Total resources configured for the service.
///
/// # Returns
///
/// Success when request bounds and resource requirements are valid.
///
/// # Errors
///
/// Returns `InvalidRequest` for malformed fields or `Unsatisfiable` when
/// configured capacity cannot meet the request.
fn validate_request(request: &TaskRequest, capacity: &ResourceCapacity) -> Result<(), TaskServiceError> {
    request
        .validate_limits()
        .map_err(|message| TaskServiceError::InvalidRequest(message.into()))?;
    if request.resources.custom.keys().any(String::is_empty)
        || request.resources.gpu_labels.iter().any(String::is_empty)
    {
        return Err(TaskServiceError::InvalidRequest(
            "resource names and GPU labels must not be empty".into(),
        ));
    }
    let matching_gpus = capacity
        .gpus
        .values()
        .filter(|labels| request.resources.gpu_labels.iter().all(|label| labels.contains(label)))
        .count();
    if request.resources.cpu_slots > capacity.cpu_slots
        || request.resources.gpu_count as usize > matching_gpus
        || request
            .resources
            .custom
            .iter()
            .any(|(key, value)| capacity.custom.get(key).is_none_or(|limit| value > limit))
    {
        return Err(TaskServiceError::Unsatisfiable);
    }
    Ok(())
}

/// Truncates diagnostics at a UTF-8 boundary so persisted values stay valid.
///
/// # Parameters
///
/// * `value` - Diagnostic string to retain.
/// * `max_bytes` - Maximum encoded byte length.
///
/// # Returns
///
/// An owned string no longer than `max_bytes` bytes.
fn truncate_utf8(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_owned();
    }
    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].to_owned()
}

/// Releases one queue slot reserved by a scheduler worker.
///
/// # Parameters
///
/// * `core` - Service state whose occupied queue count is decremented.
fn release_core_queue_slot(core: &ServiceCore) {
    let _ = core
        .queue_count
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
            Some(count.saturating_sub(1))
        });
}

/// Attempts to reserve a queue slot for an automatic retry.
///
/// # Parameters
///
/// * `core` - Service state whose queue limit and count are checked.
///
/// # Returns
///
/// Whether one queue slot was reserved.
fn try_reserve_core_queue_slot(core: &ServiceCore) -> bool {
    core.queue_count
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
            (count < core.queue_capacity).then_some(count + 1)
        })
        .is_ok()
}

/// Returns the process-wide runtime used for service-owned background work.
///
/// # Returns
///
/// The lazily initialized multi-thread Tokio runtime.
///
/// # Panics
///
/// Panics if the process-wide runtime cannot be initialized.
pub(super) fn runtime() -> &'static runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| {
        runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("task service runtime must be created")
    })
}

#[cfg(test)]
mod retry_deadline_tests {
    use std::time::Duration;

    use super::now_ms;
    use super::retry_deadline_ms;
    use crate::service::RetryPolicy;

    #[test]
    fn test_computes_retry_deadlines_with_saturating_milliseconds() {
        let policy = RetryPolicy::new(Duration::from_millis(25), Duration::from_millis(100)).unwrap();
        assert_eq!(retry_deadline_ms(100, policy, 2), 150);
        assert_eq!(retry_deadline_ms(u64::MAX, policy, 1), u64::MAX);
        assert!(now_ms() > 0);
    }
}

#[cfg(test)]
mod shutdown_result_tests {
    use super::TaskServiceError;
    use super::combine_shutdown_results;

    #[test]
    fn test_combines_shutdown_and_notification_results() {
        assert!(combine_shutdown_results(Ok(()), Ok(())).is_ok());
        assert!(matches!(
            combine_shutdown_results(
                Ok(()),
                Err(TaskServiceError::NotificationClose("notification publisher close timed out".into()))
            ),
            Err(TaskServiceError::NotificationClose(message)) if message == "notification publisher close timed out"
        ));
        assert!(matches!(
            combine_shutdown_results(
                Err(TaskServiceError::StoreUnavailable("store failed".into())),
                Ok(())
            ),
            Err(TaskServiceError::StoreUnavailable(message)) if message == "store failed"
        ));
        let combined_store_error = combine_shutdown_results(
            Err(TaskServiceError::StoreUnavailable("store failed".into())),
            Err(TaskServiceError::NotificationClose("close failed".into())),
        )
        .expect_err("combined store and notification failures are retained");
        assert_eq!(
            combined_store_error.to_string(),
            "task execution service is paused after a task store failure: store failed; notification close failed: close failed"
        );
        let combined_other_error = combine_shutdown_results(
            Err(TaskServiceError::QueueFull),
            Err(TaskServiceError::NotificationClose("close failed".into())),
        )
        .expect_err("other primary errors are represented as store unavailable");
        assert_eq!(
            combined_other_error.to_string(),
            "task execution service is paused after a task store failure: task queue is full; notification close failed: task notification publisher failed to close: close failed"
        );
    }
}
