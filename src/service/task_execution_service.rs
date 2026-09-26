// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use parking_lot::Mutex;
use tokio::sync::Notify;
use tokio::sync::oneshot;

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
use super::task_wait_registry::TaskWaitRegistry;
use crate::engine::EngineError;
use crate::engine::ExecutionOutcome;
use crate::engine::TaskExecutionEngine;
use crate::handler::LocalTaskHandler;
use crate::handler::TaskContext;
use crate::handler::TaskHandler;
use crate::handler::TaskHandlerDescriptor;
use crate::handler::TaskHandlerRegistry;
use crate::handler::TaskRunOutcome;
use crate::model::AcceptOutcome;
use crate::model::OwnerEpoch;
use crate::model::ResourceCapacity;
use crate::model::StoreCapabilities;
use crate::model::TaskId;
use crate::model::TaskPage;
use crate::model::TaskQuery;
use crate::model::TaskRecord;
use crate::model::TaskRequest;
use crate::model::TaskState;
use crate::model::TaskStateCounts;
use crate::model::TaskStats;
use crate::model::TransitionCommand;
use crate::model::checked_page_size;
use crate::scheduling::QueueSnapshot;
use crate::scheduling::QueuedTask;
use crate::scheduling::SchedulingPolicy;
use crate::store::StoreError;
use crate::store::TaskStore;

/// Effective store capabilities and local-closure support.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaskServiceCapabilities {
    /// Capabilities declared by the selected store.
    pub store: StoreCapabilities,
    /// Whether local in-process handlers can be submitted.
    pub submit_local: bool,
}

/// Failure reported by a service operation.
#[derive(Debug, thiserror::Error)]
pub enum TaskServiceError {
    /// The selected store failed an operation.
    #[error(transparent)]
    Store(#[from] StoreError),
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
    InvalidRequest(String),
    /// The requested task is blocked pending intervention.
    #[error("task is blocked and requires intervention")]
    Blocked,
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
    StoreUnavailable(String),
    /// The task notification publisher failed while draining during shutdown.
    #[error("task notification publisher failed to close: {0}")]
    NotificationClose(String),
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelOutcome {
    /// The task was cancelled while it was queued.
    CancelledBeforeStart,
    /// Cooperative cancellation was signalled to a running handler.
    CancellationRequested,
    /// The task had already reached a terminal state.
    AlreadyTerminal,
}

pub(crate) struct ServiceCore {
    pub(crate) store: Arc<dyn TaskStore>,
    pub(crate) engine: Arc<dyn TaskExecutionEngine>,
    pub(crate) policy: Arc<dyn SchedulingPolicy>,
    pub(crate) runtime_handle: tokio::runtime::Handle,
    pub(crate) handlers: TaskHandlerRegistry,
    pub(crate) queue_capacity: usize,
    pub(crate) scan_budget: usize,
    pub(crate) max_attempts: u32,
    pub(crate) retry_policy: RetryPolicy,
    pub(crate) running_slots: Arc<tokio::sync::Semaphore>,
    pub(crate) queue: Mutex<SchedulerQueue>,
    pub(crate) queue_count: AtomicUsize,
    pub(crate) local_handlers: Mutex<HashMap<TaskId, Arc<dyn TaskHandler>>>,
    pub(crate) local_finalizations: Mutex<HashMap<TaskId, oneshot::Sender<Result<TaskState, LocalTaskResultError>>>>,
    pub(crate) cancellations: Mutex<HashMap<TaskId, RunningCancellation>>,
    pub(crate) changed: Notify,
    pub(super) wait_registry: Arc<TaskWaitRegistry>,
    pub(crate) transition_event_lock: tokio::sync::RwLock<()>,
    pub(super) admission: AdmissionGate,
    pub(super) admission_budget: Arc<AdmissionBudget>,
    pub(crate) owner: Option<OwnerEpoch>,
    pub(crate) store_fault: Mutex<Option<String>>,
    #[cfg(feature = "event-bus")]
    pub(super) event_bus: Option<TaskEventPublisher>,
}

/// Signal belonging to one specific execution attempt of a task.
pub(crate) struct RunningCancellation {
    attempt: u32,
    signal: Arc<AtomicBool>,
}

/// Single service facade over volatile or restart-recoverable components.
///
/// The service owns admission and scheduling for its components. Call
/// [`shutdown`](Self::shutdown) before dropping the application runtime when
/// accepted work must be drained.
///
/// # Examples
///
/// ```
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let service = qubit_task::TaskExecutionService::in_memory().await?;
///     let capabilities = service.capabilities();
///     assert!(!capabilities.store.restart_recovery);
///     service.shutdown().await?;
///     Ok(())
/// }
/// ```
#[derive(Clone)]
pub struct TaskExecutionService {
    pub(crate) core: Arc<ServiceCore>,
}

impl TaskExecutionService {
    /// Builds an explicitly volatile, single-process service.
    pub async fn in_memory() -> Result<Self, super::task_execution_service_builder::TaskServiceBuildError> {
        TaskExecutionServiceBuilder::in_memory().build().await
    }

    /// Reports the selected store's history and recovery guarantees.
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
    #[must_use]
    pub fn last_store_error(&self) -> Option<String> {
        self.core.store_fault.lock().clone()
    }

    /// Returns lifecycle notification admission counters when a bus is
    /// configured.
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

    /// Finishes request acceptance in a background worker that holds an
    /// admission permit even if the caller is cancelled.
    async fn submit_admitted(
        &self,
        request: TaskRequest,
        reservation: AdmissionReservation,
    ) -> Result<TaskRecord, TaskServiceError> {
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
                publish_record(&self.core, &record);
                self.core.wait_registry.notify(record.id);
                Ok(record)
            }
            AcceptOutcome::Existing(record) => {
                self.release_queue_slot();
                Ok(record)
            }
        }
    }

    /// Submits a process-local closure when the selected store cannot promise
    /// restart recovery.
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

    /// Registers a local closure and retains it through acceptance and queue
    /// publication.
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
            resources: crate::model::ResourceRequest {
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
                publish_record(&self.core, &record);
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
    fn handle_store_error(&self, error: StoreError) -> TaskServiceError {
        if matches!(error, StoreError::Failure(_)) {
            record_store_fault(&self.core, error.to_string());
        }
        error.into()
    }

    /// Attempts to reserve bounded admission capacity before detaching a
    /// worker.
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

    /// Loads the latest lifecycle state of a task.
    pub async fn get(&self, id: TaskId) -> Result<Option<TaskRecord>, TaskServiceError> {
        self.core
            .store
            .get(id)
            .await
            .map_err(|error| self.handle_store_error(error))
    }

    /// Finds a retained task by the caller-supplied idempotency key.
    ///
    /// A missing record only means that the key is not committed at the time
    /// of this lookup. A submission worker may still be accepting it; retry
    /// `submit` with the same key and identical request to recover its record.
    pub async fn get_by_idempotency_key(&self, key: &str) -> Result<Option<TaskRecord>, TaskServiceError> {
        if key.is_empty() || key.len() > crate::model::MAX_IDEMPOTENCY_KEY_BYTES {
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
    pub async fn stats(&self) -> Result<TaskStats, TaskServiceError> {
        let TaskStateCounts {
            queued,
            running,
            blocked,
            terminal,
        } = self
            .core
            .store
            .count_states()
            .await
            .map_err(|error| self.handle_store_error(error))?;
        let resources = self.core.engine.capacity();
        Ok(TaskStats {
            queued,
            running,
            blocked,
            terminal,
            resources,
        })
    }

    /// Cancels queued or blocked work, or persists a cooperative cancellation
    /// request for running work before signalling its local handler.
    /// A running handler decides whether to acknowledge cancellation; a
    /// concurrent terminal transition returns `AlreadyTerminal`.
    pub async fn cancel(&self, id: TaskId) -> Result<CancelOutcome, TaskServiceError> {
        let _permit = self.core.admission.enter()?;
        let mut record = self
            .core
            .store
            .get(id)
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
                        .get(id)
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
    pub async fn retry_blocked(&self, id: TaskId) -> Result<TaskRecord, TaskServiceError> {
        let service = self.clone();
        await_admission(
            self.core
                .runtime_handle
                .spawn(async move { service.retry_blocked_admitted(id).await }),
        )
        .await
    }

    /// Requeues a blocked task while holding a permit through persistence and
    /// queue publication.
    async fn retry_blocked_admitted(&self, id: TaskId) -> Result<TaskRecord, TaskServiceError> {
        if let Some(error) = self.last_store_error() {
            return Err(TaskServiceError::StoreUnavailable(error));
        }
        let _permit = self.core.admission.enter()?;
        let record = self
            .core
            .store
            .get(id)
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

    /// Resolves when the task becomes terminal; returns an error if it becomes
    /// blocked.
    pub async fn wait(&self, id: TaskId) -> Result<TaskRecord, TaskServiceError> {
        let subscription = self.core.wait_registry.subscribe(id);
        loop {
            let notified = subscription.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(error) = self.last_store_error() {
                return Err(TaskServiceError::StoreUnavailable(error));
            }
            let record = self
                .core
                .store
                .get(id)
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
    pub async fn shutdown(&self) -> Result<(), TaskServiceError> {
        self.begin_shutdown();
        self.core.admission.wait_closed().await
    }

    /// Stops accepting work and waits until `deadline`; the shared shutdown
    /// coordinator continues draining if this caller times out.
    pub async fn shutdown_until(&self, deadline: tokio::time::Instant) -> Result<(), TaskServiceError> {
        self.begin_shutdown();
        tokio::time::timeout_at(deadline, self.core.admission.wait_closed())
            .await
            .map_err(|_| TaskServiceError::ShutdownTimedOut)?
    }

    fn begin_shutdown(&self) {
        if self.core.admission.close() {
            self.core.changed.notify_waiters();
            let service = self.clone();
            self.core.runtime_handle.spawn(async move {
                let primary = service.coordinate_shutdown().await;
                let result = combine_shutdown_results(primary, close_notification_publisher(&service.core).await);
                service.core.admission.finish_close(result);
                service.core.changed.notify_waiters();
            });
        }
    }

    /// Drains accepted work and releases store ownership after the admission
    /// gate is idle. The shutdown coordinator closes the notification worker
    /// after this convergence step returns.
    async fn coordinate_shutdown(&self) -> Result<(), TaskServiceError> {
        self.core.admission.wait_idle().await;
        let transition_guard = loop {
            let notified = self.core.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(error) = self.last_store_error() {
                return Err(TaskServiceError::StoreUnavailable(error));
            }
            let stats = self.stats().await?;
            if stats.queued == 0 && stats.running == 0 {
                let transition_guard = self.core.transition_event_lock.write().await;
                let settled_stats = self.stats().await?;
                if settled_stats.queued == 0 && settled_stats.running == 0 {
                    break transition_guard;
                }
            }
            notified.await;
        };
        if let Some(epoch) = self.core.owner
            && let Err(error) = self.core.store.release_owner(epoch).await
        {
            record_store_fault(&self.core, error.to_string());
            return Err(error.into());
        }
        drop(transition_guard);
        Ok(())
    }

    /// Starts the background scheduler and wraps its shared service state.
    pub(crate) fn start(core: ServiceCore) -> Self {
        let service = Self { core: Arc::new(core) };
        let weak = Arc::downgrade(&service.core);
        service.core.runtime_handle.spawn(scheduler_loop(weak));
        service
    }
}

/// Restores unprocessed tasks when a scheduler round exits early.
struct QueueWindowGuard {
    core: Arc<ServiceCore>,
    tasks: Option<Vec<QueuedTask>>,
}

impl QueueWindowGuard {
    /// Owns one bounded scheduler window until it is restored.
    fn new(core: Arc<ServiceCore>, tasks: Vec<QueuedTask>) -> Self {
        Self {
            core,
            tasks: Some(tasks),
        }
    }

    /// Borrows the current window for policy ordering and execution.
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
    fn drop(&mut self) {
        self.restore();
    }
}

/// Selects queued work, reserves resources, and starts eligible task attempts.
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
            if core.admission.is_closed() && core.queue.lock().is_empty() {
                return;
            }
            let notified = core.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let deadline = core.queue.lock().next_deadline();
            let wait = deadline.map(|value| std::time::Duration::from_millis(value.saturating_sub(now_ms())));
            if let Some(wait) = wait {
                let _ = futures::future::select(Box::pin(notified), Box::pin(tokio::time::sleep(wait))).await;
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
            let record = match core.store.get(id).await {
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
                    break;
                }
            };
            if core.store_fault.lock().is_some() {
                queue.push(task);
                return;
            }
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
            release_core_queue_slot(&core);
            if core.store_fault.lock().is_some() {
                return;
            }
            core.local_handlers.lock().remove(&id);
            let cancelled = Arc::new(AtomicBool::new(false));
            let context = TaskContext::new(id, running.attempt, assigned, cancelled);
            match core
                .engine
                .activate(prepared, handler, record.request.payload.clone(), context)
                .await
            {
                Ok(handle) => {
                    activated_positions.push(original_positions[&id]);
                    core.cancellations.lock().insert(
                        id,
                        RunningCancellation {
                            attempt: running.attempt,
                            signal: handle.cancelled.clone(),
                        },
                    );
                    match core.store.get(id).await {
                        Ok(Some(record))
                            if record.attempt == running.attempt
                                && matches!(record.state, TaskState::Running)
                                && record.cancel_requested =>
                        {
                            handle.cancelled.store(true, Ordering::Release);
                        }
                        Ok(Some(_)) | Ok(None) => {}
                        Err(error) => {
                            core.cancellations.lock().remove(&id);
                            pause_on_store_fault(&core, error);
                            return;
                        }
                    }
                    let weak = Arc::downgrade(&core);
                    core.runtime_handle
                        .spawn(finish_attempt(weak, running, handle.receiver, running_permit));
                    started = true;
                }
                Err(error) => {
                    let reason = format!("engine activation failed: {error}");
                    let mut latest = running;
                    loop {
                        match mark_blocked(&core, &latest, reason.clone()).await {
                            Ok(()) => break,
                            Err(StoreError::Conflict) => match core.store.get(id).await {
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
            tokio::pin!(notified);
            notified.as_mut().enable();
            let sleep_for = wait.unwrap_or(std::time::Duration::from_millis(40));
            let notified = Box::pin(notified);
            let timer = Box::pin(tokio::time::sleep(sleep_for));
            let _ = futures::future::select(notified, timer).await;
        } else {
            core.changed.notify_waiters();
        }
    }
}

/// Persists an execution result, retry decision, and local-handle completion.
async fn finish_attempt(
    core_ref: std::sync::Weak<ServiceCore>,
    running: TaskRecord,
    receiver: tokio::sync::oneshot::Receiver<ExecutionOutcome>,
    _running_permit: tokio::sync::OwnedSemaphorePermit,
) {
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
            message: truncate_utf8(message, crate::model::MAX_TASK_DIAGNOSTIC_MESSAGE_BYTES),
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
                crate::model::MAX_TASK_DIAGNOSTIC_MESSAGE_BYTES,
            ),
        },
        ExecutionOutcome::Returned(Err(error)) => TaskState::Failed {
            category: truncate_utf8(&error.category, crate::model::MAX_TASK_DIAGNOSTIC_CATEGORY_BYTES),
            message: truncate_utf8(&error.message, crate::model::MAX_TASK_DIAGNOSTIC_MESSAGE_BYTES),
        },
    };
    let output = match outcome {
        ExecutionOutcome::Returned(Ok(TaskRunOutcome::Succeeded(output))) => Some(output),
        _ => None,
    };
    let mut final_state = if output
        .as_ref()
        .is_some_and(|value| value.summary.len() > crate::model::MAX_TASK_OUTPUT_SUMMARY_BYTES)
    {
        TaskState::Failed {
            category: "output_too_large".into(),
            message: "task output summary exceeded the 65536-byte limit".into(),
        }
    } else {
        state
    };
    let output = output.filter(|value| value.summary.len() <= crate::model::MAX_TASK_OUTPUT_SUMMARY_BYTES);
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
        let latest = match core.store.get(running.id).await {
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

/// Persists a blocked state and completes any local handle waiting on it.
async fn mark_blocked(core: &ServiceCore, record: &TaskRecord, reason: String) -> Result<(), StoreError> {
    let updated = transition(
        core,
        record,
        TaskState::Blocked {
            reason: truncate_utf8(&reason, crate::model::MAX_TASK_DIAGNOSTIC_MESSAGE_BYTES),
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
fn finalize_local(core: &ServiceCore, id: TaskId, result: Result<TaskState, LocalTaskResultError>) {
    let sender = core.local_finalizations.lock().remove(&id);
    if let Some(sender) = sender {
        let _ = sender.send(result);
    }
}

/// Latches a worker-side store error and suspends service admission.
fn pause_on_store_fault(core: &Arc<ServiceCore>, error: StoreError) {
    record_store_fault(core, error.to_string());
}

/// Records the first storage failure and closes admission for all waiters.
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
    if core.admission.close() {
        let core = Arc::clone(core);
        let runtime_handle = core.runtime_handle.clone();
        runtime_handle.spawn(async move {
            core.admission.wait_idle().await;
            let notification = close_notification_publisher(&core).await;
            let result = combine_shutdown_results(Err(TaskServiceError::StoreUnavailable(diagnostic)), notification);
            core.admission.finish_close(result);
            core.changed.notify_waiters();
        });
    }
    core.changed.notify_waiters();
    core.wait_registry.notify_all();
}

/// Stops and drains the service-owned notification worker before shutdown
/// publishes its shared result.
///
/// With the `event-bus` feature, this blocks on the publisher's worker through
/// `spawn_blocking`; a provider that never returns can therefore keep the
/// service shutdown coordinator alive. The injected `EventBus` remains owned
/// by the application and is not shut down here.
///
/// # Errors
/// Returns `NotificationClose` when the worker fails to join or panics.
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
        (Err(primary), Err(close)) => Err(TaskServiceError::StoreUnavailable(format!(
            "{primary}; notification close failed: {close}"
        ))),
    }
}

/// Preserves an admission worker after caller cancellation and reports a
/// worker failure as an explicit service error.
/// Awaits an admission worker and maps task-join failures into service errors.
async fn await_admission<T>(
    handle: tokio::task::JoinHandle<Result<T, TaskServiceError>>,
) -> Result<T, TaskServiceError> {
    handle
        .await
        .map_err(|error| TaskServiceError::StoreUnavailable(format!("task admission worker stopped: {error}")))?
}

/// Enqueues a best-effort lifecycle event when event-bus support is enabled.
fn publish_record(core: &ServiceCore, record: &TaskRecord) {
    #[cfg(feature = "event-bus")]
    if let Some(bus) = &core.event_bus {
        bus.enqueue(crate::event::TaskEvent::from(record));
    }
    #[cfg(not(feature = "event-bus"))]
    let _ = (core, record);
}

/// Applies a version-checked store transition and publishes its new revision.
async fn transition(
    core: &ServiceCore,
    record: &TaskRecord,
    state: TaskState,
    output: Option<crate::model::TaskOutput>,
    assigned_resources: Vec<String>,
    cancel_requested: bool,
) -> Result<TaskRecord, StoreError> {
    transition_with_deadline(core, record, state, None, output, assigned_resources, cancel_requested).await
}

async fn transition_with_deadline(
    core: &ServiceCore,
    record: &TaskRecord,
    state: TaskState,
    retry_not_before_ms: Option<u64>,
    output: Option<crate::model::TaskOutput>,
    assigned_resources: Vec<String>,
    cancel_requested: bool,
) -> Result<TaskRecord, StoreError> {
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

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis().min(u64::MAX as u128) as u64)
}

fn retry_deadline_ms(now_ms: u64, policy: RetryPolicy, attempt: u32) -> u64 {
    now_ms.saturating_add(policy.delay_for_attempt(attempt).as_millis().min(u64::MAX as u128) as u64)
}

/// Validates request limits and whether configured resources can satisfy it.
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

/// Truncates a string without splitting a UTF-8 code point.
/// Truncates diagnostics at a UTF-8 boundary so persisted values stay valid.
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
fn release_core_queue_slot(core: &ServiceCore) {
    let _ = core
        .queue_count
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
            Some(count.saturating_sub(1))
        });
}

/// Attempts to reserve a queue slot for an automatic retry.
fn try_reserve_core_queue_slot(core: &ServiceCore) -> bool {
    core.queue_count
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
            (count < core.queue_capacity).then_some(count + 1)
        })
        .is_ok()
}

/// Returns the process-wide runtime used for service-owned background work.
pub(super) fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
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
    fn computes_retry_deadlines_with_saturating_milliseconds() {
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
    fn combines_shutdown_and_notification_results() {
        assert!(combine_shutdown_results(Ok(()), Ok(())).is_ok());
        assert!(matches!(
            combine_shutdown_results(
                Ok(()),
                Err(TaskServiceError::NotificationClose("close failed".into()))
            ),
            Err(TaskServiceError::NotificationClose(message)) if message == "close failed"
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
