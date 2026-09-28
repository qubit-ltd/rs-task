// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use futures::FutureExt;
pub(crate) use internal::RunningCancellation;
pub(crate) use internal::ServiceCore;
use internal::ServiceHandleLease;
use internal::begin_shutdown_core;
#[cfg(test)]
use internal::combine_shutdown_results;
use internal::finalize_local;
use internal::now_ms;
use internal::panic_message;
use internal::pause_on_store_fault;
use internal::publish_record;
use internal::record_scheduler_fault;
use internal::record_store_fault;
use internal::release_core_queue_slot;
use internal::retry_deadline_ms;
use internal::scheduler_loop;
use internal::task_stats;
use internal::transition;
use internal::transition_with_deadline;
use internal::truncate_utf8;
use internal::try_reserve_core_queue_slot;
use internal::validate_request;
use internal::validate_request_capacity;
use internal::validate_request_format;
use tokio::pin;
use tokio::runtime;
use tokio::sync::oneshot;
use tokio::task;
use tokio::time;

use super::admission_budget::AdmissionBudgetError;
use super::admission_budget::AdmissionReservation;
use super::cancel_outcome::CancelOutcome;
use super::local_task_handle::LocalTaskHandle;
use super::local_task_outcome::LocalTaskOutcome;
use super::local_task_outcome::adapt_local_outcome;
#[cfg(feature = "event-bus")]
use super::task_event_notification_stats::TaskEventNotificationStats;
#[cfg(feature = "event-bus")]
use super::task_event_publisher::TaskEventPublisher;
use super::task_execution_service_builder::TaskExecutionServiceBuilder;
use super::task_execution_service_builder::TaskServiceBuildError;
mod internal;
pub(crate) use super::task_service_capabilities::TaskServiceCapabilities;
pub(crate) use super::task_service_error::TaskServiceError;
use crate::engine::EngineError;
use crate::engine::ExecutionOutcome;
use crate::handler::LocalTaskHandler;
use crate::handler::TaskContext;
use crate::handler::TaskHandler;
use crate::handler::TaskHandlerDescriptor;
use crate::handler::TaskRunOutcome;
use crate::model::AcceptOutcome;
use crate::model::MAX_IDEMPOTENCY_KEY_BYTES;
use crate::model::MAX_TASK_DIAGNOSTIC_CATEGORY_BYTES;
use crate::model::MAX_TASK_DIAGNOSTIC_MESSAGE_BYTES;
use crate::model::MAX_TASK_OUTPUT_SUMMARY_BYTES;
use crate::model::ResourceRequest;
use crate::model::TaskId;
use crate::model::TaskPage;
use crate::model::TaskQuery;
use crate::model::TaskRecord;
use crate::model::TaskRequest;
use crate::model::TaskState;
use crate::model::TaskStats;
use crate::model::TaskSummary;
use crate::model::checked_page_size;
use crate::scheduling::QueueSnapshot;
use crate::scheduling::QueuedTask;
use crate::store::StoreError;

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

    /// Finds the payload-free summary for a retained idempotency key.
    ///
    /// A missing record only means that the key is not committed at the time
    /// of this lookup. A submission worker may still be accepting it; retry
    /// `submit` with the same key and identical request to recover its task.
    ///
    /// # Parameters
    ///
    /// * `key` - Caller-retained idempotency key.
    ///
    /// # Returns
    ///
    /// The matching retained summary, or `None` when not yet committed or
    /// absent.
    ///
    /// # Errors
    ///
    /// Returns `InvalidRequest` for an empty or oversized key, or a store
    /// error if lookup fails.
    pub async fn get_by_idempotency_key(&self, key: &str) -> Result<Option<TaskSummary>, TaskServiceError> {
        if key.is_empty() || key.len() > MAX_IDEMPOTENCY_KEY_BYTES {
            return Err(TaskServiceError::InvalidRequest(
                "idempotency key must contain between 1 and 256 bytes".into(),
            ));
        }
        self.core
            .store
            .get_summary_by_idempotency_key(key)
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
        let reservation = self.reserve_admission(0)?;
        let service = self.clone();
        await_admission(self.core.runtime_handle.spawn(async move {
            let _reservation = reservation;
            service
                .prune_terminal_before_admitted(accepted_before_ms, max_rows)
                .await
        }))
        .await
    }

    async fn prune_terminal_before_admitted(
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
        let reservation = self.reserve_admission(0)?;
        let service = self.clone();
        await_admission(self.core.runtime_handle.spawn(async move {
            let _reservation = reservation;
            service.cancel_admitted(id).await
        }))
        .await
    }

    async fn cancel_admitted(&self, id: TaskId) -> Result<CancelOutcome, TaskServiceError> {
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
        let reservation = self.reserve_admission(0)?;
        let service = self.clone();
        await_admission(self.core.runtime_handle.spawn(async move {
            let _reservation = reservation;
            service.retry_blocked_admitted(id).await
        }))
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
        let reservation = self.reserve_admission(0)?;
        let service = self.clone();
        await_admission(self.core.runtime_handle.spawn(async move {
            let _reservation = reservation;
            service.abandon_blocked_admitted(id, expected_version).await
        }))
        .await
    }

    async fn abandon_blocked_admitted(
        &self,
        id: TaskId,
        expected_version: u64,
    ) -> Result<TaskSummary, TaskServiceError> {
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
        validate_request_format(&request)?;
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
        let capacity = self.core.engine.capacity().capacity;
        validate_request_capacity(&request, &capacity)?;
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
    /// Returns the configured payload or operation limit error.
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
                AdmissionBudgetError::OperationLimitExceeded { limit } => {
                    TaskServiceError::OperationLimitExceeded { limit }
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
