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

use tokio::sync::oneshot;

use super::super::TaskExecutionService;
use super::fault::record_store_fault;
use super::publish_record;
use super::transition;
use super::validation::validate_request;
use super::validation::validate_request_capacity;
use super::validation::validate_request_format;
use crate::handler::LocalTaskHandler;
use crate::handler::TaskContext;
use crate::handler::TaskHandler;
use crate::handler::TaskHandlerDescriptor;
use crate::model::AcceptOutcome;
use crate::model::ResourceRequest;
use crate::model::TaskId;
use crate::model::TaskRecord;
use crate::model::TaskRequest;
use crate::model::TaskState;
use crate::model::TaskSummary;
use crate::scheduling::QueuedTask;
use crate::service::admission_budget::AdmissionBudgetError;
use crate::service::admission_budget::AdmissionReservation;
use crate::service::cancel_outcome::CancelOutcome;
use crate::service::local_task_handle::LocalTaskHandle;
use crate::service::local_task_outcome::LocalTaskOutcome;
use crate::service::local_task_outcome::adapt_local_outcome;
use crate::service::task_execution_service::TaskServiceError;
use crate::store::StoreError;

impl TaskExecutionService {
    /// Deletes terminal history while holding an admission permit through the
    /// store operation.
    ///
    /// # Parameters
    ///
    /// * `accepted_before_ms` - Exclusive acceptance-time cutoff.
    /// * `max_rows` - Maximum number of records to prune.
    ///
    /// # Returns
    ///
    /// The number of deleted records.
    ///
    /// # Errors
    ///
    /// Returns shutdown, store, or latched store-fault errors.
    pub(in crate::service::task_execution_service) async fn prune_terminal_before_admitted(
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

    /// Applies cancellation to queued work or records a cooperative request
    /// for a running attempt.
    ///
    /// # Parameters
    ///
    /// * `id` - Stable identity of the task to cancel.
    ///
    /// # Returns
    ///
    /// Whether the task was cancelled before start, cancellation was
    /// requested, or it was already terminal.
    ///
    /// # Errors
    ///
    /// Returns not-found, shutdown, scheduler, or store errors.
    pub(in crate::service::task_execution_service) async fn cancel_admitted(
        &self,
        id: TaskId,
    ) -> Result<CancelOutcome, TaskServiceError> {
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

    /// Cancels a blocked record only if it still has the reviewed revision.
    ///
    /// # Parameters
    ///
    /// * `id` - Stable identity of the blocked task.
    /// * `expected_version` - State revision observed by the caller.
    ///
    /// # Returns
    ///
    /// The committed cancelled summary.
    ///
    /// # Errors
    ///
    /// Returns not-found, conflict, not-blocked, shutdown, or store errors.
    pub(in crate::service::task_execution_service) async fn abandon_blocked_admitted(
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
    pub(in crate::service::task_execution_service) async fn submit_admitted(
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
    pub(in crate::service::task_execution_service) async fn submit_local_admitted<F, R, E>(
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
    pub(in crate::service::task_execution_service) fn handle_store_error(&self, error: StoreError) -> TaskServiceError {
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
    pub(in crate::service::task_execution_service) fn reserve_admission(
        &self,
        payload_bytes: usize,
    ) -> Result<AdmissionReservation, TaskServiceError> {
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
    pub(in crate::service::task_execution_service) fn reserve_queue_slot(&self) -> Result<(), TaskServiceError> {
        self.core
            .queue_count
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                (count < self.core.queue_capacity).then_some(count + 1)
            })
            .map(|_| ())
            .map_err(|_| TaskServiceError::QueueFull)
    }

    /// Releases one previously reserved waiting-queue position.
    pub(in crate::service::task_execution_service) fn release_queue_slot(&self) {
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
    pub(in crate::service::task_execution_service) async fn retry_blocked_admitted(
        &self,
        id: TaskId,
    ) -> Result<TaskSummary, TaskServiceError> {
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
