// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::any::Any;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use futures::FutureExt;
use qubit_progress::AsyncReporter;

use super::super::super::task_progress_reporter::TaskProgressReporter;
use super::super::now_ms;
use super::super::prepare_handler;
use super::ExternalCancelEntry;
use super::ExternalCancelFlight;
use super::ExternalCancelResult;
use super::InFlightGuard;
use super::TypedServiceCore;
use super::resource_fairness::ResourceFairness;
use crate::engine::EngineError;
use crate::engine::TypedResourceReservation;
use crate::handler::TaskRunOutcome;
use crate::handler::typed::CancellationMode;
use crate::handler::typed::TypedTaskContext;
use crate::model::MAX_TASK_OUTPUT_SUMMARY_BYTES;
use crate::model::TaskOutput;
use crate::model::TaskState;
use crate::model::typed::TaskId;
use crate::model::typed::TaskSummary;
use crate::model::typed::TransitionCommand;
use crate::service::TaskServiceError;

/// Keeps shared service state available to background workers.
#[derive(Clone)]
pub(in crate::service::typed_task_execution_service) struct ServiceRunner {
    /// Shared service state used by scheduler and supervisor tasks.
    pub(in crate::service::typed_task_execution_service) core: Arc<TypedServiceCore>,
}

impl ServiceRunner {
    /// Returns the first latched store or scheduler fault, if any.
    ///
    /// # Returns
    ///
    /// `Ok(())` when no fault has been latched, or the first fault as a service
    /// error.
    ///
    /// # Errors
    ///
    /// Returns the first fault recorded by the service.
    pub(in crate::service::typed_task_execution_service) fn check_fault(&self) -> Result<(), TaskServiceError> {
        self.core
            .fault
            .lock()
            .clone()
            .map_or(Ok(()), |fault| Err(TaskServiceError::StoreUnavailable(fault)))
    }

    /// Runs the registered external cancellation hook and reconciles its result
    /// with the persisted task state for all concurrent cancellation waiters.
    ///
    /// # Parameters
    ///
    /// - `core`: Shared service state used to access handlers, storage, and
    ///   waiters.
    /// - `key`: The task ID and attempt associated with the cancellation
    ///   request.
    /// - `kind_id`: The registered handler kind whose external hook is invoked.
    /// - `flight`: Shared result state observed by concurrent cancellation
    ///   callers.
    pub(in crate::service::typed_task_execution_service) async fn run_external_cancel(
        core: Arc<TypedServiceCore>,
        key: (TaskId, u32),
        kind_id: String,
        flight: Arc<ExternalCancelFlight>,
    ) {
        let (id, attempt) = key;
        let runner = Self {
            core: Arc::clone(&core),
        };
        let result = async {
            loop {
                let task = match core.store.get_encoded_task(id).await {
                    Ok(Some(task)) => task,
                    Ok(None) => {
                        return ExternalCancelResult::ServiceFailed(
                            "task disappeared during external cancellation".into(),
                        );
                    }
                    Err(error) => return ExternalCancelResult::ServiceFailed(error.to_string()),
                };
                let summary = task.summary;
                if summary.state.is_terminal()
                    || summary.attempt != attempt
                    || !matches!(summary.state, TaskState::Running)
                {
                    return ExternalCancelResult::AlreadyTerminal;
                }
                if !summary.cancel_requested || summary.cancel_error.is_some() {
                    match core
                        .store
                        .transition_encoded(TransitionCommand {
                            id,
                            expected_state_version: summary.state_version,
                            expected_attempt: attempt,
                            retry_not_before_ms: None,
                            state: TaskState::Running,
                            cancel_requested: true,
                            cancel_error: None,
                            finished_at_ms: None,
                            output: None,
                        })
                        .await
                    {
                        Ok(_) => {}
                        Err(crate::store::StoreError::Conflict) => continue,
                        Err(error) => {
                            return ExternalCancelResult::ServiceFailed(error.to_string());
                        }
                    }
                }
                break;
            }
            let hook = match std::panic::catch_unwind(AssertUnwindSafe(|| {
                core.handlers.cancel_externally(&kind_id, id, attempt)
            })) {
                Ok(Some(hook)) => hook,
                Ok(None) => {
                    return ExternalCancelResult::ServiceFailed("external cancellation hook is unavailable".into());
                }
                Err(panic) => {
                    let message = Self::external_cancel_panic_message(panic);
                    return Self::record_external_cancel_failure(&core, id, attempt, message).await;
                }
            };
            match AssertUnwindSafe(hook).catch_unwind().await {
                Ok(Ok(())) => loop {
                    let task = match core.store.get_encoded_task(id).await {
                        Ok(Some(task)) => task,
                        Ok(None) => {
                            return ExternalCancelResult::ServiceFailed(
                                "task disappeared after external cancellation".into(),
                            );
                        }
                        Err(error) => {
                            return ExternalCancelResult::ServiceFailed(error.to_string());
                        }
                    };
                    let summary = task.summary;
                    if summary.state.is_terminal()
                        || summary.attempt != attempt
                        || !matches!(summary.state, TaskState::Running)
                    {
                        return ExternalCancelResult::AlreadyTerminal;
                    }
                    if summary.cancel_error.is_none() {
                        return ExternalCancelResult::Success;
                    }
                    match core
                        .store
                        .transition_encoded(TransitionCommand {
                            id,
                            expected_state_version: summary.state_version,
                            expected_attempt: attempt,
                            retry_not_before_ms: None,
                            state: TaskState::Running,
                            cancel_requested: true,
                            cancel_error: None,
                            finished_at_ms: None,
                            output: None,
                        })
                        .await
                    {
                        Ok(_) => return ExternalCancelResult::Success,
                        Err(crate::store::StoreError::Conflict) => continue,
                        Err(error) => {
                            return ExternalCancelResult::ServiceFailed(error.to_string());
                        }
                    }
                },
                Ok(Err(error)) => Self::record_external_cancel_failure(&core, id, attempt, error.message).await,
                Err(panic) => {
                    let message = Self::external_cancel_panic_message(panic);
                    Self::record_external_cancel_failure(&core, id, attempt, message).await
                }
            }
        }
        .await;
        match &result {
            ExternalCancelResult::Success => {
                core.external_cancellations
                    .lock()
                    .insert(key, ExternalCancelEntry::Succeeded);
            }
            _ => {
                core.external_cancellations.lock().remove(&key);
            }
        }
        if let ExternalCancelResult::ServiceFailed(message) = &result {
            runner.latch_fault(message.clone());
        }
        flight.result.send_replace(Some(result));
        core.scheduler_changed.notify_one();
    }

    /// Waits for a stop request, drains work, and publishes one terminal
    /// result.
    ///
    /// Shutdown is requested by a service handle or by a latched service fault.
    pub(in crate::service::typed_task_execution_service) async fn shutdown_supervisor(&self) {
        self.wait_for_shutdown_request().await;
        self.stop_scheduler_and_wait().await;
        self.wait_for_active_operations().await;
        #[cfg(feature = "event-bus")]
        self.close_notification_publisher().await;
        self.release_store_owner_until_complete().await;
    }

    /// Wakes the optional durable event publisher after a store write.
    ///
    /// No action is taken when the optional publisher is not configured.
    #[cfg(feature = "event-bus")]
    pub(in crate::service::typed_task_execution_service) fn notify_notifications(&self) {
        if let Some(publisher) = &self.core.publisher {
            publisher.notify();
        }
    }

    /// Converts a store write result and wakes the optional publisher.
    ///
    /// # Type Parameters
    ///
    /// - `T`: Successful value returned by the store operation.
    ///
    /// # Parameters
    ///
    /// - `result`: Result of the store write to convert and publish.
    ///
    /// # Returns
    ///
    /// The successful store value, or a service error for a failed write.
    ///
    /// # Errors
    ///
    /// Returns a service error when the store operation fails.
    pub(in crate::service::typed_task_execution_service) fn store_write_result<T>(
        &self,
        result: Result<T, crate::store::StoreError>,
    ) -> Result<T, TaskServiceError> {
        let value = self.store_result(result)?;
        #[cfg(feature = "event-bus")]
        self.notify_notifications();
        Ok(value)
    }

    /// Records the first scheduler/store fault and wakes every shutdown stage.
    ///
    /// # Parameters
    ///
    /// - `error`: Displayable description of the fault to latch.
    pub(in crate::service::typed_task_execution_service) fn latch_fault(&self, error: impl std::fmt::Display) {
        let mut fault = self.core.fault.lock();
        if fault.is_none() {
            *fault = Some(error.to_string());
        }
        drop(fault);
        self.core.request_shutdown();
        self.core.in_flight_changed.notify_waiters();
    }

    /// Latches operational store failures while preserving conflict errors.
    ///
    /// # Type Parameters
    ///
    /// - `T`: Successful value returned by the store operation.
    ///
    /// # Parameters
    ///
    /// - `result`: Store operation result to preserve or convert.
    ///
    /// # Returns
    ///
    /// The successful value, or the corresponding service error.
    ///
    /// # Errors
    ///
    /// Returns a service error for a failed store operation. Conflicts and
    /// missing records are returned without latching a service fault.
    pub(in crate::service::typed_task_execution_service) fn store_result<T>(
        &self,
        result: Result<T, crate::store::StoreError>,
    ) -> Result<T, TaskServiceError> {
        result.map_err(|error| {
            if !matches!(
                error,
                crate::store::StoreError::Conflict | crate::store::StoreError::NotFound
            ) {
                self.latch_fault(&error);
            }
            TaskServiceError::Store(error)
        })
    }

    /// Maps known rejection errors from a single submit store result to
    /// structured service errors without changing the service state. Other
    /// store errors retain the existing fault-latching behavior.
    ///
    /// # Type Parameters
    ///
    /// - `T`: Successful value returned by the submission store operation.
    ///
    /// # Parameters
    ///
    /// - `result`: Submission store result to map to a service-level outcome.
    ///
    /// # Returns
    ///
    /// The successful value, or a service error describing the rejection or
    /// store failure.
    ///
    /// # Errors
    ///
    /// Returns a structured service error for a rejected submission or a
    /// service error for another failed store operation.
    pub(in crate::service::typed_task_execution_service) fn submission_store_result<T>(
        &self,
        result: Result<T, crate::store::StoreError>,
    ) -> Result<T, TaskServiceError> {
        match result {
            Err(crate::store::StoreError::CapacityExceeded {
                requested_bytes,
                available_bytes,
            }) => Err(TaskServiceError::SubmissionCapacityExceeded {
                requested_bytes,
                available_bytes,
            }),
            Err(crate::store::StoreError::UnfinishedRecordLimitExceeded { limit }) => {
                Err(TaskServiceError::UnfinishedTaskLimitExceeded { limit })
            }
            Err(crate::store::StoreError::IdempotencyConflict) => Err(TaskServiceError::IdempotencyConflict),
            Err(crate::store::StoreError::DuplicateTask) => Err(TaskServiceError::DuplicateTaskId),
            other => self.store_result(other),
        }
    }

    /// Restores tasks left queued or running by the previous service owner.
    ///
    /// Running tasks are returned to the queue unless cancellation was pending,
    /// in which case they are blocked to preserve that cancellation request.
    ///
    /// # Returns
    ///
    /// `Ok(())` after all queued and running tasks have been examined and
    /// recoverable running tasks have been restored.
    ///
    /// # Errors
    ///
    /// Returns an error if listing tasks or restoring a task state fails.
    pub(in crate::service::typed_task_execution_service) async fn recover_unfinished(
        &self,
    ) -> Result<(), TaskServiceError> {
        let mut after = None;
        loop {
            let page = self
                .core
                .store
                .list_encoded(crate::model::typed::TaskQuery {
                    states: vec![
                        crate::model::TaskStateKind::Queued,
                        crate::model::TaskStateKind::Running,
                    ],
                    category: None,
                    correlation_key: None,
                    after,
                    limit: 128,
                })
                .await?;
            for summary in page.records {
                if matches!(summary.state, TaskState::Running) {
                    let (state, keep_cancel) = if summary.cancel_requested {
                        (
                            TaskState::Blocked {
                                reason: "service stopped while cancellation was pending".into(),
                            },
                            true,
                        )
                    } else {
                        (TaskState::Queued, false)
                    };
                    let _recovered = self.store_write_result(
                        self.core
                            .store
                            .transition_encoded(TransitionCommand {
                                id: summary.id,
                                expected_state_version: summary.state_version,
                                expected_attempt: summary.attempt,
                                retry_not_before_ms: None,
                                state,
                                cancel_requested: keep_cancel,
                                cancel_error: summary.cancel_error.clone(),
                                finished_at_ms: None,
                                output: None,
                            })
                            .await,
                    )?;
                }
            }
            after = page.next;
            if after.is_none() {
                self.core.scheduler_changed.notify_one();
                return Ok(());
            }
        }
    }

    /// Scans ready tasks, reserves resources, and starts eligible handlers
    /// until shutdown or a latched service fault stops dispatch.
    ///
    /// Returns immediately when shutdown begins or a scheduler/store fault is
    /// latched; otherwise waits for new work or the next retry deadline.
    pub(in crate::service::typed_task_execution_service) async fn scheduler_loop(&self) {
        let mut fairness = ResourceFairness::default();
        'scheduler: loop {
            if self.core.shutting_down.load(Ordering::Acquire) || self.core.fault.lock().is_some() {
                return;
            }
            let notified = self.core.scheduler_changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.core.in_flight.load(Ordering::Acquire) < self.core.max_running_tasks {
                let now = now_ms();
                match self.scan_ready_pages(now, &mut fairness).await {
                    Ok(true) => {
                        fairness = ResourceFairness::default();
                        continue 'scheduler;
                    }
                    Ok(false) => {}
                    Err(error) => {
                        self.latch_fault(error);
                        return;
                    }
                }
            }
            let retry_at = match self.core.store.next_retry_deadline(now_ms()).await {
                Ok(deadline) => deadline,
                Err(error) => {
                    self.latch_fault(error);
                    return;
                }
            };
            if let Some(retry_at) = retry_at {
                let wait = std::time::Duration::from_millis(retry_at.saturating_sub(now_ms()));
                let _ = tokio::time::timeout(wait, notified).await;
            } else {
                notified.await;
            }
        }
    }

    /// Extracts a readable message from a panic payload, using a stable
    /// fallback for payloads with an unsupported type.
    ///
    /// # Parameters
    ///
    /// - `panic`: The payload captured from the external cancellation hook.
    ///
    /// # Returns
    ///
    /// The panic message, or a stable fallback when the payload is not a
    /// string.
    fn external_cancel_panic_message(panic: Box<dyn Any + Send>) -> String {
        panic.downcast_ref::<&str>().map_or_else(
            || {
                panic
                    .downcast_ref::<String>()
                    .cloned()
                    .unwrap_or_else(|| "external cancellation hook panicked".into())
            },
            |message| (*message).to_owned(),
        )
    }

    /// Persists a cancellation hook failure while retrying revision conflicts.
    ///
    /// Returns `AlreadyTerminal` if the task attempt ended before the failure
    /// could be recorded, and `ServiceFailed` for store errors.
    ///
    /// # Parameters
    ///
    /// - `core`: Service state used to read and update the task record.
    /// - `id`: ID of the task whose cancellation hook failed.
    /// - `attempt`: Attempt number whose failure is being recorded.
    /// - `message`: Failure message to persist for the attempt.
    ///
    /// # Returns
    ///
    /// The recorded hook failure, `AlreadyTerminal` if the attempt ended, or
    /// `ServiceFailed` if the store operation failed.
    async fn record_external_cancel_failure(
        core: &Arc<TypedServiceCore>,
        id: TaskId,
        attempt: u32,
        message: String,
    ) -> ExternalCancelResult {
        loop {
            let task = match core.store.get_encoded_task(id).await {
                Ok(Some(task)) => task,
                Ok(None) => {
                    return ExternalCancelResult::ServiceFailed(
                        "task disappeared while recording cancellation failure".into(),
                    );
                }
                Err(error) => return ExternalCancelResult::ServiceFailed(error.to_string()),
            };
            let summary = task.summary;
            if summary.state.is_terminal() || summary.attempt != attempt || !matches!(summary.state, TaskState::Running)
            {
                return ExternalCancelResult::AlreadyTerminal;
            }
            match core
                .store
                .transition_encoded(TransitionCommand {
                    id,
                    expected_state_version: summary.state_version,
                    expected_attempt: attempt,
                    retry_not_before_ms: None,
                    state: TaskState::Running,
                    cancel_requested: true,
                    cancel_error: Some(message.clone()),
                    finished_at_ms: None,
                    output: None,
                })
                .await
            {
                Ok(_) => return ExternalCancelResult::HookFailed(message),
                Err(crate::store::StoreError::Conflict) => continue,
                Err(error) => return ExternalCancelResult::ServiceFailed(error.to_string()),
            }
        }
    }

    /// Waits until shutdown is requested by a handle or a service fault.
    async fn wait_for_shutdown_request(&self) {
        loop {
            let notified = self.core.shutdown_changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.core.shutting_down.load(Ordering::Acquire) {
                break;
            }
            notified.await;
        }
    }

    /// Closes admission, wakes dispatch, and waits until the scheduler exits.
    async fn stop_scheduler_and_wait(&self) {
        let admission = self.core.admission.write().await;
        self.core.scheduler_changed.notify_waiters();
        drop(admission);
        loop {
            let notified = self.core.scheduler_stopped_changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.core.scheduler_stopped.load(Ordering::Acquire) {
                break;
            }
            notified.await;
        }
    }

    /// Waits until every handler and external cancellation operation exits.
    async fn wait_for_active_operations(&self) {
        loop {
            let notified = self.core.in_flight_changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.core.in_flight.load(Ordering::Acquire) == 0 {
                break;
            }
            notified.await;
        }
    }

    /// Closes the durable notification publisher and records any close error.
    #[cfg(feature = "event-bus")]
    async fn close_notification_publisher(&self) {
        if let Some(publisher) = &self.core.publisher
            && let Err(error) = publisher.close().await
        {
            *self.core.notification_close_error.lock() = Some(error.to_string());
        }
    }

    /// Releases exclusive store ownership and serves retries from shutdown
    /// callers.
    async fn release_store_owner_until_complete(&self) {
        loop {
            let release = {
                let mut guard = self.core.owner_guard.lock().await;
                match guard.as_mut() {
                    Some(owner_guard) => owner_guard.release().await,
                    None => Err(crate::store::StoreError::Failure(
                        "started service has no owner release guard".into(),
                    )),
                }
            };
            let release_error = release.err().map(|error| error.to_string());
            let retryable = release_error.is_some();
            if !retryable {
                self.core.owner_released.store(true, Ordering::Release);
            }
            let outcome = if let Some(fault) = self.core.fault.lock().clone() {
                ShutdownOutcome::StoreUnavailable(fault)
            } else if let Some(error) = release_error {
                ShutdownOutcome::StoreUnavailable(error)
            } else {
                #[cfg(feature = "event-bus")]
                {
                    if let Some(error) = self.core.notification_close_error.lock().clone() {
                        ShutdownOutcome::NotificationClose(error)
                    } else {
                        ShutdownOutcome::Complete
                    }
                }
                #[cfg(not(feature = "event-bus"))]
                {
                    ShutdownOutcome::Complete
                }
            };
            {
                let mut progress = self.core.shutdown_progress.lock();
                progress.results.push(outcome);
                progress.retryable_release = retryable;
                progress.retry_requested = false;
            }
            self.core.shutdown_completed.notify_waiters();
            if !retryable {
                return;
            }
            loop {
                let notified = self.core.shutdown_changed.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self.core.public_gone.load(Ordering::Acquire) {
                    let armed_guard = self.core.owner_guard.lock().await.take();
                    drop(armed_guard);
                    return;
                }
                if self.core.shutdown_progress.lock().retry_requested {
                    break;
                }
                notified.await;
            }
        }
    }

    /// Pages through ready tasks and dispatches each eligible task in order.
    ///
    /// The cursor and anchor observation stay local to one complete scan.
    /// Fairness state is passed explicitly because reservations can establish
    /// or advance its anchor. Returns `true` when the full scan missed an
    /// existing anchor, so the caller can reset fairness and scan again.
    ///
    /// # Parameters
    ///
    /// - `now`: Timestamp used to select tasks that are ready to run.
    /// - `fairness`: Mutable fairness state updated as reservations are
    ///   attempted.
    ///
    /// # Returns
    ///
    /// `true` when a completed scan missed its fairness anchor, and `false`
    /// otherwise.
    ///
    /// # Errors
    ///
    /// Returns a store error if reading a page or starting an eligible task
    /// fails.
    async fn scan_ready_pages(
        &self,
        now: u64,
        fairness: &mut ResourceFairness,
    ) -> Result<bool, crate::store::StoreError> {
        let mut after = None;
        let mut saw_anchor = fairness.anchor.is_none();
        let mut completed_scan = false;
        loop {
            if self.core.shutting_down.load(Ordering::Acquire)
                || self.core.fault.lock().is_some()
                || self.core.in_flight.load(Ordering::Acquire) >= self.core.max_running_tasks
            {
                break;
            }
            let page = self
                .core
                .store
                .list_ready_queued(
                    after,
                    std::num::NonZeroUsize::new(self.core.scan_page_size).expect("scan size is nonzero"),
                    now,
                )
                .await?;
            for summary in &page.records {
                if self.core.shutting_down.load(Ordering::Acquire)
                    || self.core.in_flight.load(Ordering::Acquire) >= self.core.max_running_tasks
                {
                    break;
                }
                if fairness.anchor == Some((summary.id, summary.accepted_at_ms)) {
                    saw_anchor = true;
                }
                if !self.dispatch_is_eligible(summary).await {
                    continue;
                }
                let Some(reservation) = self.reserve_task_resources(summary, fairness).await else {
                    if fairness.anchor == Some((summary.id, summary.accepted_at_ms)) {
                        saw_anchor = true;
                    }
                    continue;
                };
                if let Some((anchor_id, anchor_accepted_at_ms)) = fairness.anchor
                    && fairness.bypasses >= self.core.max_resource_bypasses
                    && (summary.accepted_at_ms, summary.id) > (anchor_accepted_at_ms, anchor_id)
                {
                    drop(reservation);
                    continue;
                }
                let admission = self.core.admission.read().await;
                if self.core.shutting_down.load(Ordering::Acquire) {
                    drop(reservation);
                    break;
                }
                let running = match self
                    .core
                    .store
                    .start_encoded(crate::model::typed::StartCommand {
                        id: summary.id,
                        expected_state_version: summary.state_version,
                        started_at_ms: now_ms(),
                    })
                    .await
                {
                    Ok(running) => running,
                    Err(crate::store::StoreError::Conflict | crate::store::StoreError::NotFound) => {
                        drop(admission);
                        drop(reservation);
                        continue;
                    }
                    Err(error) => {
                        drop(admission);
                        drop(reservation);
                        return Err(error);
                    }
                };
                drop(admission);
                if let Some((anchor_id, anchor_accepted_at_ms)) = fairness.anchor {
                    let key = (summary.accepted_at_ms, summary.id);
                    let anchor_key = (anchor_accepted_at_ms, anchor_id);
                    if key == anchor_key {
                        *fairness = ResourceFairness::default();
                    } else if key > anchor_key {
                        fairness.bypasses += 1;
                    }
                }
                #[cfg(feature = "event-bus")]
                self.notify_notifications();
                self.core.in_flight.fetch_add(1, Ordering::AcqRel);
                let service = self.clone();
                let core = Arc::clone(&self.core);
                let id = summary.id;
                tokio::spawn(async move {
                    let _in_flight = InFlightGuard(core);
                    if AssertUnwindSafe(service.run_one(id, running, reservation))
                        .catch_unwind()
                        .await
                        .is_err()
                    {
                        service.latch_fault(format!(
                            "task execution supervisor panicked for {}",
                            id.to_padded_decimal()
                        ));
                    }
                });
            }
            if self.core.shutting_down.load(Ordering::Acquire)
                || self.core.in_flight.load(Ordering::Acquire) >= self.core.max_running_tasks
            {
                break;
            }
            after = page.next;
            if after.is_none() {
                completed_scan = true;
                break;
            }
        }
        Ok(completed_scan && !saw_anchor && fairness.anchor.is_some())
    }

    /// Validates that a queued task has a registered handler and compatible
    /// payload.
    ///
    /// Inadmissible tasks are moved to the blocked state before returning
    /// `false`; eligible tasks return `true` for subsequent resource
    /// reservation and start.
    ///
    /// # Parameters
    ///
    /// - `summary`: Queued task metadata checked against registered handlers
    ///   and codecs.
    ///
    /// # Returns
    ///
    /// `true` when the task can proceed; otherwise `false` after marking it
    /// blocked.
    async fn dispatch_is_eligible(&self, summary: &TaskSummary) -> bool {
        let Some(descriptor) = self.core.handlers.descriptor(&summary.kind_id) else {
            self.mark_blocked(summary, "handler is not registered").await;
            return false;
        };
        if summary.payload_type_id != descriptor.payload_type_id.as_str()
            || !descriptor
                .accepted_schema_versions
                .contains(&summary.payload_schema_version)
        {
            self.mark_blocked(
                summary,
                "handler does not accept the payload identity or schema version",
            )
            .await;
            return false;
        }
        if self.core.codecs.get(&summary.payload_codec_id).is_none() {
            self.mark_blocked(summary, "payload bytes codec is not registered")
                .await;
            return false;
        }
        true
    }

    /// Reserves the task's requested resources or records why it cannot
    /// proceed.
    ///
    /// Temporary capacity pressure establishes the fairness anchor; permanent
    /// reservation failures move the task to the blocked state. The returned
    /// lease stays owned by the caller and is moved into the spawned task.
    ///
    /// # Parameters
    ///
    /// - `summary`: Task metadata containing its ID and resource request.
    /// - `fairness`: Mutable state updated when temporary capacity pressure
    ///   establishes the fairness anchor.
    ///
    /// # Returns
    ///
    /// A reservation lease when resources are available, or `None` when the
    /// task cannot currently proceed.
    #[must_use]
    async fn reserve_task_resources(
        &self,
        summary: &TaskSummary,
        fairness: &mut ResourceFairness,
    ) -> Option<TypedResourceReservation> {
        match self
            .core
            .engine
            .try_prepare_typed(summary.id, summary.resource_limit.clone())
        {
            Ok(reservation) => Some(reservation),
            Err(EngineError::TemporarilyUnavailable) => {
                if fairness.anchor.is_none() {
                    fairness.anchor = Some((summary.id, summary.accepted_at_ms));
                }
                None
            }
            Err(EngineError::Unsatisfiable) => {
                self.mark_blocked(summary, "requested resources exceed configured capacity")
                    .await;
                None
            }
            Err(error) => {
                self.mark_blocked(summary, &error.to_string()).await;
                None
            }
        }
    }

    /// Loads and executes one admitted task attempt, then persists its outcome.
    ///
    /// The reservation remains held for the duration of handler execution and
    /// is released on every return path.
    ///
    /// # Parameters
    ///
    /// - `id`: ID of the admitted task to load and execute.
    /// - `running`: Persisted summary for the attempt that was started.
    /// - `reservation`: Resource lease held until handler execution completes.
    async fn run_one(&self, id: TaskId, running: TaskSummary, reservation: TypedResourceReservation) {
        let task = match self.core.store.get_encoded_task(id).await {
            Ok(Some(task)) => task,
            Ok(None) => return,
            Err(error) => {
                self.latch_fault(error);
                return;
            }
        };
        if !matches!(task.summary.state, TaskState::Running) {
            return;
        }
        let prepared = match prepare_handler(
            &self.core.handlers,
            &self.core.codecs,
            &running.kind_id,
            task.request.payload,
        ) {
            Ok(prepared) => prepared,
            Err(message) => {
                self.finish(
                    &running,
                    TaskState::Failed {
                        category: "payload_decode".into(),
                        message,
                    },
                    None,
                    false,
                )
                .await;
                return;
            }
        };
        let cancelled = Arc::new(AtomicBool::new(false));
        self.core
            .cancellations
            .lock()
            .insert((id, running.attempt), Arc::clone(&cancelled));
        let latest_cancel_requested = match self.core.store.get_encoded_task(id).await {
            Ok(task) => task.is_some_and(|task| task.summary.cancel_requested),
            Err(error) => {
                self.latch_fault(error);
                return;
            }
        };
        if latest_cancel_requested {
            cancelled.store(true, Ordering::Release);
        }
        let reporter: Arc<dyn AsyncReporter> = Arc::new(TaskProgressReporter::new(
            Arc::clone(&self.core.store),
            id,
            running.attempt,
        ));
        let context = TypedTaskContext::new(id, running.attempt, cancelled, reporter);
        let execution = AssertUnwindSafe(prepared.run(context)).catch_unwind().await;
        self.core.cancellations.lock().remove(&(id, running.attempt));
        drop(reservation);
        match execution {
            Ok(Ok(TaskRunOutcome::Succeeded(output))) => {
                if output.summary.len() <= MAX_TASK_OUTPUT_SUMMARY_BYTES {
                    self.finish(&running, TaskState::Succeeded, Some(output), false).await;
                } else {
                    self.finish(
                        &running,
                        TaskState::Failed {
                            category: "task_output_too_large".into(),
                            message: format!(
                                "task output summary exceeds the {}-byte limit",
                                MAX_TASK_OUTPUT_SUMMARY_BYTES
                            ),
                        },
                        None,
                        false,
                    )
                    .await;
                }
            }
            Ok(Ok(TaskRunOutcome::Cancelled)) => self.finish(&running, TaskState::Cancelled, None, false).await,
            Ok(Err(error)) => {
                let retryable = error.retryable;
                self.finish(
                    &running,
                    TaskState::Failed {
                        category: error.category,
                        message: error.message,
                    },
                    None,
                    retryable,
                )
                .await
            }
            Err(_) => {
                self.finish(
                    &running,
                    TaskState::Panicked {
                        message: "typed task handler panicked".into(),
                    },
                    None,
                    false,
                )
                .await
            }
        }
    }

    /// Moves an inadmissible queued task to the blocked state using its
    /// observed revision, latching unexpected store failures for the
    /// service supervisor.
    ///
    /// # Parameters
    ///
    /// - `summary`: Queued task revision used for the conditional transition.
    /// - `reason`: Explanation persisted with the blocked state.
    async fn mark_blocked(&self, summary: &TaskSummary, reason: &str) {
        match self
            .core
            .store
            .transition_encoded(TransitionCommand {
                id: summary.id,
                expected_state_version: summary.state_version,
                expected_attempt: summary.attempt,
                retry_not_before_ms: None,
                state: TaskState::Blocked { reason: reason.into() },
                cancel_requested: false,
                cancel_error: None,
                finished_at_ms: None,
                output: None,
            })
            .await
        {
            Ok(_updated) => {
                #[cfg(feature = "event-bus")]
                self.notify_notifications();
            }
            Err(crate::store::StoreError::Conflict) => {
                self.check_competing_transition(summary.id, summary.state_version).await
            }
            Err(error) => self.latch_fault(error),
        }
    }

    /// Persists the final attempt state, scheduling a retry when policy
    /// permits.
    ///
    /// A newer terminal transition wins; other unexpected store failures latch
    /// a service fault rather than being silently discarded.
    ///
    /// # Parameters
    ///
    /// - `started`: Summary of the attempt whose outcome is being persisted.
    /// - `state`: State to persist if the attempt is not retried.
    /// - `output`: Optional output produced by the attempt.
    /// - `retryable`: Whether retry policy may schedule another attempt.
    async fn finish(&self, started: &TaskSummary, state: TaskState, output: Option<TaskOutput>, retryable: bool) {
        let latest = match self.core.store.get_encoded_task(started.id).await {
            Ok(Some(latest)) => latest,
            Ok(None) => {
                self.latch_fault("task disappeared while finalizing");
                return;
            }
            Err(error) => {
                self.latch_fault(error);
                return;
            }
        };
        if latest.summary.state.is_terminal() {
            self.core
                .external_cancellations
                .lock()
                .remove(&(started.id, started.attempt));
            return;
        }
        let retry = retryable && !latest.summary.cancel_requested && started.attempt < self.core.max_attempts;
        let retry_deadline = retry.then(|| {
            let delay = self.core.retry_policy.delay_for_attempt(started.attempt);
            now_ms().saturating_add(delay.as_millis().min(u64::MAX as u128) as u64)
        });
        let final_state = if retry { TaskState::Queued } else { state };
        match self
            .core
            .store
            .transition_encoded(TransitionCommand {
                id: started.id,
                expected_state_version: latest.summary.state_version,
                expected_attempt: started.attempt,
                retry_not_before_ms: retry_deadline,
                state: final_state,
                cancel_requested: latest.summary.cancel_requested,
                cancel_error: latest.summary.cancel_error,
                finished_at_ms: Some(now_ms()),
                output,
            })
            .await
        {
            Ok(_updated) => {
                self.core
                    .external_cancellations
                    .lock()
                    .remove(&(started.id, started.attempt));
                #[cfg(feature = "event-bus")]
                self.notify_notifications();
            }
            Err(crate::store::StoreError::Conflict) => {
                self.check_competing_transition(started.id, latest.summary.state_version)
                    .await
            }
            Err(error) => self.latch_fault(error),
        }
    }

    /// Confirms that a compare-and-set conflict corresponds to a newer task
    /// revision; otherwise records the inconsistency as a service fault.
    ///
    /// # Parameters
    ///
    /// - `id`: ID of the task whose transition conflicted.
    /// - `expected_version`: Revision used by the failed compare-and-set.
    async fn check_competing_transition(&self, id: TaskId, expected_version: u64) {
        match self.core.store.get_encoded_task(id).await {
            Ok(Some(task)) if task.summary.state_version != expected_version => {}
            Ok(Some(_)) => self.latch_fault("task lifecycle compare-and-set conflicted without a competing revision"),
            Ok(None) => self.latch_fault("task disappeared after lifecycle compare-and-set conflict"),
            Err(error) => self.latch_fault(error),
        }
    }
}
