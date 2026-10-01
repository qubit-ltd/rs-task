// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::collections::HashMap;
use std::collections::HashSet;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use futures::FutureExt;
use parking_lot::Mutex;
use qubit_codec::ValueBytesCodecRegistry;
use qubit_id::Id;
use qubit_id::IdGenerationError;
use qubit_id::IdGenerator;
use qubit_progress::AsyncReporter;

use crate::engine::EngineError;
use crate::engine::LocalTaskExecutionEngine;
use crate::handler::TaskRunOutcome;
use crate::handler::typed::CancellationMode;
use crate::handler::typed::TypedTaskContext;
use crate::handler::typed::TypedTaskHandlerRegistry;
use crate::model::MAX_TASK_OUTPUT_SUMMARY_BYTES;
use crate::model::OwnerEpoch;
use crate::model::TaskOutput;
use crate::model::TaskState;
use crate::model::next::TaskId;
use crate::model::next::TaskPage;
use crate::model::next::TaskQuery;
use crate::model::next::TaskRequest;
use crate::model::next::TaskSummary;
use crate::model::next::TransitionCommand;
use crate::service::CancelOutcome;
use crate::service::RetryPolicy;
use crate::service::TaskServiceError;
use crate::store::TaskStore;

struct TypedServiceCore {
    store: Arc<dyn TaskStore>,
    codecs: Arc<ValueBytesCodecRegistry>,
    id_generator: Arc<dyn IdGenerator<Id, IdGenerationError>>,
    engine: Arc<LocalTaskExecutionEngine>,
    handlers: TypedTaskHandlerRegistry,
    cancellations: Mutex<HashMap<(TaskId, u32), Arc<AtomicBool>>>,
    owner: OwnerEpoch,
    admission: tokio::sync::RwLock<()>,
    scheduler_changed: tokio::sync::Notify,
    scheduler_stopped: AtomicBool,
    scheduler_stopped_changed: tokio::sync::Notify,
    scheduled_ids: Mutex<HashSet<TaskId>>,
    max_running_tasks: usize,
    scan_page_size: usize,
    max_attempts: u32,
    retry_policy: RetryPolicy,
    fault: Mutex<Option<String>>,
    in_flight: AtomicUsize,
    in_flight_changed: tokio::sync::Notify,
    shutting_down: AtomicBool,
    shutdown_lock: tokio::sync::Mutex<()>,
    owner_released: AtomicBool,
}

pub(super) struct TypedServiceOptions {
    pub(super) max_running_tasks: usize,
    pub(super) scan_page_size: usize,
    pub(super) max_attempts: u32,
    pub(super) retry_policy: RetryPolicy,
}

struct InFlightGuard(Arc<TypedServiceCore>, TaskId);

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.0.scheduled_ids.lock().remove(&self.1);
        self.0.in_flight.fetch_sub(1, Ordering::AcqRel);
        self.0.in_flight_changed.notify_waiters();
        self.0.scheduler_changed.notify_one();
    }
}

/// Typed task submit, dispatch, execution, and cancellation facade.
#[derive(Clone)]
pub struct TypedTaskExecutionService {
    core: Arc<TypedServiceCore>,
}

impl TypedTaskExecutionService {
    pub(super) async fn new(
        store: Arc<dyn TaskStore>,
        codecs: Arc<ValueBytesCodecRegistry>,
        id_generator: Arc<dyn IdGenerator<Id, IdGenerationError>>,
        engine: Arc<LocalTaskExecutionEngine>,
        handlers: TypedTaskHandlerRegistry,
        options: TypedServiceOptions,
    ) -> Result<Self, TaskServiceError> {
        let owner = store.acquire_owner().await?;
        let service = Self {
            core: Arc::new(TypedServiceCore {
                store,
                codecs,
                id_generator,
                engine,
                handlers,
                cancellations: Mutex::new(HashMap::new()),
                owner,
                admission: tokio::sync::RwLock::new(()),
                scheduler_changed: tokio::sync::Notify::new(),
                scheduler_stopped: AtomicBool::new(false),
                scheduler_stopped_changed: tokio::sync::Notify::new(),
                scheduled_ids: Mutex::new(HashSet::new()),
                max_running_tasks: options.max_running_tasks,
                scan_page_size: options.scan_page_size,
                max_attempts: options.max_attempts,
                retry_policy: options.retry_policy,
                fault: Mutex::new(None),
                in_flight: AtomicUsize::new(0),
                in_flight_changed: tokio::sync::Notify::new(),
                shutting_down: AtomicBool::new(false),
                shutdown_lock: tokio::sync::Mutex::new(()),
                owner_released: AtomicBool::new(false),
            }),
        };
        if let Err(error) = service.recover_unfinished().await {
            let release = service.core.store.release_owner(owner).await;
            if let Err(release_error) = release {
                return Err(TaskServiceError::Store(release_error));
            }
            return Err(error);
        }
        let scheduler = service.clone();
        let scheduler_core = Arc::clone(&service.core);
        tokio::spawn(async move {
            if AssertUnwindSafe(scheduler.scheduler_loop())
                .catch_unwind()
                .await
                .is_err()
            {
                scheduler.latch_fault("typed task scheduler panicked");
            }
            scheduler_core.scheduler_stopped.store(true, Ordering::Release);
            scheduler_core.scheduler_stopped_changed.notify_waiters();
        });
        Ok(service)
    }

    /// Encodes, assigns an ID, durably accepts, and dispatches a typed request.
    pub async fn submit<T: Send + Sync + 'static>(
        &self,
        request: TaskRequest<T>,
    ) -> Result<TaskSummary, TaskServiceError> {
        let _admission = self.core.admission.read().await;
        self.check_fault()?;
        if self.core.shutting_down.load(Ordering::Acquire) {
            return Err(TaskServiceError::ShuttingDown);
        }
        let stored = request
            .encode(&self.core.codecs)
            .map_err(|error| TaskServiceError::TypedRequest(error.to_string()))?;
        let id = TaskId::from_id(self.core.id_generator.generate()?);
        let accepted = self.store_result(self.core.store.accept_encoded(id, stored).await)?;
        if accepted.created {
            self.core.scheduler_changed.notify_one();
        }
        Ok(accepted.summary)
    }

    /// Returns a typed task's current persisted summary.
    pub async fn get(&self, id: TaskId) -> Result<Option<TaskSummary>, TaskServiceError> {
        Ok(self.core.store.get_encoded_task(id).await?.map(|task| task.summary))
    }

    /// Queries typed task summaries using category filters and numeric cursors.
    pub async fn query(&self, query: TaskQuery) -> Result<TaskPage, TaskServiceError> {
        Ok(self.core.store.list_encoded(query).await?)
    }

    /// Requeues a blocked task after its configuration or handler is repaired.
    pub async fn resume_blocked(
        &self,
        id: TaskId,
        expected_state_version: u64,
    ) -> Result<TaskSummary, TaskServiceError> {
        let _admission = self.core.admission.read().await;
        self.check_fault()?;
        if self.core.shutting_down.load(Ordering::Acquire) || self.core.owner_released.load(Ordering::Acquire) {
            return Err(TaskServiceError::ShuttingDown);
        }
        let task = self
            .store_result(self.core.store.get_encoded_task(id).await)?
            .ok_or(crate::store::StoreError::NotFound)?;
        if task.summary.state_version != expected_state_version {
            return Err(crate::store::StoreError::Conflict.into());
        }
        if !matches!(task.summary.state, TaskState::Blocked { .. }) {
            return Err(TaskServiceError::NotBlocked {
                actual: task.summary.state.kind(),
            });
        }
        if task.summary.cancel_requested {
            return Err(TaskServiceError::CancellationPending);
        }
        let resumed = self.store_result(
            self.core
                .store
                .transition_encoded(TransitionCommand {
                    id,
                    expected_state_version,
                    expected_attempt: task.summary.attempt,
                    state: TaskState::Queued,
                    cancel_requested: false,
                    cancel_error: None,
                    retry_not_before_ms: None,
                    finished_at_ms: None,
                    output: None,
                })
                .await,
        )?;
        self.core.scheduler_changed.notify_one();
        Ok(resumed)
    }

    /// Stops submissions and dispatch, waits for active attempts, then releases
    /// exclusive store ownership. Queued tasks remain recoverable for the next
    /// service owner.
    pub async fn shutdown(&self) -> Result<(), TaskServiceError> {
        let _shutdown = self.core.shutdown_lock.lock().await;
        if self.core.owner_released.load(Ordering::Acquire) {
            return self.check_fault();
        }
        {
            let _admission = self.core.admission.write().await;
            self.core.shutting_down.store(true, Ordering::Release);
            self.core.scheduler_changed.notify_waiters();
        }
        loop {
            let notified = self.core.scheduler_stopped_changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.core.scheduler_stopped.load(Ordering::Acquire) {
                break;
            }
            notified.await;
        }
        loop {
            let notified = self.core.in_flight_changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.core.in_flight.load(Ordering::Acquire) == 0 {
                break;
            }
            notified.await;
        }
        if let Err(error) = self.core.store.release_owner(self.core.owner).await {
            self.latch_fault(&error);
            return Err(TaskServiceError::StoreUnavailable(error.to_string()));
        }
        self.core.owner_released.store(true, Ordering::Release);
        if let Some(fault) = self.core.fault.lock().clone() {
            Err(TaskServiceError::StoreUnavailable(fault))
        } else {
            Ok(())
        }
    }

    /// Requests cancellation, persisting intent before signalling a handler.
    pub async fn cancel(&self, id: TaskId) -> Result<CancelOutcome, TaskServiceError> {
        let _admission = self.core.admission.read().await;
        self.check_fault()?;
        if self.core.shutting_down.load(Ordering::Acquire) || self.core.owner_released.load(Ordering::Acquire) {
            return Err(TaskServiceError::ShuttingDown);
        }
        let Some(task) = self.store_result(self.core.store.get_encoded_task(id).await)? else {
            return Err(TaskServiceError::Store(crate::store::StoreError::NotFound));
        };
        let summary = task.summary;
        if summary.state.is_terminal() {
            return Ok(CancelOutcome::AlreadyTerminal);
        }
        if matches!(summary.state, TaskState::Queued | TaskState::Blocked { .. }) {
            let updated = self.store_result(
                self.core
                    .store
                    .transition_encoded(TransitionCommand {
                        id,
                        expected_state_version: summary.state_version,
                        expected_attempt: summary.attempt,
                        retry_not_before_ms: None,
                        state: TaskState::Cancelled,
                        cancel_requested: false,
                        cancel_error: None,
                        finished_at_ms: Some(now_ms()),
                        output: None,
                    })
                    .await,
            )?;
            let _ = updated;
            return Ok(CancelOutcome::CancelledBeforeStart);
        }
        if summary.cancel_requested {
            if summary.cancel_error.is_some() {
                return Err(TaskServiceError::TypedRequest(
                    "external cancellation hook previously failed".into(),
                ));
            }
            return Ok(CancelOutcome::CancellationRequested);
        }
        let descriptor = self
            .core
            .handlers
            .descriptor(&summary.kind_id)
            .ok_or(TaskServiceError::CancellationUnsupported)?;
        if descriptor.cancellation_mode == CancellationMode::Unsupported {
            return Ok(CancelOutcome::CancellationUnsupported);
        }
        self.store_result(
            self.core
                .store
                .transition_encoded(TransitionCommand {
                    id,
                    expected_state_version: summary.state_version,
                    expected_attempt: summary.attempt,
                    retry_not_before_ms: None,
                    state: TaskState::Running,
                    cancel_requested: true,
                    cancel_error: None,
                    finished_at_ms: None,
                    output: None,
                })
                .await,
        )?;
        match descriptor.cancellation_mode {
            CancellationMode::Unsupported => Ok(CancelOutcome::CancellationUnsupported),
            CancellationMode::Cooperative => {
                if let Some(signal) = self.core.cancellations.lock().get(&(id, summary.attempt)) {
                    signal.store(true, Ordering::Release);
                }
                Ok(CancelOutcome::CancellationRequested)
            }
            CancellationMode::ExternalHook => {
                let hook = self
                    .core
                    .handlers
                    .cancel_externally(&summary.kind_id, id, summary.attempt)
                    .ok_or(TaskServiceError::CancellationUnsupported)?;
                if let Err(error) = hook.await {
                    let diagnostic = error.message;
                    let latest = self
                        .store_result(self.core.store.get_encoded_task(id).await)?
                        .ok_or(crate::store::StoreError::NotFound)?;
                    self.store_result(
                        self.core
                            .store
                            .transition_encoded(TransitionCommand {
                                id,
                                expected_state_version: latest.summary.state_version,
                                expected_attempt: latest.summary.attempt,
                                retry_not_before_ms: None,
                                state: latest.summary.state,
                                cancel_requested: true,
                                cancel_error: Some(diagnostic),
                                finished_at_ms: None,
                                output: None,
                            })
                            .await,
                    )?;
                    return Err(TaskServiceError::TypedRequest(
                        "external cancellation hook failed".into(),
                    ));
                }
                Ok(CancelOutcome::CancellationRequested)
            }
        }
    }

    fn latch_fault(&self, error: impl std::fmt::Display) {
        let mut fault = self.core.fault.lock();
        if fault.is_none() {
            *fault = Some(error.to_string());
        }
        self.core.scheduler_changed.notify_waiters();
        self.core.in_flight_changed.notify_waiters();
    }

    fn check_fault(&self) -> Result<(), TaskServiceError> {
        self.core
            .fault
            .lock()
            .clone()
            .map_or(Ok(()), |fault| Err(TaskServiceError::StoreUnavailable(fault)))
    }

    fn store_result<T>(&self, result: Result<T, crate::store::StoreError>) -> Result<T, TaskServiceError> {
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

    async fn recover_unfinished(&self) -> Result<(), TaskServiceError> {
        let mut after = None;
        loop {
            let page = self
                .core
                .store
                .list_encoded(crate::model::next::TaskQuery {
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
                    let recovered = self
                        .core
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
                        .await?;
                    let _ = recovered;
                }
            }
            after = page.next;
            if after.is_none() {
                self.core.scheduler_changed.notify_one();
                return Ok(());
            }
        }
    }

    async fn scheduler_loop(&self) {
        loop {
            if self.core.shutting_down.load(Ordering::Acquire) || self.core.fault.lock().is_some() {
                return;
            }
            let notified = self.core.scheduler_changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let mut launched = false;
            let mut retry_at = None;
            while self.core.in_flight.load(Ordering::Acquire) < self.core.max_running_tasks
                && !self.core.shutting_down.load(Ordering::Acquire)
                && self.core.fault.lock().is_none()
            {
                let (candidate, deadline) = match self.next_queued_candidate().await {
                    Ok(candidate) => candidate,
                    Err(error) => {
                        self.latch_fault(error);
                        return;
                    }
                };
                retry_at = deadline;
                let Some(id) = candidate else { break };
                if !self.core.scheduled_ids.lock().insert(id) {
                    break;
                }
                self.core.in_flight.fetch_add(1, Ordering::AcqRel);
                let service = self.clone();
                let core = Arc::clone(&self.core);
                tokio::spawn(async move {
                    let _in_flight = InFlightGuard(core, id);
                    if AssertUnwindSafe(service.run_one(id)).catch_unwind().await.is_err() {
                        service.latch_fault(format!(
                            "task execution supervisor panicked for {}",
                            id.to_padded_decimal()
                        ));
                    }
                });
                launched = true;
            }
            if launched {
                continue;
            }
            if let Some(retry_at) = retry_at {
                let wait = std::time::Duration::from_millis(retry_at.saturating_sub(now_ms()));
                let _ = tokio::time::timeout(wait, notified).await;
            } else {
                notified.await;
            }
        }
    }

    async fn next_queued_candidate(&self) -> Result<(Option<TaskId>, Option<u64>), crate::store::StoreError> {
        let mut after = None;
        let mut retry_at: Option<u64> = None;
        loop {
            let page = self
                .core
                .store
                .list_encoded(TaskQuery {
                    states: vec![crate::model::TaskStateKind::Queued],
                    after,
                    limit: self.core.scan_page_size,
                    ..TaskQuery::default()
                })
                .await?;
            for summary in &page.records {
                if self.core.scheduled_ids.lock().contains(&summary.id) {
                    continue;
                }
                if let Some(deadline) = summary.retry_not_before_ms.filter(|deadline| *deadline > now_ms()) {
                    retry_at = Some(retry_at.map_or(deadline, |current| current.min(deadline)));
                } else {
                    return Ok((Some(summary.id), retry_at));
                }
            }
            let Some(cursor) = page.next else {
                return Ok((None, retry_at));
            };
            after = Some(cursor);
        }
    }

    async fn run_one(&self, id: TaskId) {
        'retry: loop {
            if self.core.shutting_down.load(Ordering::Acquire) {
                return;
            }
            let task = match self.core.store.get_encoded_task(id).await {
                Ok(Some(task)) => task,
                Ok(None) => return,
                Err(error) => {
                    self.latch_fault(error);
                    return;
                }
            };
            let summary = task.summary;
            if !matches!(summary.state, TaskState::Queued) {
                return;
            }
            let Some(descriptor) = self.core.handlers.descriptor(&summary.kind_id) else {
                self.mark_blocked(&summary, "handler is not registered").await;
                return;
            };
            if summary.payload_type_id != descriptor.payload_type_id.as_str()
                || !descriptor
                    .accepted_schema_versions
                    .contains(&summary.payload_schema_version)
            {
                self.mark_blocked(
                    &summary,
                    "handler does not accept the payload identity or schema version",
                )
                .await;
                return;
            }
            if self.core.codecs.get(&summary.payload_codec_id).is_none() {
                self.mark_blocked(&summary, "payload bytes codec is not registered")
                    .await;
                return;
            }
            let reservation = match self
                .core
                .engine
                .try_prepare_typed(id, task.request.resource_limit.clone())
            {
                Ok(reservation) => reservation,
                Err(EngineError::Unsatisfiable) => {
                    self.mark_blocked(&summary, "requested resources exceed configured capacity")
                        .await;
                    return;
                }
                Err(EngineError::TemporarilyUnavailable) => {
                    let notified = self.core.in_flight_changed.notified();
                    tokio::pin!(notified);
                    notified.as_mut().enable();
                    notified.await;
                    continue 'retry;
                }
                Err(error) => {
                    self.mark_blocked(&summary, &error.to_string()).await;
                    return;
                }
            };
            let admission = self.core.admission.read().await;
            if self.core.shutting_down.load(Ordering::Acquire) {
                drop(reservation);
                return;
            }
            let running = match self
                .core
                .store
                .start_encoded(crate::model::next::StartCommand {
                    id,
                    expected_state_version: summary.state_version,
                    started_at_ms: now_ms(),
                })
                .await
            {
                Ok(summary) => summary,
                Err(crate::store::StoreError::Conflict) => {
                    self.check_competing_transition(id, summary.state_version).await;
                    return;
                }
                Err(error) => {
                    self.latch_fault(error);
                    return;
                }
            };
            drop(admission);
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
            let reporter: Arc<dyn AsyncReporter> = Arc::new(super::task_progress_reporter::TaskProgressReporter::new(
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
                                    "task output summary exceeds the {MAX_TASK_OUTPUT_SUMMARY_BYTES}-byte limit"
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
            return;
        }
    }

    async fn mark_blocked(&self, summary: &TaskSummary, reason: &str) {
        if let Err(error) = self
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
            if matches!(error, crate::store::StoreError::Conflict) {
                self.check_competing_transition(summary.id, summary.state_version).await;
            } else {
                self.latch_fault(error);
            }
        }
    }

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
            return;
        }
        let retry = retryable && !latest.summary.cancel_requested && started.attempt < self.core.max_attempts;
        let retry_deadline = retry.then(|| {
            let delay = self.core.retry_policy.delay_for_attempt(started.attempt);
            now_ms().saturating_add(delay.as_millis().min(u64::MAX as u128) as u64)
        });
        let final_state = if retry { TaskState::Queued } else { state };
        if let Err(error) = self
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
            if matches!(error, crate::store::StoreError::Conflict) {
                self.check_competing_transition(started.id, latest.summary.state_version)
                    .await;
            } else {
                self.latch_fault(error);
            }
        }
    }

    async fn check_competing_transition(&self, id: TaskId, expected_version: u64) {
        match self.core.store.get_encoded_task(id).await {
            Ok(Some(task)) if task.summary.state_version != expected_version => {}
            Ok(Some(_)) => self.latch_fault("task lifecycle compare-and-set conflicted without a competing revision"),
            Ok(None) => self.latch_fault("task disappeared after lifecycle compare-and-set conflict"),
            Err(error) => self.latch_fault(error),
        }
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn prepare_handler(
    handlers: &TypedTaskHandlerRegistry,
    codecs: &ValueBytesCodecRegistry,
    kind_id: &str,
    payload: crate::model::next::StoredPayload,
) -> Result<crate::handler::typed::PreparedTask, String> {
    handlers
        .prepare(kind_id, payload, codecs)
        .map_err(|error| error.to_string())
}
