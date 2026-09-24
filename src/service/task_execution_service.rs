// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use parking_lot::Mutex;
use tokio::sync::Notify;
use tokio::sync::oneshot;

use super::admission_gate::AdmissionGate;
use super::local_task_handle::LocalTaskHandle;
use super::local_task_outcome::LocalTaskOutcome;
use super::local_task_outcome::adapt_local_outcome;
use super::local_task_result_error::LocalTaskResultError;
#[cfg(feature = "event-bus")]
use super::task_event_notification_stats::TaskEventNotificationStats;
#[cfg(feature = "event-bus")]
use super::task_event_publisher::TaskEventPublisher;
use super::task_execution_service_builder::TaskExecutionServiceBuilder;
use crate::engine::EngineError;
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
    /// The request exceeds available configured capacity.
    #[error("task request cannot be satisfied by configured resources")]
    Unsatisfiable,
    /// The request contains invalid metadata or an oversized payload.
    #[error("invalid task request: {0}")]
    InvalidRequest(String),
    /// The requested task is blocked pending intervention.
    #[error("task is blocked and requires intervention")]
    Blocked,
    /// New task submissions have been stopped.
    #[error("task execution service is shutting down")]
    ShuttingDown,
    /// A persistence failure suspended task acceptance and scheduling.
    #[error("task execution service is paused after a task store failure: {0}")]
    StoreUnavailable(String),
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
    pub(crate) handlers: TaskHandlerRegistry,
    pub(crate) queue_capacity: usize,
    pub(crate) scan_budget: usize,
    pub(crate) max_attempts: u32,
    pub(crate) queue: Mutex<VecDeque<QueuedTask>>,
    pub(crate) queue_count: AtomicUsize,
    pub(crate) local_handlers: Mutex<HashMap<TaskId, Arc<dyn TaskHandler>>>,
    pub(crate) local_finalizations: Mutex<HashMap<TaskId, oneshot::Sender<Result<TaskState, LocalTaskResultError>>>>,
    pub(crate) cancellations: Mutex<HashMap<TaskId, RunningCancellation>>,
    pub(crate) changed: Notify,
    pub(super) admission: AdmissionGate,
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
    pub async fn submit(&self, request: TaskRequest) -> Result<TaskRecord, TaskServiceError> {
        let service = self.clone();
        await_admission(runtime().spawn(async move { service.submit_admitted(request).await })).await
    }

    /// Runs an admitted request to completion even if its caller is cancelled.
    async fn submit_admitted(&self, request: TaskRequest) -> Result<TaskRecord, TaskServiceError> {
        if let Some(error) = self.last_store_error() {
            return Err(TaskServiceError::StoreUnavailable(error));
        }
        let _permit = self.core.admission.enter()?;
        let capacity = self.core.engine.capacity().capacity;
        validate_request(&request, &capacity)?;
        if let Some(record) = self
            .core
            .store
            .find_idempotent(request.clone())
            .await
            .map_err(|error| self.handle_store_error(error))?
        {
            return Ok(record);
        }
        if self.core.queue_count.load(Ordering::Acquire) >= self.core.queue_capacity {
            if let Some(record) = self
                .core
                .store
                .find_idempotent(request.clone())
                .await
                .map_err(|error| self.handle_store_error(error))?
            {
                return Ok(record);
            }
            return Err(TaskServiceError::QueueFull);
        }
        self.reserve_queue_slot()?;
        let outcome = match self.core.store.accept(TaskId::generate(), request.clone()).await {
            Ok(outcome) => outcome,
            Err(error) => {
                self.release_queue_slot();
                return Err(self.handle_store_error(error));
            }
        };
        match outcome {
            AcceptOutcome::Accepted(record) => {
                self.core.queue.lock().push_back(QueuedTask {
                    id: record.id,
                    request,
                    bypasses: 0,
                });
                self.core.changed.notify_one();
                publish_record(&self.core, &record);
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
        let service = self.clone();
        await_admission(runtime().spawn(async move { service.submit_local_admitted(task).await })).await
    }

    /// Retains a local handler through acceptance and queue publication.
    async fn submit_local_admitted<F, R, E>(&self, task: F) -> Result<LocalTaskHandle<R, E>, TaskServiceError>
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
        self.reserve_queue_slot()?;
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
        {
            let fault = self.core.store_fault.lock();
            if let Some(error) = fault.as_ref() {
                self.release_queue_slot();
                return Err(TaskServiceError::StoreUnavailable(error.clone()));
            }
            self.core.local_finalizations.lock().insert(id, final_sender);
        }
        match self.core.store.accept(id, request.clone()).await {
            Ok(AcceptOutcome::Accepted(record)) => {
                let finalizations = self.core.local_finalizations.lock();
                if !finalizations.contains_key(&id) {
                    self.release_queue_slot();
                    return Ok(LocalTaskHandle::new(record.id, typed_receiver, final_receiver));
                }
                self.core.local_handlers.lock().insert(id, handler);
                self.core.queue.lock().push_back(QueuedTask {
                    id,
                    request,
                    bypasses: 0,
                });
                drop(finalizations);
                self.core.changed.notify_one();
                publish_record(&self.core, &record);
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

    /// Latches operational store failures while preserving ordinary conflicts
    /// and not-found results for the calling operation.
    fn handle_store_error(&self, error: StoreError) -> TaskServiceError {
        if matches!(error, StoreError::Failure(_)) {
            record_store_fault(&self.core, error.to_string());
        }
        error.into()
    }

    fn reserve_queue_slot(&self) -> Result<(), TaskServiceError> {
        self.core
            .queue_count
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                (count < self.core.queue_capacity).then_some(count + 1)
            })
            .map(|_| ())
            .map_err(|_| TaskServiceError::QueueFull)
    }

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
        self.core.store.get(id).await.map_err(|error| self.handle_store_error(error))
    }

    /// Returns a bounded page of retained history.
    pub async fn list(&self, query: TaskQuery) -> Result<TaskPage, TaskServiceError> {
        self.core.store.list(query).await.map_err(|error| self.handle_store_error(error))
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
                        let mut queue = self.core.queue.lock();
                        let previous_len = queue.len();
                        queue.retain(|task| task.id != id);
                        if queue.len() < previous_len {
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
                    publish_record(&self.core, &updated);
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
        await_admission(runtime().spawn(async move { service.retry_blocked_admitted(id).await })).await
    }

    /// Retains the permit through retry persistence and queue publication.
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
        self.reserve_queue_slot()?;
        let updated = match transition(&self.core, &record, TaskState::Queued, None, Vec::new(), false).await {
            Ok(updated) => updated,
            Err(error) => {
                self.core.queue_count.fetch_sub(1, Ordering::AcqRel);
                return Err(self.handle_store_error(error));
            }
        };
        self.core.queue.lock().push_back(QueuedTask {
            id,
            request: updated.request.clone(),
            bypasses: 0,
        });
        self.core.changed.notify_one();
        publish_record(&self.core, &updated);
        Ok(updated)
    }

    /// Resolves when the task becomes terminal; returns an error if it becomes
    /// blocked.
    pub async fn wait(&self, id: TaskId) -> Result<TaskRecord, TaskServiceError> {
        loop {
            let notified = self.core.changed.notified();
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
        if self.core.admission.close() {
            self.core.changed.notify_waiters();
            let service = self.clone();
            runtime().spawn(async move {
                let result = service.coordinate_shutdown().await;
                service
                    .core
                    .admission
                    .finish_close(result.map_err(|error| error.to_string()));
                service.core.changed.notify_waiters();
            });
        }
        self.core.admission.wait_closed().await
    }

    /// Drains accepted work and releases ownership after the admission gate is
    /// idle.
    async fn coordinate_shutdown(&self) -> Result<(), TaskServiceError> {
        self.core.admission.wait_idle().await;
        loop {
            let notified = self.core.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(error) = self.last_store_error() {
                return Err(TaskServiceError::StoreUnavailable(error));
            }
            let stats = self.stats().await?;
            if stats.queued == 0 && stats.running == 0 {
                break;
            }
            notified.await;
        }
        if let Some(epoch) = self.core.owner
            && let Err(error) = self.core.store.release_owner(epoch).await
        {
            record_store_fault(&self.core, error.to_string());
            return Err(error.into());
        }
        #[cfg(feature = "event-bus")]
        if let Some(publisher) = &self.core.event_bus {
            publisher.close().await;
        }
        Ok(())
    }

    pub(crate) fn start(core: ServiceCore) -> Self {
        let service = Self { core: Arc::new(core) };
        let weak = Arc::downgrade(&service.core);
        runtime().spawn(scheduler_loop(weak));
        service
    }
}

async fn scheduler_loop(core_ref: std::sync::Weak<ServiceCore>) {
    loop {
        let Some(core) = core_ref.upgrade() else {
            return;
        };
        if core.store_fault.lock().is_some() {
            return;
        }
        let mut queue = core.queue.lock().drain(..).collect::<Vec<_>>();
        if queue.is_empty() {
            if core.admission.is_closed() {
                return;
            }
            drop(core);
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            continue;
        }
        let order = core.policy.order(
            &QueueSnapshot {
                tasks: queue.clone(),
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
            let task = queue.remove(index);
            let record = match core.store.get(id).await {
                Ok(Some(record)) => record,
                Ok(None) => {
                    release_core_queue_slot(&core);
                    core.local_handlers.lock().remove(&id);
                    continue;
                }
                Err(error) => {
                    pause_on_store_fault(&core, error);
                    return;
                }
            };
            if core.store_fault.lock().is_some() {
                return;
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
                    .resolve(&task.request.task_type, &task.request.handler_version)
            });
            let Some(handler) = handler else {
                release_core_queue_slot(&core);
                match mark_blocked(
                    &core,
                    &record,
                    format!(
                        "missing handler {}@{}",
                        task.request.task_type, task.request.handler_version
                    ),
                )
                .await
                {
                    Ok(()) | Err(StoreError::Conflict | StoreError::NotFound) => {}
                    Err(error) => {
                        pause_on_store_fault(&core, error);
                        return;
                    }
                }
                continue;
            };
            let prepared = match core.engine.prepare(id, task.request.resources.clone()).await {
                Ok(value) => value,
                Err(EngineError::TemporarilyUnavailable) => {
                    queue.push(task);
                    continue;
                }
                Err(EngineError::Unsatisfiable) => {
                    release_core_queue_slot(&core);
                    match mark_blocked(&core, &record, "resource request is unsatisfiable".into()).await {
                        Ok(()) | Err(StoreError::Conflict | StoreError::NotFound) => {}
                        Err(error) => {
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
                    release_core_queue_slot(&core);
                    pause_on_store_fault(&core, error);
                    return;
                }
            };
            release_core_queue_slot(&core);
            publish_record(&core, &running);
            if core.store_fault.lock().is_some() {
                return;
            }
            core.local_handlers.lock().remove(&id);
            let cancelled = Arc::new(AtomicBool::new(false));
            let context = TaskContext::new(id, running.attempt, assigned, cancelled);
            match core
                .engine
                .activate(prepared, handler, task.request.payload.clone(), context)
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
                    runtime().spawn(finish_attempt(weak, running, handle.receiver));
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
        for item in &mut queue {
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
            let mut retained = core.queue.lock();
            for item in queue.into_iter().rev() {
                retained.push_front(item);
            }
        }
        if !started {
            drop(core);
            tokio::time::sleep(std::time::Duration::from_millis(40)).await;
        } else {
            core.changed.notify_waiters();
        }
    }
}

async fn finish_attempt(
    core_ref: std::sync::Weak<ServiceCore>,
    running: TaskRecord,
    receiver: tokio::sync::oneshot::Receiver<crate::handler::TaskRunResult>,
) {
    let result = receiver.await.unwrap_or_else(|_| {
        Err(crate::model::TaskRunError {
            category: "engine".into(),
            message: "execution worker stopped".into(),
            retryable: true,
        })
    });
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
    let state = match &result {
        Ok(TaskRunOutcome::Succeeded(_)) => TaskState::Succeeded,
        Ok(TaskRunOutcome::Cancelled) => TaskState::Cancelled,
        Err(error) if error.category == "panic" => TaskState::Panicked {
            message: error.message.clone(),
        },
        Err(error) if error.retryable && running.attempt < core.max_attempts => TaskState::Queued,
        Err(error) if error.retryable => TaskState::Blocked {
            reason: format!("retry limit reached: {}", error.message),
        },
        Err(error) => TaskState::Failed {
            category: error.category.clone(),
            message: error.message.clone(),
        },
    };
    let output = match result {
        Ok(TaskRunOutcome::Succeeded(output)) => Some(output),
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
    let mut retry_slot_reserved = false;
    if matches!(final_state, TaskState::Queued) {
        retry_slot_reserved = try_reserve_core_queue_slot(&core);
        if !retry_slot_reserved {
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
        match transition(
            &core,
            &latest,
            final_state.clone(),
            output.clone(),
            latest.assigned_resources.clone(),
            latest.cancel_requested,
        )
        .await
        {
            Ok(updated) => {
                if matches!(final_state, TaskState::Queued) {
                    core.queue.lock().push_back(QueuedTask {
                        id: updated.id,
                        request: updated.request.clone(),
                        bypasses: 0,
                    });
                }
                core.changed.notify_waiters();
                publish_record(&core, &updated);
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

async fn mark_blocked(core: &ServiceCore, record: &TaskRecord, reason: String) -> Result<(), StoreError> {
    let updated = transition(
        core,
        record,
        TaskState::Blocked { reason },
        None,
        Vec::new(),
        record.cancel_requested,
    )
    .await?;
    core.changed.notify_waiters();
    publish_record(core, &updated);
    finalize_local(core, updated.id, Ok(updated.state));
    Ok(())
}

fn finalize_local(core: &ServiceCore, id: TaskId, result: Result<TaskState, LocalTaskResultError>) {
    let sender = core.local_finalizations.lock().remove(&id);
    if let Some(sender) = sender {
        let _ = sender.send(result);
    }
}

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
        runtime().spawn(async move {
            core.admission.wait_idle().await;
            core.admission.finish_close(Err(diagnostic));
            core.changed.notify_waiters();
        });
    }
    core.changed.notify_waiters();
}

/// Preserves an admission worker after caller cancellation and reports a
/// worker failure as an explicit service error.
async fn await_admission<T>(
    handle: tokio::task::JoinHandle<Result<T, TaskServiceError>>,
) -> Result<T, TaskServiceError> {
    handle
        .await
        .map_err(|error| TaskServiceError::StoreUnavailable(format!("task admission worker stopped: {error}")))?
}

fn publish_record(core: &ServiceCore, record: &TaskRecord) {
    #[cfg(feature = "event-bus")]
    if let Some(bus) = &core.event_bus {
        bus.enqueue(crate::event::TaskEvent::from(record));
    }
    #[cfg(not(feature = "event-bus"))]
    let _ = (core, record);
}

async fn transition(
    core: &ServiceCore,
    record: &TaskRecord,
    state: TaskState,
    output: Option<crate::model::TaskOutput>,
    assigned_resources: Vec<String>,
    cancel_requested: bool,
) -> Result<TaskRecord, StoreError> {
    core.store
        .transition(TransitionCommand {
            id: record.id,
            expected_version: record.state_version,
            expected_attempt: record.attempt,
            state,
            output,
            assigned_resources,
            cancel_requested,
        })
        .await
}

fn validate_request(request: &TaskRequest, capacity: &ResourceCapacity) -> Result<(), TaskServiceError> {
    if request.task_type.is_empty() || request.handler_version.is_empty() {
        return Err(TaskServiceError::InvalidRequest(
            "task type and handler version must not be empty".into(),
        ));
    }
    if request.payload.len() > crate::model::MAX_TASK_PAYLOAD_BYTES {
        return Err(TaskServiceError::InvalidRequest(
            "payload exceeds the 16 MiB limit".into(),
        ));
    }
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

fn release_core_queue_slot(core: &ServiceCore) {
    let _ = core
        .queue_count
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
            Some(count.saturating_sub(1))
        });
}

fn try_reserve_core_queue_slot(core: &ServiceCore) -> bool {
    core.queue_count
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
            (count < core.queue_capacity).then_some(count + 1)
        })
        .is_ok()
}

pub(super) fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("task service runtime must be created")
    })
}
