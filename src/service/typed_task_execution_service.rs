// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::Weak;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use futures::FutureExt;
use parking_lot::Mutex;
use qubit_codec::ValueBytesCodecRegistry;
#[cfg(feature = "event-bus")]
use qubit_event_bus::spi::DurabilityCapability;
#[cfg(feature = "event-bus")]
use qubit_event_bus::spi::PublishGuarantee;
use qubit_id::Id;
use qubit_id::IdGenerationError;
use qubit_id::IdGenerator;
use qubit_model_id::HasModelId;
use qubit_progress::AsyncReporter;
use tokio::sync::watch;

#[cfg(feature = "event-bus")]
use super::NotificationStats;
use super::owner_release_guard::OwnerReleaseGuard;
use super::owner_release_guard::OwnerReleaseWorker;
#[cfg(feature = "event-bus")]
use super::task_event_publisher::TaskEventPublisher;
use crate::engine::EngineError;
use crate::engine::LocalTaskExecutionEngine;
use crate::engine::TypedResourceReservation;
use crate::handler::TaskRunOutcome;
use crate::handler::typed::CancellationMode;
use crate::handler::typed::TypedTaskContext;
use crate::handler::typed::TypedTaskHandlerRegistry;
use crate::model::MAX_TASK_OUTPUT_SUMMARY_BYTES;
use crate::model::TaskOutput;
use crate::model::TaskState;
use crate::model::typed::TaskId;
use crate::model::typed::TaskPage;
use crate::model::typed::TaskQuery;
use crate::model::typed::TaskRequest;
use crate::model::typed::TaskSummary;
use crate::model::typed::TransitionCommand;
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
    external_cancellations: Mutex<HashMap<(TaskId, u32), ExternalCancelEntry>>,
    owner_guard: tokio::sync::Mutex<Option<OwnerReleaseGuard>>,
    admission: tokio::sync::RwLock<()>,
    scheduler_changed: tokio::sync::Notify,
    scheduler_stopped: AtomicBool,
    scheduler_stopped_changed: tokio::sync::Notify,
    max_running_tasks: usize,
    scan_page_size: usize,
    max_resource_bypasses: usize,
    max_attempts: u32,
    retry_policy: RetryPolicy,
    fault: Mutex<Option<String>>,
    in_flight: AtomicUsize,
    in_flight_changed: tokio::sync::Notify,
    shutting_down: AtomicBool,
    shutdown_changed: tokio::sync::Notify,
    shutdown_completed: tokio::sync::Notify,
    shutdown_progress: Mutex<ShutdownProgress>,
    public_gone: AtomicBool,
    owner_released: AtomicBool,
    #[cfg(feature = "event-bus")]
    publisher: Option<TaskEventPublisher>,
    #[cfg(feature = "event-bus")]
    notification_close_error: Mutex<Option<String>>,
}

enum ExternalCancelEntry {
    InFlight(Arc<ExternalCancelFlight>),
    Succeeded,
}

struct ExternalCancelFlight {
    result: watch::Sender<Option<ExternalCancelResult>>,
}

#[derive(Clone)]
enum ExternalCancelResult {
    Success,
    AlreadyTerminal,
    HookFailed(String),
    ServiceFailed(String),
}

impl TypedServiceCore {
    /// Signals the supervisor without blocking the dropping handle or caller.
    fn request_shutdown(&self) {
        self.shutting_down.store(true, Ordering::Release);
        self.scheduler_changed.notify_waiters();
        self.shutdown_changed.notify_waiters();
    }
}

struct PublicLifetime {
    core: Weak<TypedServiceCore>,
}

impl Drop for PublicLifetime {
    fn drop(&mut self) {
        if let Some(core) = self.core.upgrade() {
            core.public_gone.store(true, Ordering::Release);
            core.request_shutdown();
        }
    }
}

#[derive(Clone)]
struct ServiceRunner {
    core: Arc<TypedServiceCore>,
}

#[derive(Clone)]
enum ShutdownOutcome {
    Complete,
    StoreUnavailable(String),
    #[cfg(feature = "event-bus")]
    NotificationClose(String),
}

#[derive(Default)]
struct ShutdownProgress {
    attempt: usize,
    results: Vec<ShutdownOutcome>,
    retryable_release: bool,
    retry_requested: bool,
}

impl ShutdownOutcome {
    /// Reconstructs an owned public error for each independent shutdown waiter.
    fn into_result(self) -> Result<(), TaskServiceError> {
        match self {
            Self::Complete => Ok(()),
            Self::StoreUnavailable(error) => Err(TaskServiceError::StoreUnavailable(error)),
            #[cfg(feature = "event-bus")]
            Self::NotificationClose(error) => Err(TaskServiceError::NotificationClose(error)),
        }
    }
}

pub(super) struct TypedServiceOptions {
    pub(super) max_running_tasks: usize,
    pub(super) scan_page_size: usize,
    pub(super) max_resource_bypasses: usize,
    pub(super) max_attempts: u32,
    pub(super) retry_policy: RetryPolicy,
    #[cfg(feature = "event-bus")]
    pub(super) event_bus: Option<Arc<qubit_event_bus::AsyncEventBus>>,
    #[cfg(feature = "event-bus")]
    pub(super) notification_shutdown_timeout: std::time::Duration,
}

/// Tracks the oldest resource-waiting task and successful newer starts.
#[derive(Default)]
struct ResourceFairness {
    anchor: Option<(TaskId, u64)>,
    bypasses: usize,
}

struct InFlightGuard(Arc<TypedServiceCore>);

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.0.in_flight.fetch_sub(1, Ordering::AcqRel);
        self.0.in_flight_changed.notify_waiters();
        self.0.scheduler_changed.notify_one();
    }
}

/// Typed task submit, dispatch, execution, and cancellation facade.
#[derive(Clone)]
pub struct TypedTaskExecutionService {
    core: Arc<TypedServiceCore>,
    _lifetime: Arc<PublicLifetime>,
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
        let cleanup_worker = OwnerReleaseWorker::shared().map_err(TaskServiceError::SchedulerUnavailable)?;
        let owner = store.acquire_owner().await?;
        let mut owner_guard = OwnerReleaseGuard::new(Arc::clone(&store), owner, cleanup_worker);
        #[cfg(feature = "event-bus")]
        let publisher = if let Some(bus) = options.event_bus {
            if bus.capabilities().durability() != DurabilityCapability::Durable {
                owner_guard.release().await?;
                return Err(TaskServiceError::NotificationProviderNotDurable {
                    provider_id: bus.provider_id().as_str().to_owned(),
                });
            }
            let actual = bus.capabilities().publish_guarantee();
            if !matches!(
                actual,
                PublishGuarantee::Accepted | PublishGuarantee::Confirmed | PublishGuarantee::DurablyStored
            ) {
                owner_guard.release().await?;
                return Err(TaskServiceError::NotificationProviderInsufficientGuarantee {
                    provider_id: bus.provider_id().as_str().to_owned(),
                    actual,
                });
            }
            let prepared = async {
                let publisher =
                    TaskEventPublisher::new(Arc::clone(&store), bus, options.notification_shutdown_timeout)?;
                store.enable_event_outbox().await?;
                Ok(publisher)
            }
            .await;
            match prepared {
                Ok(publisher) => Some(publisher),
                Err(error) => {
                    owner_guard.release().await?;
                    return Err(error);
                }
            }
        } else {
            None
        };
        let core = Arc::new(TypedServiceCore {
            store,
            codecs,
            id_generator,
            engine,
            handlers,
            cancellations: Mutex::new(HashMap::new()),
            external_cancellations: Mutex::new(HashMap::new()),
            owner_guard: tokio::sync::Mutex::new(None),
            admission: tokio::sync::RwLock::new(()),
            scheduler_changed: tokio::sync::Notify::new(),
            scheduler_stopped: AtomicBool::new(false),
            scheduler_stopped_changed: tokio::sync::Notify::new(),
            max_running_tasks: options.max_running_tasks,
            scan_page_size: options.scan_page_size,
            max_resource_bypasses: options.max_resource_bypasses,
            max_attempts: options.max_attempts,
            retry_policy: options.retry_policy,
            fault: Mutex::new(None),
            in_flight: AtomicUsize::new(0),
            in_flight_changed: tokio::sync::Notify::new(),
            shutting_down: AtomicBool::new(false),
            shutdown_changed: tokio::sync::Notify::new(),
            shutdown_completed: tokio::sync::Notify::new(),
            shutdown_progress: Mutex::new(ShutdownProgress::default()),
            public_gone: AtomicBool::new(false),
            owner_released: AtomicBool::new(false),
            #[cfg(feature = "event-bus")]
            publisher,
            #[cfg(feature = "event-bus")]
            notification_close_error: Mutex::new(None),
        });
        let service = Self {
            _lifetime: Arc::new(PublicLifetime {
                core: Arc::downgrade(&core),
            }),
            core,
        };
        if let Err(error) = service.runner().recover_unfinished().await {
            let release = owner_guard.release().await;
            if let Err(release_error) = release {
                return Err(TaskServiceError::Store(release_error));
            }
            return Err(error);
        }
        #[cfg(feature = "event-bus")]
        if let Some(publisher) = &service.core.publisher {
            publisher.start().await;
        }
        *service
            .core
            .owner_guard
            .try_lock()
            .expect("runner owner guard is uncontended") = Some(owner_guard);
        let scheduler = service.runner();
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
        let supervisor = service.runner();
        let supervisor_core = Arc::clone(&service.core);
        tokio::spawn(async move {
            if AssertUnwindSafe(supervisor.shutdown_supervisor())
                .catch_unwind()
                .await
                .is_err()
            {
                let armed_guard = supervisor_core.owner_guard.lock().await.take();
                drop(armed_guard);
                let mut progress = supervisor_core.shutdown_progress.lock();
                let outcome = ShutdownOutcome::StoreUnavailable("typed task shutdown supervisor panicked".into());
                let attempt = progress.attempt;
                if progress.results.len() == attempt {
                    progress.results.push(outcome);
                } else {
                    progress.results[attempt] = outcome;
                }
                progress.retryable_release = false;
                progress.retry_requested = false;
                drop(progress);
                supervisor_core.shutdown_completed.notify_waiters();
            }
        });
        Ok(service)
    }

    /// Creates an internal handle that never retains the public lifetime token.
    fn runner(&self) -> ServiceRunner {
        ServiceRunner {
            core: Arc::clone(&self.core),
        }
    }

    /// Encodes, assigns an ID, durably accepts, and dispatches a typed request.
    pub async fn submit<T: HasModelId + Send + Sync + 'static>(
        &self,
        request: TaskRequest<T>,
    ) -> Result<TaskSummary, TaskServiceError> {
        let _admission = self.core.admission.read().await;
        self.runner().check_fault()?;
        if self.core.shutting_down.load(Ordering::Acquire) {
            return Err(TaskServiceError::ShuttingDown);
        }
        let stored = request
            .encode(&self.core.codecs)
            .map_err(|error| TaskServiceError::TypedRequest(error.to_string()))?;
        let id = TaskId::from_id(self.core.id_generator.generate()?);
        let accepted = self
            .runner()
            .submission_store_result(self.core.store.accept_encoded(id, stored).await)?;
        if accepted.created {
            #[cfg(feature = "event-bus")]
            self.runner().notify_notifications();
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

    /// Returns process-local notification outcomes. Durable backlog remains in
    /// SQLite and is not bounded by this snapshot.
    #[cfg(feature = "event-bus")]
    pub fn notification_stats(&self) -> NotificationStats {
        self.core
            .publisher
            .as_ref()
            .map_or_else(NotificationStats::default, TaskEventPublisher::stats)
    }

    /// Requeues a blocked task after its configuration or handler is repaired.
    pub async fn resume_blocked(
        &self,
        id: TaskId,
        expected_state_version: u64,
    ) -> Result<TaskSummary, TaskServiceError> {
        let _admission = self.core.admission.read().await;
        self.runner().check_fault()?;
        if self.core.shutting_down.load(Ordering::Acquire) || self.core.owner_released.load(Ordering::Acquire) {
            return Err(TaskServiceError::ShuttingDown);
        }
        let task = self
            .runner()
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
        let resumed = self.runner().store_write_result(
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
    /// service owner. Concurrent waiters share one release attempt's result;
    /// a later call retries an owner release that returned a store error.
    pub async fn shutdown(&self) -> Result<(), TaskServiceError> {
        self.core.request_shutdown();
        let (attempt, retry) = {
            let mut progress = self.core.shutdown_progress.lock();
            let retry = progress.retryable_release && progress.results.len() > progress.attempt;
            if retry {
                progress.attempt += 1;
                progress.retry_requested = true;
            }
            (progress.attempt, retry)
        };
        if retry {
            self.core.shutdown_changed.notify_waiters();
        }
        loop {
            let notified = self.core.shutdown_completed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(outcome) = self.core.shutdown_progress.lock().results.get(attempt).cloned() {
                return outcome.into_result();
            }
            notified.await;
        }
    }

    /// Requests cancellation, persisting intent before signalling a handler.
    pub async fn cancel(&self, id: TaskId) -> Result<CancelOutcome, TaskServiceError> {
        let _admission = self.core.admission.read().await;
        self.runner().check_fault()?;
        if self.core.shutting_down.load(Ordering::Acquire) || self.core.owner_released.load(Ordering::Acquire) {
            return Err(TaskServiceError::ShuttingDown);
        }
        let Some(task) = self.runner().store_result(self.core.store.get_encoded_task(id).await)? else {
            return Err(TaskServiceError::Store(crate::store::StoreError::NotFound));
        };
        let summary = task.summary;
        if summary.state.is_terminal() {
            return Ok(CancelOutcome::AlreadyTerminal);
        }
        if matches!(summary.state, TaskState::Queued | TaskState::Blocked { .. }) {
            let updated = self.runner().store_write_result(
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
            self.core.scheduler_changed.notify_one();
            return Ok(CancelOutcome::CancelledBeforeStart);
        }
        let descriptor = self
            .core
            .handlers
            .descriptor(&summary.kind_id)
            .ok_or(TaskServiceError::CancellationUnsupported)?;
        if descriptor.cancellation_mode == CancellationMode::Unsupported {
            return Ok(CancelOutcome::CancellationUnsupported);
        }
        if descriptor.cancellation_mode == CancellationMode::ExternalHook {
            let key = (id, summary.attempt);
            let (flight, start) = {
                let mut entries = self.core.external_cancellations.lock();
                match entries.get(&key) {
                    Some(ExternalCancelEntry::InFlight(flight)) => (Arc::clone(flight), false),
                    Some(ExternalCancelEntry::Succeeded) => {
                        return Ok(CancelOutcome::CancellationRequested);
                    }
                    None => {
                        let (result, _) = watch::channel(None);
                        let flight = Arc::new(ExternalCancelFlight { result });
                        entries.insert(key, ExternalCancelEntry::InFlight(Arc::clone(&flight)));
                        (flight, true)
                    }
                }
            };
            if start {
                self.core.in_flight.fetch_add(1, Ordering::AcqRel);
                let core = Arc::clone(&self.core);
                let kind_id = summary.kind_id.clone();
                let task_flight = Arc::clone(&flight);
                tokio::spawn(async move {
                    let _in_flight = InFlightGuard(Arc::clone(&core));
                    ServiceRunner::run_external_cancel(core, key, kind_id, task_flight).await;
                });
            }
            drop(_admission);
            return Self::wait_external_cancel(id, summary.attempt, flight).await;
        }
        if summary.cancel_requested {
            return Ok(CancelOutcome::CancellationRequested);
        }
        let _cancellation_requested = self.runner().store_write_result(
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
            CancellationMode::ExternalHook => unreachable!("external hooks are handled above"),
        }
    }

    async fn wait_external_cancel(
        id: TaskId,
        attempt: u32,
        flight: Arc<ExternalCancelFlight>,
    ) -> Result<CancelOutcome, TaskServiceError> {
        let mut result = flight.result.subscribe();
        loop {
            if let Some(result) = result.borrow().clone() {
                return match result {
                    ExternalCancelResult::Success => Ok(CancelOutcome::CancellationRequested),
                    ExternalCancelResult::AlreadyTerminal => Ok(CancelOutcome::AlreadyTerminal),
                    ExternalCancelResult::HookFailed(message) => Err(TaskServiceError::ExternalCancellationFailed {
                        task_id: id,
                        attempt,
                        message,
                    }),
                    ExternalCancelResult::ServiceFailed(message) => Err(TaskServiceError::StoreUnavailable(message)),
                };
            }
            if result.changed().await.is_err() {
                return Err(TaskServiceError::SchedulerUnavailable(
                    "external cancellation task ended without a result".into(),
                ));
            }
        }
    }
}

impl ServiceRunner {
    fn external_cancel_panic_message(panic: Box<dyn std::any::Any + Send>) -> String {
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

    async fn run_external_cancel(
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

    /// Waits for a stop request, drains work, and publishes one terminal
    /// result.
    async fn shutdown_supervisor(&self) {
        loop {
            let notified = self.core.shutdown_changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.core.shutting_down.load(Ordering::Acquire) {
                break;
            }
            notified.await;
        }
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
        loop {
            let notified = self.core.in_flight_changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.core.in_flight.load(Ordering::Acquire) == 0 {
                break;
            }
            notified.await;
        }
        #[cfg(feature = "event-bus")]
        if let Some(publisher) = &self.core.publisher
            && let Err(error) = publisher.close().await
        {
            *self.core.notification_close_error.lock() = Some(error.to_string());
        }
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

    #[cfg(feature = "event-bus")]
    /// Wakes the optional durable event publisher after a store write.
    fn notify_notifications(&self) {
        if let Some(publisher) = &self.core.publisher {
            publisher.notify();
        }
    }

    /// Converts a store write result and wakes the optional publisher.
    fn store_write_result<T>(&self, result: Result<T, crate::store::StoreError>) -> Result<T, TaskServiceError> {
        let value = self.store_result(result)?;
        #[cfg(feature = "event-bus")]
        self.notify_notifications();
        Ok(value)
    }

    /// Records the first scheduler/store fault and wakes every shutdown stage.
    fn latch_fault(&self, error: impl std::fmt::Display) {
        let mut fault = self.core.fault.lock();
        if fault.is_none() {
            *fault = Some(error.to_string());
        }
        drop(fault);
        self.core.request_shutdown();
        self.core.in_flight_changed.notify_waiters();
    }

    /// Returns the first latched store or scheduler fault, if any.
    fn check_fault(&self) -> Result<(), TaskServiceError> {
        self.core
            .fault
            .lock()
            .clone()
            .map_or(Ok(()), |fault| Err(TaskServiceError::StoreUnavailable(fault)))
    }

    /// Latches operational store failures while preserving conflict errors.
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

    /// Maps known rejection errors from a single submit store result to
    /// structured service errors without changing the service state. Other
    /// store errors retain the existing fault-latching behavior.
    fn submission_store_result<T>(&self, result: Result<T, crate::store::StoreError>) -> Result<T, TaskServiceError> {
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

    async fn recover_unfinished(&self) -> Result<(), TaskServiceError> {
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

    async fn scheduler_loop(&self) {
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
                    let page = match self
                        .core
                        .store
                        .list_ready_queued(
                            after,
                            std::num::NonZeroUsize::new(self.core.scan_page_size).expect("scan size is nonzero"),
                            now,
                        )
                        .await
                    {
                        Ok(page) => page,
                        Err(error) => {
                            self.latch_fault(error);
                            return;
                        }
                    };
                    for summary in &page.records {
                        if self.core.shutting_down.load(Ordering::Acquire)
                            || self.core.in_flight.load(Ordering::Acquire) >= self.core.max_running_tasks
                        {
                            break;
                        }
                        if fairness.anchor == Some((summary.id, summary.accepted_at_ms)) {
                            saw_anchor = true;
                        }
                        let Some(descriptor) = self.core.handlers.descriptor(&summary.kind_id) else {
                            self.mark_blocked(summary, "handler is not registered").await;
                            continue;
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
                            continue;
                        }
                        if self.core.codecs.get(&summary.payload_codec_id).is_none() {
                            self.mark_blocked(summary, "payload bytes codec is not registered")
                                .await;
                            continue;
                        }
                        let reservation = match self
                            .core
                            .engine
                            .try_prepare_typed(summary.id, summary.resource_limit.clone())
                        {
                            Ok(reservation) => reservation,
                            Err(EngineError::TemporarilyUnavailable) => {
                                if fairness.anchor.is_none() {
                                    fairness.anchor = Some((summary.id, summary.accepted_at_ms));
                                    saw_anchor = true;
                                }
                                continue;
                            }
                            Err(EngineError::Unsatisfiable) => {
                                self.mark_blocked(summary, "requested resources exceed configured capacity")
                                    .await;
                                continue;
                            }
                            Err(error) => {
                                self.mark_blocked(summary, &error.to_string()).await;
                                continue;
                            }
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
                                self.latch_fault(error);
                                return;
                            }
                        };
                        drop(admission);
                        if let Some((anchor_id, anchor_accepted_at_ms)) = fairness.anchor {
                            let key = (summary.accepted_at_ms, summary.id);
                            let anchor_key = (anchor_accepted_at_ms, anchor_id);
                            if key == anchor_key {
                                fairness = ResourceFairness::default();
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
                if completed_scan && !saw_anchor && fairness.anchor.is_some() {
                    fairness = ResourceFairness::default();
                    continue 'scheduler;
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
    }

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
    payload: crate::model::typed::StoredPayload,
) -> Result<crate::handler::typed::PreparedTask, String> {
    handlers
        .prepare(kind_id, payload, codecs)
        .map_err(|error| error.to_string())
}
