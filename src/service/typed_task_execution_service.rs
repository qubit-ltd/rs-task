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
use tokio::sync::watch;

#[cfg(feature = "event-bus")]
use super::NotificationStats;
use super::owner_release_guard::OwnerReleaseGuard;
use super::owner_release_guard::OwnerReleaseWorker;
#[cfg(feature = "event-bus")]
use super::task_event_publisher::TaskEventPublisher;
use crate::engine::LocalTaskExecutionEngine;
use crate::handler::typed::CancellationMode;
use crate::handler::typed::TypedTaskHandlerRegistry;
use crate::model::TaskState;
use crate::model::typed::TaskId;
use crate::model::typed::TaskPage;
use crate::model::typed::TaskQuery;
use crate::model::typed::TaskRequest;
use crate::model::typed::TaskSummary;
use crate::model::typed::TransitionCommand;
use crate::service::CancelOutcome;
#[cfg(feature = "event-bus")]
use crate::service::NotificationFailure;
use crate::service::RetryPolicy;
use crate::service::TaskServiceError;
use crate::store::TaskStore;

mod internal;
mod typed_service_options;

use internal::ExternalCancelEntry;
use internal::ExternalCancelFlight;
use internal::ExternalCancelResult;
use internal::InFlightGuard;
use internal::PublicLifetime;
use internal::ServiceRunner;
use internal::ShutdownOutcome;
use internal::ShutdownProgress;
use internal::TypedServiceCore;
pub(super) use typed_service_options::TypedServiceOptions;

/// Typed task submit, dispatch, execution, and cancellation facade.
/// A value is created by [`TypedTaskExecutionServiceBuilder::build`] after the
/// caller supplies the store, codec registry, and ID generator to the builder.
/// Dropping the final handle requests graceful shutdown of the service.
///
/// # Examples
///
/// ```
/// use qubit_task::service::TaskExecutionServiceBuilder;
/// use qubit_task::service::TaskServiceError;
///
/// async fn start_service(
///     builder: TaskExecutionServiceBuilder,
/// ) -> Result<(), TaskServiceError> {
///     let service = builder.build().await?;
///     service.shutdown().await
/// }
/// ```
#[must_use = "keep the service handle alive until shutdown is requested"]
#[derive(Clone)]
pub struct TypedTaskExecutionService {
    /// Shared state used by this public handle and its background workers.
    core: Arc<TypedServiceCore>,
    /// Requests graceful shutdown when the final public handle is dropped.
    _lifetime: Arc<PublicLifetime>,
}

impl TypedTaskExecutionService {
    /// Creates the service after acquiring store ownership and recovering
    /// unfinished tasks, then starts its background workers.
    ///
    /// # Parameters
    ///
    /// * `store` - Persistence implementation whose ownership is acquired.
    /// * `codecs` - Payload codec registry shared with task handlers.
    /// * `id_generator` - Generator used for newly accepted task identities.
    /// * `engine` - Resource reservation engine used by task dispatch.
    /// * `handlers` - Registry of handlers available to execute tasks.
    /// * `options` - Scheduler, retry, and optional notification settings.
    ///
    /// # Returns
    ///
    /// A service with its scheduler and shutdown supervisor running.
    ///
    /// # Errors
    ///
    /// Returns an error if store ownership, recovery, or publisher setup fails.
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

    /// Encodes, assigns an ID, durably accepts, and dispatches a typed request.
    ///
    /// # Parameters
    ///
    /// * `request` - Typed request containing routing, payload, and scheduling
    ///   metadata.
    ///
    /// # Returns
    ///
    /// The accepted task summary, including its assigned identity and initial
    /// state.
    ///
    /// # Errors
    ///
    /// Returns an error if admission has stopped, request encoding or ID
    /// generation fails, or the store rejects the request or cannot accept
    /// it.
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
    ///
    /// # Parameters
    ///
    /// * `id` - Stable identity of the task to read.
    ///
    /// # Returns
    ///
    /// `Some` contains the persisted summary; `None` means no current record is
    /// retained.
    ///
    /// # Errors
    ///
    /// Returns an error if the store cannot read the task.
    pub async fn get(&self, id: TaskId) -> Result<Option<TaskSummary>, TaskServiceError> {
        Ok(self.core.store.get_encoded_task(id).await?.map(|task| task.summary))
    }

    /// Returns the authoritative current state and revision when the store has
    /// one.
    ///
    /// A pruned SQLite task can still return its terminal checkpoint. `None`
    /// means the task was never accepted or the store no longer retains its
    /// state.
    ///
    /// # Parameters
    ///
    /// * `id` - Stable identity of the task to inspect.
    ///
    /// # Returns
    ///
    /// The retained authoritative checkpoint, or `None` when no checkpoint
    /// remains.
    ///
    /// # Errors
    ///
    /// Returns `TaskServiceError` when the store fails to read or decode the
    /// lifecycle state.
    pub async fn get_state_checkpoint(
        &self,
        id: TaskId,
    ) -> Result<Option<crate::model::typed::TaskStateCheckpoint>, TaskServiceError> {
        Ok(self.core.store.get_state_checkpoint(id).await?)
    }

    /// Queries typed task summaries using category filters and numeric cursors.
    ///
    /// # Parameters
    ///
    /// * `query` - Filters, cursor, and page bound for the history query.
    ///
    /// # Returns
    ///
    /// A page of matching summaries and an optional continuation cursor.
    ///
    /// # Errors
    ///
    /// Returns an error if the store cannot execute the query.
    pub async fn query(&self, query: TaskQuery) -> Result<TaskPage, TaskServiceError> {
        Ok(self.core.store.list_encoded(query).await?)
    }

    /// Returns process-local notification outcomes. Durable backlog remains in
    /// SQLite and is not bounded by this snapshot.
    ///
    /// # Returns
    ///
    /// A snapshot of this process's notification counters, or zeroed counters
    /// when no publisher is configured.
    #[cfg(feature = "event-bus")]
    pub fn notification_stats(&self) -> NotificationStats {
        self.core
            .publisher
            .as_ref()
            .map_or_else(NotificationStats::default, TaskEventPublisher::stats)
    }

    /// Returns the current process-local failure snapshot for the ordered
    /// notification outbox, if its head is failing publication.
    ///
    /// The snapshot is cleared when that head is published, quarantined, or
    /// replaced. Quarantining intentionally leaves a notification gap; the
    /// archived event is not replayed automatically, so consumers should use
    /// task state versions to detect gaps and query authoritative task state.
    ///
    /// # Returns
    ///
    /// `Some` contains the current failed outbox head's diagnostic snapshot;
    /// `None` means there is no publisher or no failing head.
    #[cfg(feature = "event-bus")]
    pub fn notification_failure(&self) -> Option<NotificationFailure> {
        self.core.publisher.as_ref().and_then(TaskEventPublisher::failure)
    }

    /// Archives and removes the exact currently failed notification head.
    ///
    /// This intentionally creates a notification gap and does not count the
    /// event as published. Check the task's authoritative state and decide how
    /// consumers should recover before calling this method. If publication
    /// succeeds or the outbox head changes before the worker processes this
    /// command, the method returns `StoreError::Conflict` and leaves the event
    /// untouched. A command accepted before shutdown completes before store
    /// ownership is released, even if this future's caller is cancelled.
    ///
    /// # Parameters
    ///
    /// * `expected_task_id` - Identity of the task owning the failed
    ///   notification.
    /// * `expected_state_version` - Revision that must still be current.
    /// * `expected_event_id` - Identity that must still be the failed outbox
    ///   head.
    /// * `reason` - Operator explanation stored with the archived event.
    ///
    /// # Returns
    ///
    /// `Ok(())` when the exact failed notification head is archived and
    /// removed.
    ///
    /// # Errors
    ///
    /// Returns `ShuttingDown` after shutdown begins, `StoreError::Conflict`
    /// when the supplied identity is no longer the failed head,
    /// `StoreError::UnsupportedCapability` when no publisher or quarantine
    /// capability is available, or `InvalidRequest` for a blank or oversized
    /// reason and when the bounded operator-control queue is full.
    #[cfg(feature = "event-bus")]
    pub async fn quarantine_failed_notification(
        &self,
        expected_task_id: TaskId,
        expected_state_version: u64,
        expected_event_id: &str,
        reason: &str,
    ) -> Result<(), TaskServiceError> {
        if self.core.shutting_down.load(Ordering::Acquire) || self.core.owner_released.load(Ordering::Acquire) {
            return Err(TaskServiceError::ShuttingDown);
        }
        let publisher = self
            .core
            .publisher
            .as_ref()
            .ok_or(crate::store::StoreError::UnsupportedCapability)?;
        publisher
            .quarantine_failed(
                expected_task_id,
                expected_state_version,
                expected_event_id,
                reason,
                &self.core.shutting_down,
            )
            .await
    }

    /// Requeues a blocked task after its configuration or handler is repaired.
    ///
    /// # Parameters
    ///
    /// * `id` - Identity of the blocked task.
    /// * `expected_state_version` - Revision observed by the caller, preventing
    ///   stale updates.
    ///
    /// # Returns
    ///
    /// The task summary after it has been queued.
    ///
    /// # Errors
    ///
    /// Returns an error if shutdown has begun, the task is missing or changed,
    /// it is not blocked, cancellation remains pending, or the store fails
    /// the transition.
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
    ///
    /// # Returns
    ///
    /// `Ok(())` when shutdown releases ownership successfully.
    ///
    /// # Errors
    ///
    /// Returns an error when a store fault, owner release, or notification
    /// close fails.
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
    ///
    /// # Parameters
    ///
    /// * `id` - Identity of the task whose cancellation is requested.
    ///
    /// # Returns
    ///
    /// The outcome describing whether cancellation was requested, unsupported,
    /// or already complete.
    ///
    /// # Errors
    ///
    /// Returns an error if shutdown has begun, the task is missing, storage
    /// fails, or an external cancellation hook fails.
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
            let Some(flight) = self.start_or_join_external_cancel(key, &summary.kind_id) else {
                return Ok(CancelOutcome::CancellationRequested);
            };
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

    /// Shares an existing external cancellation hook or starts the task
    /// attempt's hook.
    fn start_or_join_external_cancel(&self, key: (TaskId, u32), kind_id: &str) -> Option<Arc<ExternalCancelFlight>> {
        let (flight, start) = {
            let mut entries = self.core.external_cancellations.lock();
            match entries.get(&key) {
                Some(ExternalCancelEntry::InFlight(flight)) => (Arc::clone(flight), false),
                Some(ExternalCancelEntry::Succeeded) => return None,
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
            let kind_id = kind_id.to_owned();
            let task_flight = Arc::clone(&flight);
            tokio::spawn(async move {
                let _in_flight = InFlightGuard(Arc::clone(&core));
                ServiceRunner::run_external_cancel(core, key, kind_id, task_flight).await;
            });
        }
        Some(flight)
    }

    /// Creates an internal handle that never retains the public lifetime token.
    #[inline]
    fn runner(&self) -> ServiceRunner {
        ServiceRunner {
            core: Arc::clone(&self.core),
        }
    }

    /// Waits for the shared external hook and maps its outcome for this caller.
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

/// Returns the current Unix timestamp in milliseconds, or zero if the system
/// clock predates the Unix epoch.
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Resolves a typed handler and decodes its stored payload before execution.
///
/// Returns the prepared task on success, or the handler/codec error rendered as
/// a string when the registered handler cannot accept the payload.
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
