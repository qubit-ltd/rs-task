// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::panic::resume_unwind;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

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
use tokio::task::spawn_blocking;

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
use crate::model::typed::AcceptOutcome;
use crate::model::typed::TaskId;
use crate::model::typed::TaskPage;
use crate::model::typed::TaskQuery;
use crate::model::typed::TaskRequest;
use crate::model::typed::TaskSummary;
use crate::model::typed::TransitionCommand;
use crate::service::CancelOutcome;
#[cfg(feature = "event-bus")]
use crate::service::NotificationFailure;
use crate::service::TaskPages;
use crate::service::TaskServiceError;
use crate::service::WaitTaskError;
use crate::service::task_route_error::validate_task_route;
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
use internal::SubmissionOperation;
use internal::TypedServiceCore;
pub(super) use typed_service_options::TypedServiceOptions;

/// Typed task submit, dispatch, execution, and cancellation facade.
/// A value is created by [`crate::service::TaskExecutionServiceBuilder::build`]
/// after the caller supplies the store, codec registry, and ID generator to the
/// builder. Dropping the final handle requests graceful shutdown of the
/// service.
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
pub struct TaskExecutionService {
    /// Shared state used by this public handle and its background workers.
    core: Arc<TypedServiceCore>,
    /// Requests graceful shutdown when the final public handle is dropped.
    _lifetime: Arc<PublicLifetime>,
}

impl TaskExecutionService {
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
            admission: Arc::new(tokio::sync::RwLock::new(())),
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
    /// # Type Parameters
    ///
    /// * `T` - Request payload type, which supplies model identity and is safe
    ///   to share across threads for the duration of submission.
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
    ///
    /// Synchronous request encoding and ID generation run on Tokio's blocking
    /// pool. Cancelling before acceptance is dispatched prevents the task from
    /// being accepted, but does not stop either blocking call after it starts.
    /// Once acceptance is dispatched, cancellation can leave its outcome
    /// uncertain; use a stable idempotency key to safely retry the request.
    ///
    /// # Panics
    ///
    /// Resumes a panic from the request encoder or ID generator after its
    /// blocking task has been joined.
    pub async fn submit<T: HasModelId + Send + Sync + 'static>(
        &self,
        request: TaskRequest<T>,
    ) -> Result<AcceptOutcome, TaskServiceError> {
        self.submit_inner(request, false).await
    }

    /// Encodes a request, validates it against the currently registered route,
    /// and submits it only when the handler and codec are ready.
    ///
    /// Route validation happens before allocating a task ID or writing to the
    /// store. This is useful when callers need configuration errors reported
    /// synchronously with submission, while [`Self::submit`] defers route
    /// validation until dispatch.
    ///
    /// # Type Parameters
    ///
    /// * `T` - Request payload type with model identity that can be shared
    ///   between the service and its blocking encoder.
    ///
    /// # Parameters
    ///
    /// * `request` - Typed request whose route metadata is checked before
    ///   acceptance.
    ///
    /// # Returns
    ///
    /// The accepted task summary with its assigned identity and initial state.
    ///
    /// # Errors
    ///
    /// Returns an error if encoding or ID generation fails, the route is not
    /// registered, admission has stopped, or the store rejects the request.
    ///
    /// # Panics
    ///
    /// Resumes a panic from the request encoder or ID generator after its
    /// blocking task has been joined.
    pub async fn submit_checked<T: HasModelId + Send + Sync + 'static>(
        &self,
        request: TaskRequest<T>,
    ) -> Result<AcceptOutcome, TaskServiceError> {
        self.submit_inner(request, true).await
    }

    /// Encodes and accepts a request while honoring the shared admission lock.
    ///
    /// Route validation is performed only when `checked` is true. Encoding,
    /// ID generation, and store acceptance retain the service's cancellation
    /// and shutdown behavior for both submission entry points.
    ///
    /// # Type Parameters
    ///
    /// * `T` - Request payload type used by the typed encoder.
    ///
    /// # Parameters
    ///
    /// * `request` - Typed request to encode and submit.
    /// * `checked` - Whether to validate handler and codec registration before
    ///   allocating an ID.
    ///
    /// # Returns
    ///
    /// The accepted task outcome after the store records the request.
    ///
    /// # Errors
    ///
    /// Returns request encoding, route, ID generation, admission, or store
    /// errors encountered during submission.
    ///
    /// # Panics
    ///
    /// Resumes a panic from the request encoder or ID generator after its
    /// blocking task has been joined.
    async fn submit_inner<T: HasModelId + Send + Sync + 'static>(
        &self,
        request: TaskRequest<T>,
        checked: bool,
    ) -> Result<AcceptOutcome, TaskServiceError> {
        let admission = Arc::clone(&self.core.admission).read_owned().await;
        self.runner().check_fault()?;
        if self.core.shutting_down.load(Ordering::Acquire) {
            return Err(TaskServiceError::ShuttingDown);
        }
        let codecs = Arc::clone(&self.core.codecs);
        let stored = match spawn_blocking(move || request.encode(&codecs)).await {
            Ok(Ok(stored)) => stored,
            Ok(Err(error)) => return Err(TaskServiceError::RequestEncoding(error)),
            Err(error) if error.is_panic() => resume_unwind(error.into_panic()),
            Err(error) => {
                return Err(TaskServiceError::SchedulerUnavailable(format!(
                    "task request encoding worker unavailable: {error}"
                )));
            }
        };
        if checked {
            self.validate_route(&stored)?;
        }
        let generator = Arc::clone(&self.core.id_generator);
        let allocated = match spawn_blocking(move || generator.generate()).await {
            Ok(result) => result?,
            Err(error) if error.is_panic() => resume_unwind(error.into_panic()),
            Err(error) => {
                return Err(TaskServiceError::SchedulerUnavailable(format!(
                    "task ID generation worker unavailable: {error}"
                )));
            }
        };
        let id = TaskId::from_id(allocated);
        let operation = SubmissionOperation::new(Arc::clone(&self.core), admission, id, stored);
        match tokio::spawn(operation.run()).await {
            Ok(result) => result,
            Err(error) if error.is_panic() => resume_unwind(error.into_panic()),
            Err(error) => Err(TaskServiceError::SchedulerUnavailable(format!(
                "task submission worker unavailable: {error}"
            ))),
        }
    }

    /// Validates stored request metadata against the configured
    /// handlers/codecs.
    ///
    /// # Parameters
    ///
    /// * `request` - Encoded payload metadata produced from the submitted
    ///   request.
    ///
    /// # Returns
    ///
    /// Returns unit when the handler, payload type, schema, and codec match.
    ///
    /// # Errors
    ///
    /// Returns the first [`crate::service::TaskRouteError`] describing a route
    /// mismatch.
    fn validate_route(
        &self,
        request: &crate::model::typed::StoredTaskRequest,
    ) -> Result<(), crate::service::TaskRouteError> {
        validate_task_route(
            &self.core.handlers,
            &self.core.codecs,
            &request.kind_id,
            request.payload.type_id.as_str(),
            request.payload.schema_version,
            &request.payload.codec_id,
        )
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
    pub async fn get_summary(&self, id: TaskId) -> Result<Option<TaskSummary>, TaskServiceError> {
        Ok(self.core.store.get_summary(id).await?)
    }

    /// Creates a cursor-based paginator for matching task summaries.
    ///
    /// The paginator borrows this service and advances only after each page is
    /// read successfully.
    ///
    /// # Parameters
    ///
    /// * `query` - Filters, initial cursor, and bound for each returned page.
    ///
    /// # Returns
    ///
    /// A paginator that fetches successive pages from the same query.
    pub fn pages(&self, query: TaskQuery) -> TaskPages<'_> {
        TaskPages::new(self, query)
    }

    /// Waits until a task reaches a terminal state or the timeout expires.
    ///
    /// The store is polled at bounded intervals; the returned checkpoint is
    /// the authoritative terminal state observed by the store.
    ///
    /// # Parameters
    ///
    /// * `id` - Identity of the task to observe.
    /// * `timeout` - Maximum duration to wait for a terminal checkpoint.
    ///
    /// # Returns
    ///
    /// The task's terminal state checkpoint.
    ///
    /// # Errors
    ///
    /// Returns [`WaitTaskError::Unavailable`] when the task is absent,
    /// [`WaitTaskError::Timeout`] when the deadline passes, or
    /// [`WaitTaskError::Read`] when the store fails.
    pub async fn wait_terminal(
        &self,
        id: TaskId,
        timeout: Duration,
    ) -> Result<crate::model::typed::TaskStateCheckpoint, WaitTaskError> {
        let wait = async {
            loop {
                let checkpoint = self
                    .core
                    .store
                    .get_state_checkpoint(id)
                    .await
                    .map_err(WaitTaskError::Read)?
                    .ok_or(WaitTaskError::Unavailable(id))?;
                if checkpoint.state.is_terminal() {
                    return Ok(checkpoint);
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        };
        match tokio::time::timeout(timeout, wait).await {
            Ok(result) => result,
            Err(_) => Err(WaitTaskError::Timeout(id)),
        }
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
        observed: &crate::service::NotificationFailure,
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
                observed.task_id,
                observed.state_version,
                &observed.event_id,
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
    pub async fn resume_blocked(&self, observed: &TaskSummary) -> Result<TaskSummary, TaskServiceError> {
        let id = observed.id;
        let expected_state_version = observed.state_version;
        let _admission = self.core.admission.read().await;
        self.runner().check_fault()?;
        if self.core.shutting_down.load(Ordering::Acquire) || self.core.owner_released.load(Ordering::Acquire) {
            return Err(TaskServiceError::ShuttingDown);
        }
        let summary = self
            .runner()
            .store_result(self.core.store.get_summary(id).await)?
            .ok_or(crate::store::StoreError::NotFound)?;
        if summary.state_version != expected_state_version {
            return Err(crate::store::StoreError::Conflict.into());
        }
        if !matches!(summary.state, TaskState::Blocked { .. }) {
            return Err(TaskServiceError::NotBlocked {
                actual: summary.state.kind(),
            });
        }
        if summary.cancel_requested {
            return Err(TaskServiceError::CancellationPending);
        }
        let resumed = self.runner().store_write_result(
            self.core
                .store
                .transition_encoded(TransitionCommand {
                    id,
                    expected_state_version,
                    expected_attempt: summary.attempt,
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
    ///
    /// # Panics
    ///
    /// Resumes a panic from the store after latching a service fault.
    pub async fn request_cancel(&self, id: TaskId) -> Result<CancelOutcome, TaskServiceError> {
        let _admission = Arc::clone(&self.core.admission).read_owned().await;
        self.runner().check_fault()?;
        if self.core.shutting_down.load(Ordering::Acquire) || self.core.owner_released.load(Ordering::Acquire) {
            return Err(TaskServiceError::ShuttingDown);
        }
        let Some(summary) = self.runner().store_result(self.core.store.get_summary(id).await)? else {
            return Err(TaskServiceError::Store(crate::store::StoreError::NotFound));
        };
        if summary.state.is_terminal() {
            return Ok(CancelOutcome::AlreadyTerminal);
        }
        if matches!(summary.state, TaskState::Queued | TaskState::Blocked { .. }) {
            return Self::run_cancel_transition(
                self.runner(),
                _admission,
                TransitionCommand {
                    id,
                    expected_state_version: summary.state_version,
                    expected_attempt: summary.attempt,
                    retry_not_before_ms: None,
                    state: TaskState::Cancelled,
                    cancel_requested: false,
                    cancel_error: None,
                    finished_at_ms: Some(now_ms()),
                    output: None,
                },
                false,
            )
            .await;
        }
        let Some(cancellation_mode) = self.core.handlers.cancellation_mode(&summary.kind_id) else {
            return Ok(CancelOutcome::CancellationUnsupported);
        };
        if cancellation_mode == CancellationMode::Unsupported {
            return Ok(CancelOutcome::CancellationUnsupported);
        }
        if cancellation_mode == CancellationMode::ExternalHook {
            let key = (id, summary.attempt);
            let Some(flight) = self.start_or_join_external_cancel(key, &summary.kind_id) else {
                return Ok(CancelOutcome::CancellationRequested);
            };
            drop(_admission);
            return Self::wait_external_cancel(id, summary.attempt, flight).await;
        }
        if summary.cancel_requested {
            if let Some(state) = self.core.cancellations.lock().get(&(id, summary.attempt)) {
                state.request(cancellation_mode == CancellationMode::Cooperative);
            }
            return Ok(CancelOutcome::CancellationRequested);
        }
        Self::run_cancel_transition(
            self.runner(),
            _admission,
            TransitionCommand {
                id,
                expected_state_version: summary.state_version,
                expected_attempt: summary.attempt,
                retry_not_before_ms: None,
                state: TaskState::Running,
                cancel_requested: true,
                cancel_error: None,
                finished_at_ms: None,
                output: None,
            },
            cancellation_mode == CancellationMode::Cooperative,
        )
        .await
    }

    /// Completes a local cancellation write and its follow-up notification in
    /// a service-owned task so caller cancellation cannot release store
    /// ownership between a committed transition and its wake or signal.
    ///
    /// # Parameters
    ///
    /// - `runner`: Shared service operations for writing, signaling, and waking
    ///   dispatch.
    /// - `admission`: Owned read guard retained until persistence and its
    ///   follow-up notification finish.
    /// - `command`: Compare-and-set transition to persist.
    /// - `cooperative`: Whether a successful Running transition should signal
    ///   the cooperative handler.
    ///
    /// # Returns
    ///
    /// The cancellation outcome after the transition and follow-up complete.
    ///
    /// # Errors
    ///
    /// Returns the mapped store error, or a scheduler-unavailable error if
    /// the service-owned task is cancelled.
    ///
    /// # Panics
    ///
    /// Resumes a panic from the store after latching a service fault.
    async fn run_cancel_transition(
        runner: ServiceRunner,
        admission: tokio::sync::OwnedRwLockReadGuard<()>,
        command: TransitionCommand,
        cooperative: bool,
    ) -> Result<CancelOutcome, TaskServiceError> {
        let runner_on_join_error = runner.clone();
        let before_run = matches!(command.state, TaskState::Cancelled);
        let cancellation_key = (command.id, command.expected_attempt);
        let task = tokio::spawn(async move {
            let _admission = admission;
            runner.store_write_result(runner.core.store.transition_encoded(command).await)?;
            if before_run {
                runner.core.scheduler_changed.notify_one();
                Ok(CancelOutcome::CancelledBeforeRun)
            } else {
                if cooperative && let Some(state) = runner.core.cancellations.lock().get(&cancellation_key) {
                    state.request(true);
                }
                Ok(CancelOutcome::CancellationRequested)
            }
        });
        match task.await {
            Ok(result) => result,
            Err(error) if error.is_panic() => {
                runner_on_join_error.latch_fault(format!("task cancellation operation failed: {error}"));
                resume_unwind(error.into_panic())
            }
            Err(error) => Err(TaskServiceError::SchedulerUnavailable(format!(
                "task cancellation worker unavailable: {error}"
            ))),
        }
    }

    /// Reuses an active external cancellation hook or starts one for this
    /// attempt.
    ///
    /// # Parameters
    ///
    /// * `key` - Task ID and attempt number identifying the cancellation.
    /// * `kind_id` - Handler kind whose external hook should be invoked.
    ///
    /// # Returns
    ///
    /// The shared in-flight hook state, or `None` when no hook is configured.
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
    ///
    /// # Parameters
    ///
    /// * `id` - Task ID associated with the cancellation hook.
    /// * `attempt` - Attempt number associated with the hook.
    /// * `flight` - Shared result channel for the in-flight hook.
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
///
/// # Parameters
///
/// * `handlers` - Registry used to resolve the task kind.
/// * `codecs` - Registry used to decode the stored payload.
/// * `kind_id` - Task kind identifier to resolve.
/// * `payload` - Encoded payload stored with the task.
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
