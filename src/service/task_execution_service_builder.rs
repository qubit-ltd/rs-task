// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::num::NonZeroUsize;
use std::sync::Arc;
#[cfg(feature = "event-bus")]
use std::time::Duration;

#[cfg(feature = "event-bus")]
use qubit_event_bus::EventBus;
use tokio::runtime;
use tokio::sync;

mod internal;
use internal::OwnerGuard;
use internal::restore_tasks_paged;

use super::admission_budget::AdmissionBudget;
use super::admission_gate::AdmissionGate;
use super::retry_policy::RetryPolicy;
use super::scheduler_queue::SchedulerQueue;
#[cfg(feature = "event-bus")]
use super::task_event_publisher::TaskEventPublisher;
use super::task_execution_service::ServiceCore;
use super::task_execution_service::TaskExecutionService;
pub(crate) use super::task_service_build_error::TaskServiceBuildError;
use crate::engine::LocalTaskExecutionEngine;
use crate::engine::TaskExecutionEngine;
use crate::handler::TaskHandler;
use crate::handler::TaskHandlerRegistry;
use crate::model::ResourceCapacity;
use crate::scheduling::FairFifoPolicy;
use crate::scheduling::SchedulingPolicy;
use crate::store::MemoryTaskStore;
use crate::store::TaskStore;

/// Explicit component assembly and resource policy for one task service.
///
/// The in-memory preset supplies a volatile store and local scheduler/engine;
/// applications can replace each component before calling
/// [`build`](Self::build).
///
/// # Examples
///
/// ```
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use qubit_task::TaskExecutionServiceBuilder;
///
/// let service = TaskExecutionServiceBuilder::in_memory().build().await?;
/// assert!(!service.capabilities().store.restart_recovery);
/// service.shutdown().await?;
/// # Ok(())
/// # }
/// ```
pub struct TaskExecutionServiceBuilder {
    /// Selected persistent or volatile task history store.
    store: Option<Arc<dyn TaskStore>>,
    /// Selected resource reservation and execution backend.
    engine: Option<Arc<dyn TaskExecutionEngine>>,
    /// Selected candidate ordering policy.
    policy: Option<Arc<dyn SchedulingPolicy>>,
    /// Exact-version handlers available during service execution.
    handlers: TaskHandlerRegistry,
    /// Capacity used when the builder creates its default local engine.
    capacity: ResourceCapacity,
    /// Maximum waiting tasks accepted by the scheduler.
    queue_capacity: usize,
    /// Aggregate payload bytes held by detached admissions.
    max_inflight_payload_bytes: NonZeroUsize,
    /// Maximum detached admission workers.
    max_inflight_operations: NonZeroUsize,
    /// Maximum simultaneously running attempts.
    max_running_tasks: NonZeroUsize,
    /// Maximum candidate tasks inspected per scheduler pass.
    scan_budget: usize,
    /// Maximum execution attempts for one task.
    max_attempts: u32,
    /// Delay schedule for retryable failures.
    retry_policy: RetryPolicy,
    /// Whether the selected store must recover unfinished work.
    require_recovery: bool,
    /// Runtime for service-owned background workers, when supplied.
    runtime_handle: Option<runtime::Handle>,
    /// Optional destination for lifecycle notifications.
    #[cfg(feature = "event-bus")]
    event_bus: Option<EventBus>,
    /// Maximum pending lifecycle notifications.
    #[cfg(feature = "event-bus")]
    event_bus_buffer_capacity: NonZeroUsize,
    /// Maximum wait for notification worker shutdown.
    #[cfg(feature = "event-bus")]
    event_bus_close_timeout: Duration,
}

impl Default for TaskExecutionServiceBuilder {
    /// Creates a builder with bounded local-service defaults and no store.
    ///
    /// # Returns
    ///
    /// A builder that requires a store to be selected before `build`.
    fn default() -> Self {
        let cpu_slots = std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get) as u32;
        Self {
            store: None,
            engine: None,
            policy: None,
            handlers: TaskHandlerRegistry::new(),
            capacity: ResourceCapacity {
                cpu_slots,
                ..ResourceCapacity::default()
            },
            queue_capacity: 1024,
            max_inflight_payload_bytes: NonZeroUsize::new(64 * 1024 * 1024).expect("default payload budget is nonzero"),
            max_inflight_operations: NonZeroUsize::new(64).expect("default operation limit is nonzero"),
            max_running_tasks: std::thread::available_parallelism().unwrap_or(NonZeroUsize::MIN),
            scan_budget: 128,
            max_attempts: 3,
            retry_policy: RetryPolicy::default(),
            require_recovery: false,
            runtime_handle: None,
            #[cfg(feature = "event-bus")]
            event_bus: None,
            #[cfg(feature = "event-bus")]
            event_bus_buffer_capacity: NonZeroUsize::new(256).expect("default event bus buffer capacity is nonzero"),
            #[cfg(feature = "event-bus")]
            event_bus_close_timeout: Duration::from_secs(30),
        }
    }
}

impl TaskExecutionServiceBuilder {
    /// Selects explicit volatile storage with standard local components.
    ///
    /// # Returns
    ///
    /// A builder configured with bounded in-memory task history.
    #[must_use]
    pub fn in_memory() -> Self {
        Self::default().store(Arc::new(MemoryTaskStore::new(1024)))
    }

    /// Selects volatile storage with an explicit retained payload budget.
    ///
    /// # Parameters
    ///
    /// * `limit` - Maximum payload bytes retained by the store.
    ///
    /// # Returns
    ///
    /// A builder configured with the supplied payload budget.
    #[must_use]
    pub fn in_memory_with_payload_budget(limit: NonZeroUsize) -> Self {
        Self::default().store(Arc::new(MemoryTaskStore::with_payload_budget(1024, limit)))
    }

    /// Selects a restart-recoverable SQLite store when enabled.
    ///
    /// # Parameters
    ///
    /// * `path` - SQLite database path.
    ///
    /// # Returns
    ///
    /// A builder configured to require restart recovery.
    ///
    /// # Errors
    ///
    /// Returns the store error if opening the database fails.
    #[cfg(feature = "sqlite")]
    pub fn recoverable_sqlite(path: impl AsRef<std::path::Path>) -> Result<Self, TaskServiceBuildError> {
        let store = crate::store::SqliteTaskStore::open(path)?;
        Ok(Self::default().store(Arc::new(store)).require_recovery(true))
    }

    /// Creates a builder with all three core components selected.
    ///
    /// # Parameters
    ///
    /// * `store` - Authoritative task store.
    /// * `engine` - Resource reservation and execution backend.
    /// * `policy` - Task ordering policy.
    ///
    /// # Returns
    ///
    /// A builder initialized with those components.
    #[must_use]
    pub fn from_components(
        store: Arc<dyn TaskStore>,
        engine: Arc<dyn TaskExecutionEngine>,
        policy: Arc<dyn SchedulingPolicy>,
    ) -> Self {
        Self::default().store(store).engine(engine).policy(policy)
    }

    /// Selects the authoritative task store.
    ///
    /// # Parameters
    ///
    /// * `store` - Task store used for acceptance, reads, and transitions.
    ///
    /// # Returns
    ///
    /// This builder with the supplied store selected.
    #[must_use]
    pub fn store(mut self, store: Arc<dyn TaskStore>) -> Self {
        self.store = Some(store);
        self
    }

    /// Selects the resource execution engine.
    ///
    /// # Parameters
    ///
    /// * `engine` - Backend that reserves resources and runs handlers.
    ///
    /// # Returns
    ///
    /// This builder with the supplied engine selected.
    #[must_use]
    pub fn engine(mut self, engine: Arc<dyn TaskExecutionEngine>) -> Self {
        self.engine = Some(engine);
        self
    }

    /// Selects the task ordering policy.
    ///
    /// # Parameters
    ///
    /// * `policy` - Strategy that orders eligible task candidates.
    ///
    /// # Returns
    ///
    /// This builder with the supplied scheduling policy selected.
    #[must_use]
    pub fn policy(mut self, policy: Arc<dyn SchedulingPolicy>) -> Self {
        self.policy = Some(policy);
        self
    }

    /// Adds one exact-version business handler.
    ///
    /// # Parameters
    ///
    /// * `handler` - Handler registered under its descriptor.
    ///
    /// # Returns
    ///
    /// This builder with the handler registered.
    ///
    /// # Errors
    ///
    /// Returns an error when the handler descriptor is invalid or duplicates
    /// an existing registration.
    pub fn register_handler(mut self, handler: Arc<dyn TaskHandler>) -> Result<Self, TaskServiceBuildError> {
        self.handlers
            .register(handler)
            .map_err(|error| TaskServiceBuildError::HandlerConflict(error.to_string()))?;
        Ok(self)
    }

    /// Replaces the handler registry, for example with handlers created by an
    /// SPI registry.
    ///
    /// # Parameters
    ///
    /// * `handlers` - Complete exact-version handler registry.
    ///
    /// # Returns
    ///
    /// This builder with its handler registry replaced.
    #[must_use]
    pub fn handlers(mut self, handlers: TaskHandlerRegistry) -> Self {
        self.handlers = handlers;
        self
    }

    /// Overrides local CPU, GPU, and custom resource capacity.
    ///
    /// # Parameters
    ///
    /// * `capacity` - Total resources supplied by the local engine.
    ///
    /// # Returns
    ///
    /// This builder with the local engine capacity replaced.
    #[must_use]
    pub fn capacity(mut self, capacity: ResourceCapacity) -> Self {
        self.capacity = capacity;
        self
    }

    /// Sets the maximum number of waiting tasks accepted by the service.
    ///
    /// # Parameters
    ///
    /// * `capacity` - Maximum number of queued tasks.
    ///
    /// # Returns
    ///
    /// This builder with the queue limit replaced.
    #[must_use]
    pub fn queue_capacity(mut self, capacity: usize) -> Self {
        self.queue_capacity = capacity;
        self
    }

    /// Sets the maximum payload bytes retained by in-flight admission workers.
    ///
    /// # Parameters
    ///
    /// * `limit` - Aggregate in-flight payload byte limit.
    ///
    /// # Returns
    ///
    /// This builder with the admission payload limit replaced.
    #[must_use]
    pub fn max_inflight_payload_bytes(mut self, limit: NonZeroUsize) -> Self {
        self.max_inflight_payload_bytes = limit;
        self
    }

    /// Sets the maximum number of in-flight admission workers.
    ///
    /// # Parameters
    ///
    /// * `limit` - Maximum detached admission worker count.
    ///
    /// # Returns
    ///
    /// This builder with the admission worker limit replaced.
    #[must_use]
    pub fn max_inflight_operations(mut self, limit: NonZeroUsize) -> Self {
        self.max_inflight_operations = limit;
        self
    }

    /// Sets the maximum number of task attempts that may run simultaneously.
    ///
    /// # Parameters
    ///
    /// * `limit` - Maximum concurrent handler attempts.
    ///
    /// # Returns
    ///
    /// This builder with the running task limit replaced.
    #[must_use]
    pub fn max_running_tasks(mut self, limit: NonZeroUsize) -> Self {
        self.max_running_tasks = limit;
        self
    }

    /// Sets the maximum number of candidates inspected in each scheduler cycle.
    ///
    /// # Parameters
    ///
    /// * `budget` - Requested number of candidates; zero is normalized to one.
    ///
    /// # Returns
    ///
    /// This builder with the normalized scheduler scan budget.
    #[must_use]
    pub fn scan_budget(mut self, budget: usize) -> Self {
        self.scan_budget = budget.max(1);
        self
    }

    /// Sets the maximum execution attempts for a retryable handler result.
    ///
    /// # Parameters
    ///
    /// * `attempts` - Requested attempt limit; zero is normalized to one.
    ///
    /// # Returns
    ///
    /// This builder with the normalized retry attempt limit.
    #[must_use]
    pub fn max_attempts(mut self, attempts: u32) -> Self {
        self.max_attempts = attempts.max(1);
        self
    }

    /// Sets the exponential delay applied between retryable attempts.
    ///
    /// # Parameters
    ///
    /// * `policy` - Delay policy applied to retryable failures.
    ///
    /// # Returns
    ///
    /// This builder with the retry delay policy replaced.
    #[must_use]
    pub fn retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.retry_policy = policy;
        self
    }

    /// Fails construction unless storage declares restart recovery.
    ///
    /// # Parameters
    ///
    /// * `required` - Whether restart recovery is mandatory.
    ///
    /// # Returns
    ///
    /// This builder with the recovery requirement replaced.
    #[must_use]
    pub fn require_recovery(mut self, required: bool) -> Self {
        self.require_recovery = required;
        self
    }

    /// Selects the Tokio runtime used for service-owned background tasks.
    ///
    /// The runtime must remain alive until `TaskExecutionService::shutdown`
    /// completes. When omitted, the service uses its process-wide default
    /// runtime. Store futures continue to run on the runtime polling them.
    ///
    /// # Parameters
    ///
    /// * `handle` - Runtime used for service-owned background workers.
    ///
    /// # Returns
    ///
    /// This builder configured with the supplied runtime.
    #[must_use]
    pub fn runtime_handle(mut self, handle: runtime::Handle) -> Self {
        self.runtime_handle = Some(handle);
        self
    }

    /// Injects the concrete event bus facade for optional status notifications.
    ///
    /// # Parameters
    ///
    /// * `event_bus` - Event bus used for lifecycle notifications.
    ///
    /// # Returns
    ///
    /// This builder configured with the supplied event bus.
    #[cfg(feature = "event-bus")]
    #[must_use]
    pub fn event_bus(mut self, event_bus: EventBus) -> Self {
        self.event_bus = Some(event_bus);
        self
    }

    /// Sets the number of lifecycle notifications waiting behind the publisher.
    ///
    /// # Parameters
    ///
    /// * `capacity` - Maximum number of queued notifications.
    ///
    /// # Returns
    ///
    /// This builder with the notification queue capacity replaced.
    #[cfg(feature = "event-bus")]
    #[must_use]
    pub fn event_bus_buffer_capacity(mut self, capacity: NonZeroUsize) -> Self {
        self.event_bus_buffer_capacity = capacity;
        self
    }

    /// Sets how long service shutdown waits for the lifecycle notification
    /// worker.
    ///
    /// # Parameters
    ///
    /// * `timeout` - Maximum worker close wait.
    ///
    /// # Returns
    ///
    /// This builder with the notification close timeout replaced.
    #[cfg(feature = "event-bus")]
    #[must_use]
    pub fn event_bus_close_timeout(mut self, timeout: Duration) -> Self {
        self.event_bus_close_timeout = timeout;
        self
    }

    /// Builds one unified task service after validating and preparing recovery.
    ///
    /// # Returns
    ///
    /// A running service with recovered work queued for execution.
    ///
    /// # Errors
    ///
    /// Returns a build error when configuration, store setup, recovery, or
    /// background worker startup fails.
    pub async fn build(self) -> Result<TaskExecutionService, TaskServiceBuildError> {
        let (sender, receiver) = sync::oneshot::channel();
        // Construction can acquire a persistent store owner and then await
        // arbitrary store futures. Keep that work alive if the caller drops
        // its build future, so ownership cleanup is not cancelled midway.
        super::task_execution_service::runtime().handle().spawn(async move {
            let result = self.build_inner(&sender).await;
            if let Err(error) = sender.send(result) {
                match error {
                    Ok(service) => {
                        if let Err(error) = service.shutdown().await {
                            eprintln!("cancelled task service build could not finish shutdown: {error}");
                        }
                    }
                    Err(error) => eprintln!("cancelled task service build failed: {error}"),
                }
            }
        });
        receiver.await.unwrap_or(Err(TaskServiceBuildError::WorkerStopped))
    }

    /// Validates components, recovers unfinished rows, and starts service
    /// state.
    ///
    /// # Parameters
    ///
    /// * `sender` - Build caller used to detect cancellation before ownership
    ///   is transferred.
    ///
    /// # Returns
    ///
    /// The started service with any recovered work queued.
    ///
    /// # Errors
    ///
    /// Returns a build error for invalid setup, failed recovery, or failed
    /// ownership cleanup.
    async fn build_inner(
        self,
        sender: &sync::oneshot::Sender<Result<TaskExecutionService, TaskServiceBuildError>>,
    ) -> Result<TaskExecutionService, TaskServiceBuildError> {
        let store = self.store.ok_or(TaskServiceBuildError::MissingStore)?;
        let store_capabilities = store.capabilities();
        if self.require_recovery && !store_capabilities.restart_recovery {
            return Err(TaskServiceBuildError::RecoveryRequired);
        }
        if store_capabilities.restart_recovery && !store_capabilities.persistent_history {
            return Err(TaskServiceBuildError::RecoveryRequired);
        }
        let engine = self
            .engine
            .unwrap_or_else(|| Arc::new(LocalTaskExecutionEngine::new(self.capacity.clone())));
        let policy = self.policy.unwrap_or_else(|| Arc::new(FairFifoPolicy::default()));
        let recovery_limit = self
            .queue_capacity
            .checked_add(self.max_running_tasks.get())
            .ok_or_else(|| {
                TaskServiceBuildError::InvalidConfiguration("queue_capacity + max_running_tasks overflows usize".into())
            })?;
        let owner = if store_capabilities.restart_recovery {
            Some(store.acquire_owner().await?)
        } else {
            None
        };
        let mut owner = OwnerGuard::new(Arc::clone(&store), owner);
        if sender.is_closed() {
            owner.release().await?;
            return Err(TaskServiceBuildError::InvalidConfiguration(
                "service construction caller was cancelled".into(),
            ));
        }
        let recovered_result = async {
            if store_capabilities.restart_recovery {
                if store.has_unfinished_over_limit(recovery_limit).await? {
                    return Err(TaskServiceBuildError::RecoveryCapacityExceeded { limit: recovery_limit });
                }
                restore_tasks_paged(&store, &self.handlers, self.max_attempts, recovery_limit, sender).await
            } else {
                Ok(std::collections::VecDeque::new())
            }
        }
        .await;
        let queue = match recovered_result {
            Ok(queue) => queue,
            Err(error) => {
                if let Err(cleanup) = owner.release().await {
                    return Err(TaskServiceBuildError::CleanupFailed {
                        primary: Box::new(error),
                        cleanup,
                    });
                }
                return Err(error);
            }
        };
        if sender.is_closed() {
            owner.release().await?;
            return Err(TaskServiceBuildError::InvalidConfiguration(
                "service construction caller was cancelled".into(),
            ));
        }
        let queue_count = queue.len();
        let mut scheduler_queue = SchedulerQueue::new();
        for task in queue {
            scheduler_queue.push(task);
        }
        let runtime_handle = self
            .runtime_handle
            .unwrap_or_else(|| super::task_execution_service::runtime().handle().clone());
        #[cfg(feature = "event-bus")]
        let event_bus = match self.event_bus {
            Some(bus) => {
                match TaskEventPublisher::new(bus, self.event_bus_buffer_capacity, self.event_bus_close_timeout) {
                    Ok(publisher) => Some(publisher),
                    Err(error) => {
                        if let Err(cleanup) = owner.release().await {
                            return Err(TaskServiceBuildError::CleanupFailed {
                                primary: Box::new(TaskServiceBuildError::EventPublisherThread(error)),
                                cleanup,
                            });
                        }
                        return Err(TaskServiceBuildError::EventPublisherThread(error));
                    }
                }
            }
            None => None,
        };
        let core = ServiceCore {
            store,
            engine,
            policy,
            runtime_handle,
            handlers: self.handlers,
            queue_capacity: self.queue_capacity,
            running_slots: Arc::new(sync::Semaphore::new(self.max_running_tasks.get())),
            scan_budget: self.scan_budget,
            max_attempts: self.max_attempts,
            retry_policy: self.retry_policy,
            queue: parking_lot::Mutex::new(scheduler_queue),
            queue_count: std::sync::atomic::AtomicUsize::new(queue_count),
            local_handlers: parking_lot::Mutex::new(Default::default()),
            local_finalizations: parking_lot::Mutex::new(Default::default()),
            cancellations: parking_lot::Mutex::new(Default::default()),
            changed: sync::Notify::new(),
            wait_registry: Arc::new(super::task_wait_registry::TaskWaitRegistry::default()),
            transition_event_lock: sync::RwLock::new(()),
            admission: AdmissionGate::new(),
            admission_budget: Arc::new(AdmissionBudget::new(
                self.max_inflight_payload_bytes,
                self.max_inflight_operations,
            )),
            owner: owner.transfer(),
            store_fault: parking_lot::Mutex::new(None),
            scheduler_fault: parking_lot::Mutex::new(None),
            attempts_in_flight: std::sync::atomic::AtomicUsize::new(0),
            attempts_changed: sync::Notify::new(),
            scheduler_finished: std::sync::atomic::AtomicBool::new(false),
            scheduler_finished_notify: sync::Notify::new(),
            #[cfg(feature = "event-bus")]
            event_bus,
        };
        let service = TaskExecutionService::start(core);
        Ok(service)
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::sync::Arc;

    use tokio as tokio_crate;
    #[cfg(feature = "sqlite")]
    use tokio::time;

    use super::TaskExecutionServiceBuilder;
    use super::TaskServiceBuildError;
    #[cfg(feature = "sqlite")]
    use crate::handler::TaskContext;
    #[cfg(feature = "sqlite")]
    use crate::handler::TaskHandler;
    #[cfg(feature = "sqlite")]
    use crate::handler::TaskRunOutcome;
    #[cfg(feature = "sqlite")]
    use crate::model::TaskId;
    #[cfg(feature = "sqlite")]
    use crate::model::TaskOutput;
    #[cfg(feature = "sqlite")]
    use crate::model::TaskState;
    use crate::store::MemoryTaskStore;
    use crate::store::TaskStore;

    #[cfg(feature = "sqlite")]
    struct Echo;

    #[cfg(feature = "sqlite")]
    impl TaskHandler for Echo {
        fn descriptor(&self) -> crate::handler::TaskHandlerDescriptor {
            crate::handler::TaskHandlerDescriptor {
                task_type: "builder-test".into(),
                version: "1".into(),
            }
        }

        fn run<'a>(
            &'a self,
            _payload: &'a [u8],
            _context: TaskContext,
        ) -> crate::store::TaskFuture<'a, crate::handler::TaskRunResult> {
            Box::pin(async { Ok(TaskRunOutcome::Succeeded(TaskOutput::default())) })
        }
    }

    #[tokio_crate::test]
    async fn test_builder_rejects_overflowing_recovery_capacity_configuration() {
        let result = TaskExecutionServiceBuilder::in_memory()
            .queue_capacity(usize::MAX)
            .max_running_tasks(NonZeroUsize::MIN)
            .build()
            .await;
        assert!(matches!(result, Err(TaskServiceBuildError::InvalidConfiguration(_))));
    }

    #[tokio_crate::test]
    async fn test_owner_guard_without_lease_is_a_noop() {
        let store: Arc<dyn TaskStore> = Arc::new(MemoryTaskStore::new(4));
        let mut guard = super::OwnerGuard::new(store, None);
        guard.release().await.expect("no lease needs no release");
        assert!(guard.transfer().is_none());
    }

    #[tokio_crate::test]
    async fn test_owner_guard_preserves_release_failure() {
        let store: Arc<dyn TaskStore> = Arc::new(MemoryTaskStore::new(4));
        let mut guard = super::OwnerGuard::new(store, Some(crate::model::OwnerEpoch(1)));
        assert!(matches!(
            guard.release().await,
            Err(crate::store::StoreError::UnsupportedCapability)
        ));
        assert!(guard.transfer().is_none());
    }

    #[cfg(feature = "sqlite")]
    #[tokio_crate::test]
    async fn test_owner_guard_drop_releases_sqlite_lease() {
        let path = std::env::temp_dir().join(format!("qubit-task-owner-guard-{}.sqlite", TaskId::generate()));
        let store = Arc::new(crate::store::SqliteTaskStore::open(&path).expect("SQLite store opens"));
        let epoch = store.acquire_owner().await.expect("store acquires ownership");
        drop(super::OwnerGuard::new(
            Arc::clone(&store) as Arc<dyn TaskStore>,
            Some(epoch),
        ));

        let replacement = time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                if let Ok(store) = crate::store::SqliteTaskStore::open(&path) {
                    break store;
                }
                time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("dropping the guard releases SQLite ownership");
        drop(replacement);
        drop(store);
        for suffix in ["", "-shm", "-wal"] {
            let path = std::path::PathBuf::from(format!("{}{suffix}", path.display()));
            let _ = std::fs::remove_file(path);
        }
    }

    #[cfg(feature = "sqlite")]
    #[tokio_crate::test]
    async fn test_sqlite_builder_recovers_unfinished_records() {
        use crate::model::AcceptOutcome;
        use crate::store::SqliteTaskStore;
        use crate::store::TaskStore;

        let path = std::env::temp_dir().join(format!("qubit-task-unit-recovery-{}.sqlite", TaskId::generate()));
        let store = SqliteTaskStore::open(&path).unwrap();
        let id = TaskId::generate();
        let request = crate::model::TaskRequest::new("builder-test", "1", Vec::new());
        assert!(
            store
                .get_by_idempotency_key("builder-test-key")
                .await
                .unwrap()
                .is_none()
        );
        assert!(matches!(
            store.accept(id, request).await.unwrap(),
            AcceptOutcome::Accepted(_)
        ));
        assert_eq!(
            store
                .list(crate::model::TaskQuery::default())
                .await
                .unwrap()
                .records
                .len(),
            1
        );
        drop(store);

        let service = TaskExecutionServiceBuilder::recoverable_sqlite(&path)
            .unwrap()
            .register_handler(Arc::new(Echo))
            .unwrap()
            .build()
            .await
            .unwrap();
        assert!(matches!(service.wait(id).await.unwrap().state, TaskState::Succeeded));
        service.shutdown().await.unwrap();
        drop(service);
        let _ = std::fs::remove_file(&path);
        let mut lock_path = path.as_os_str().to_owned();
        lock_path.push(".owner.lock");
        let _ = std::fs::remove_file(std::path::PathBuf::from(lock_path));
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }

    #[test]
    fn test_build_errors_preserve_worker_and_cleanup_diagnostics() {
        let worker_error = TaskServiceBuildError::WorkerStopped;
        assert!(worker_error.to_string().contains("worker stopped"));

        let cleanup_error = TaskServiceBuildError::CleanupFailed {
            primary: Box::new(TaskServiceBuildError::InvalidRecoveryPage("bad cursor".into())),
            cleanup: crate::store::StoreError::Failure("release failed".into()),
        };
        let message = cleanup_error.to_string();
        assert!(message.contains("bad cursor"));
        assert!(message.contains("release failed"));
        assert!(std::error::Error::source(&cleanup_error).is_some());
    }
}
