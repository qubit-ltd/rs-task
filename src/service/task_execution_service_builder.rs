// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::num::NonZeroUsize;
use std::sync::Arc;

use super::admission_budget::AdmissionBudget;
use super::admission_gate::AdmissionGate;
use super::retry_policy::RetryPolicy;
use super::scheduler_queue::SchedulerQueue;
#[cfg(feature = "event-bus")]
use super::task_event_publisher::TaskEventPublisher;
use super::task_execution_service::ServiceCore;
use super::task_execution_service::TaskExecutionService;
use crate::engine::LocalTaskExecutionEngine;
use crate::engine::TaskExecutionEngine;
use crate::handler::TaskHandler;
use crate::handler::TaskHandlerRegistry;
use crate::model::ResourceCapacity;
use crate::model::StoredTask;
use crate::model::TaskId;
use crate::model::TaskState;
use crate::scheduling::FairFifoPolicy;
use crate::scheduling::QueuedTask;
use crate::scheduling::SchedulingPolicy;
use crate::store::MemoryTaskStore;
use crate::store::TaskStore;

/// Service construction error, including unsupported or unavailable recovery.
#[derive(Debug, thiserror::Error)]
pub enum TaskServiceBuildError {
    /// A generic builder did not select a store explicitly.
    #[error("a task store must be selected explicitly")]
    MissingStore,
    /// Recovery was required but the selected store does not support it.
    #[error("restart recovery was required but the selected store does not support it")]
    RecoveryRequired,
    /// Store initialization or recovery scan failed.
    #[error(transparent)]
    Store(#[from] crate::store::StoreError),
    /// Two handlers claimed the same task type and version.
    #[error("{0}")]
    HandlerConflict(String),
    /// SQLite support is disabled for this crate build.
    #[error("SQLite support requires the `sqlite` feature")]
    SqliteFeatureDisabled,
    /// Existing unfinished work is larger than the configured recovery bound.
    #[error("unfinished task count exceeds recovery capacity {limit}")]
    RecoveryCapacityExceeded {
        /// Maximum unfinished task count accepted by this configuration.
        limit: usize,
    },
    /// A task store returned an invalid recovery page.
    #[error("invalid recovery page: {0}")]
    InvalidRecoveryPage(String),
    /// The selected queue and running capacities overflow the supported range.
    #[error("invalid service configuration: {0}")]
    InvalidConfiguration(String),
    /// The dedicated lifecycle event publisher thread could not start.
    #[cfg(feature = "event-bus")]
    #[error("failed to start task event publisher thread: {0}")]
    EventPublisherThread(#[source] std::io::Error),
}

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
/// let service = qubit_task::TaskExecutionServiceBuilder::in_memory().build().await?;
/// assert!(!service.capabilities().store.restart_recovery);
/// service.shutdown().await?;
/// # Ok(())
/// # }
/// ```
pub struct TaskExecutionServiceBuilder {
    store: Option<Arc<dyn TaskStore>>,
    engine: Option<Arc<dyn TaskExecutionEngine>>,
    policy: Option<Arc<dyn SchedulingPolicy>>,
    handlers: TaskHandlerRegistry,
    capacity: ResourceCapacity,
    queue_capacity: usize,
    max_inflight_payload_bytes: NonZeroUsize,
    max_inflight_submissions: NonZeroUsize,
    max_running_tasks: NonZeroUsize,
    scan_budget: usize,
    max_attempts: u32,
    retry_policy: RetryPolicy,
    require_recovery: bool,
    runtime_handle: Option<tokio::runtime::Handle>,
    #[cfg(feature = "event-bus")]
    event_bus: Option<qubit_event_bus::EventBus>,
    #[cfg(feature = "event-bus")]
    event_bus_buffer_capacity: NonZeroUsize,
}

impl Default for TaskExecutionServiceBuilder {
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
            max_inflight_submissions: NonZeroUsize::new(64).expect("default submission limit is nonzero"),
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
        }
    }
}

impl TaskExecutionServiceBuilder {
    /// Selects explicit volatile storage with standard local components.
    #[must_use]
    pub fn in_memory() -> Self {
        Self::default().store(Arc::new(MemoryTaskStore::new(1024)))
    }

    /// Selects volatile storage with an explicit retained payload budget.
    #[must_use]
    pub fn in_memory_with_payload_budget(limit: NonZeroUsize) -> Self {
        Self::default().store(Arc::new(MemoryTaskStore::with_payload_budget(1024, limit)))
    }

    /// Selects a restart-recoverable SQLite store when enabled.
    #[cfg(feature = "sqlite")]
    pub fn recoverable_sqlite(path: impl AsRef<std::path::Path>) -> Result<Self, TaskServiceBuildError> {
        let store = crate::store::SqliteTaskStore::open(path)?;
        Ok(Self::default().store(Arc::new(store)).require_recovery(true))
    }

    /// Returns a builder that refuses construction if no store was selected.
    #[must_use]
    pub fn from_components(
        store: Arc<dyn TaskStore>,
        engine: Arc<dyn TaskExecutionEngine>,
        policy: Arc<dyn SchedulingPolicy>,
    ) -> Self {
        Self::default().store(store).engine(engine).policy(policy)
    }

    /// Selects the authoritative task store.
    #[must_use]
    pub fn store(mut self, store: Arc<dyn TaskStore>) -> Self {
        self.store = Some(store);
        self
    }

    /// Selects the resource execution engine.
    #[must_use]
    pub fn engine(mut self, engine: Arc<dyn TaskExecutionEngine>) -> Self {
        self.engine = Some(engine);
        self
    }

    /// Selects the task ordering policy.
    #[must_use]
    pub fn policy(mut self, policy: Arc<dyn SchedulingPolicy>) -> Self {
        self.policy = Some(policy);
        self
    }

    /// Adds one exact-version business handler.
    pub fn register_handler(mut self, handler: Arc<dyn TaskHandler>) -> Result<Self, TaskServiceBuildError> {
        self.handlers
            .register(handler)
            .map_err(|error| TaskServiceBuildError::HandlerConflict(error.to_string()))?;
        Ok(self)
    }

    /// Replaces the handler registry, for example with handlers created by an
    /// SPI registry.
    #[must_use]
    pub fn handlers(mut self, handlers: TaskHandlerRegistry) -> Self {
        self.handlers = handlers;
        self
    }

    /// Overrides local CPU, GPU, and custom resource capacity.
    #[must_use]
    pub fn capacity(mut self, capacity: ResourceCapacity) -> Self {
        self.capacity = capacity;
        self
    }

    /// Sets the maximum number of waiting tasks accepted by the service.
    #[must_use]
    pub fn queue_capacity(mut self, capacity: usize) -> Self {
        self.queue_capacity = capacity;
        self
    }

    /// Sets the maximum payload bytes retained by in-flight admission workers.
    #[must_use]
    pub fn max_inflight_payload_bytes(mut self, limit: NonZeroUsize) -> Self {
        self.max_inflight_payload_bytes = limit;
        self
    }

    /// Sets the maximum number of in-flight admission workers.
    #[must_use]
    pub fn max_inflight_submissions(mut self, limit: NonZeroUsize) -> Self {
        self.max_inflight_submissions = limit;
        self
    }

    /// Sets the maximum number of task attempts that may run simultaneously.
    #[must_use]
    pub fn max_running_tasks(mut self, limit: NonZeroUsize) -> Self {
        self.max_running_tasks = limit;
        self
    }

    /// Sets the maximum number of candidates inspected in each scheduler cycle.
    #[must_use]
    pub fn scan_budget(mut self, budget: usize) -> Self {
        self.scan_budget = budget.max(1);
        self
    }

    /// Sets the maximum execution attempts for a retryable handler result.
    #[must_use]
    pub fn max_attempts(mut self, attempts: u32) -> Self {
        self.max_attempts = attempts.max(1);
        self
    }

    /// Sets the exponential delay applied between retryable attempts.
    #[must_use]
    pub fn retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.retry_policy = policy;
        self
    }

    /// Fails construction unless storage declares restart recovery.
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
    #[must_use]
    pub fn runtime_handle(mut self, handle: tokio::runtime::Handle) -> Self {
        self.runtime_handle = Some(handle);
        self
    }

    /// Injects the concrete event bus facade for optional status notifications.
    #[cfg(feature = "event-bus")]
    #[must_use]
    pub fn event_bus(mut self, event_bus: qubit_event_bus::EventBus) -> Self {
        self.event_bus = Some(event_bus);
        self
    }

    /// Sets the number of lifecycle notifications waiting behind the publisher.
    #[cfg(feature = "event-bus")]
    #[must_use]
    pub fn event_bus_buffer_capacity(mut self, capacity: NonZeroUsize) -> Self {
        self.event_bus_buffer_capacity = capacity;
        self
    }

    /// Builds one unified task service after validating and preparing recovery.
    pub async fn build(self) -> Result<TaskExecutionService, TaskServiceBuildError> {
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
        let recovered_result = async {
            if store_capabilities.restart_recovery {
                if store.has_unfinished_over_limit(recovery_limit).await? {
                    return Err(TaskServiceBuildError::RecoveryCapacityExceeded { limit: recovery_limit });
                }
                restore_tasks_paged(&store, &self.handlers, self.max_attempts, recovery_limit).await
            } else {
                Ok(std::collections::VecDeque::new())
            }
        }
        .await;
        let queue = match recovered_result {
            Ok(queue) => queue,
            Err(error) => {
                if let Some(epoch) = owner {
                    let _ = store.release_owner(epoch).await;
                }
                return Err(error);
            }
        };
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
            Some(bus) => match TaskEventPublisher::new(bus, self.event_bus_buffer_capacity) {
                Ok(publisher) => Some(publisher),
                Err(error) => {
                    if let Some(epoch) = owner {
                        let _ = store.release_owner(epoch).await;
                    }
                    return Err(TaskServiceBuildError::EventPublisherThread(error));
                }
            },
            None => None,
        };
        let core = ServiceCore {
            store,
            engine,
            policy,
            runtime_handle,
            handlers: self.handlers,
            queue_capacity: self.queue_capacity,
            running_slots: Arc::new(tokio::sync::Semaphore::new(self.max_running_tasks.get())),
            scan_budget: self.scan_budget,
            max_attempts: self.max_attempts,
            retry_policy: self.retry_policy,
            queue: parking_lot::Mutex::new(scheduler_queue),
            queue_count: std::sync::atomic::AtomicUsize::new(queue_count),
            local_handlers: parking_lot::Mutex::new(Default::default()),
            local_finalizations: parking_lot::Mutex::new(Default::default()),
            cancellations: parking_lot::Mutex::new(Default::default()),
            changed: tokio::sync::Notify::new(),
            wait_registry: Arc::new(super::task_wait_registry::TaskWaitRegistry::default()),
            transition_event_lock: tokio::sync::RwLock::new(()),
            admission: AdmissionGate::new(),
            admission_budget: Arc::new(AdmissionBudget::new(
                self.max_inflight_payload_bytes,
                self.max_inflight_submissions,
            )),
            owner,
            store_fault: parking_lot::Mutex::new(None),
            scheduler_fault: parking_lot::Mutex::new(None),
            attempts_in_flight: std::sync::atomic::AtomicUsize::new(0),
            attempts_changed: tokio::sync::Notify::new(),
            scheduler_finished: std::sync::atomic::AtomicBool::new(false),
            scheduler_finished_notify: tokio::sync::Notify::new(),
            #[cfg(feature = "event-bus")]
            event_bus,
        };
        let service = TaskExecutionService::start(core);
        Ok(service)
    }
}

const RECOVERY_PAGE_LIMIT: usize = 256;

fn validate_recovery_page(
    tasks: &[StoredTask],
    previous: Option<TaskId>,
    next: Option<TaskId>,
) -> Result<(), TaskServiceBuildError> {
    if tasks.len() > RECOVERY_PAGE_LIMIT {
        return Err(TaskServiceBuildError::InvalidRecoveryPage(format!(
            "page contains {} records; maximum is {RECOVERY_PAGE_LIMIT}",
            tasks.len()
        )));
    }
    if next.is_some() && tasks.is_empty() {
        return Err(TaskServiceBuildError::InvalidRecoveryPage(
            "empty page returned a next cursor".into(),
        ));
    }
    if let Some(next) = next
        && previous.is_some_and(|previous| next <= previous)
    {
        return Err(TaskServiceBuildError::InvalidRecoveryPage(
            "cursor did not advance".into(),
        ));
    }
    Ok(())
}

/// Resets interrupted attempts and queues recoverable tasks with available
/// handlers, retaining only one store page at a time.
async fn restore_tasks_paged(
    store: &Arc<dyn TaskStore>,
    handlers: &TaskHandlerRegistry,
    max_attempts: u32,
    limit: usize,
) -> Result<std::collections::VecDeque<QueuedTask>, TaskServiceBuildError> {
    let mut queue = std::collections::VecDeque::new();
    let mut cursor = None;
    let mut count = 0_usize;
    loop {
        let page = store.scan_unfinished(cursor).await?;
        validate_recovery_page(&page.tasks, cursor, page.next)?;
        count = count.checked_add(page.tasks.len()).ok_or_else(|| {
            TaskServiceBuildError::InvalidConfiguration("unfinished task count overflows usize".into())
        })?;
        if count > limit {
            return Err(TaskServiceBuildError::RecoveryCapacityExceeded { limit });
        }
        for stored in page.tasks {
            let mut record = stored.record.summary();
            if matches!(record.state, TaskState::Queued | TaskState::Running) && record.attempt >= max_attempts {
                store
                    .transition(crate::model::TransitionCommand {
                        id: record.id,
                        expected_version: record.state_version,
                        expected_attempt: record.attempt,
                        state: TaskState::Blocked {
                            reason: format!(
                                "retry limit reached during recovery: {}/{} attempts used",
                                record.attempt, max_attempts
                            ),
                        },
                        retry_not_before_ms: None,
                        output: None,
                        assigned_resources: Vec::new(),
                        cancel_requested: record.cancel_requested,
                    })
                    .await?;
                continue;
            }
            if matches!(record.state, TaskState::Running) {
                record = store
                    .transition(crate::model::TransitionCommand {
                        id: record.id,
                        expected_version: record.state_version,
                        expected_attempt: record.attempt,
                        state: TaskState::Queued,
                        retry_not_before_ms: None,
                        output: None,
                        assigned_resources: Vec::new(),
                        cancel_requested: false,
                    })
                    .await?;
            }
            if matches!(record.state, TaskState::Queued) {
                if handlers
                    .resolve(&record.request.task_type, &record.request.handler_version)
                    .is_none()
                {
                    store
                        .transition(crate::model::TransitionCommand {
                            id: record.id,
                            expected_version: record.state_version,
                            expected_attempt: record.attempt,
                            state: TaskState::Blocked {
                                reason: format!(
                                    "missing handler {}@{} during recovery",
                                    record.request.task_type, record.request.handler_version
                                ),
                            },
                            retry_not_before_ms: None,
                            output: None,
                            assigned_resources: Vec::new(),
                            cancel_requested: false,
                        })
                        .await?;
                } else {
                    queue.push_back(QueuedTask {
                        id: record.id,
                        resources: record.request.resources.clone(),
                        retry_not_before_ms: record.retry_not_before_ms,
                        bypasses: 0,
                    });
                }
            }
        }
        cursor = page.next;
        if cursor.is_none() {
            break;
        }
    }
    Ok(queue)
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use super::TaskExecutionService;
    use super::TaskExecutionServiceBuilder;
    use super::TaskServiceBuildError;
    use crate::engine::LocalTaskExecutionEngine;
    use crate::handler::TaskContext;
    use crate::handler::TaskHandler;
    use crate::handler::TaskHandlerRegistry;
    use crate::handler::TaskRunOutcome;
    use crate::model::ResourceCapacity;
    use crate::model::TaskId;
    use crate::model::TaskOutput;
    use crate::model::TaskRunError;
    use crate::model::TaskState;
    use crate::scheduling::FairFifoPolicy;
    use crate::store::MemoryTaskStore;

    struct Echo;

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

    struct RetryOnce(AtomicUsize);

    impl TaskHandler for RetryOnce {
        fn descriptor(&self) -> crate::handler::TaskHandlerDescriptor {
            crate::handler::TaskHandlerDescriptor {
                task_type: "retry-once".into(),
                version: "1".into(),
            }
        }

        fn run<'a>(
            &'a self,
            _payload: &'a [u8],
            _context: TaskContext,
        ) -> crate::store::TaskFuture<'a, crate::handler::TaskRunResult> {
            Box::pin(async move {
                if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
                    Err(TaskRunError {
                        category: "test".into(),
                        message: "retry once".into(),
                        retryable: true,
                    })
                } else {
                    Ok(TaskRunOutcome::Succeeded(TaskOutput::default()))
                }
            })
        }
    }

    #[tokio::test]
    async fn test_public_builder_and_service_lifecycle_contracts() {
        let mut registry = TaskHandlerRegistry::new();
        registry.register(Arc::new(Echo)).unwrap();
        registry.register(Arc::new(RetryOnce(AtomicUsize::new(0)))).unwrap();
        let capacity = ResourceCapacity {
            cpu_slots: 1,
            ..ResourceCapacity::default()
        };
        let builder = TaskExecutionServiceBuilder::from_components(
            Arc::new(MemoryTaskStore::new(16)),
            Arc::new(LocalTaskExecutionEngine::new(capacity.clone())),
            Arc::new(FairFifoPolicy::default()),
        )
        .register_handler(Arc::new(Echo))
        .unwrap()
        .handlers(registry)
        .capacity(capacity)
        .queue_capacity(8)
        .max_running_tasks(NonZeroUsize::new(2).unwrap())
        .scan_budget(8)
        .max_attempts(2)
        .require_recovery(false);
        #[cfg(feature = "event-bus")]
        let builder = builder
            .event_bus(
                qubit_event_bus::EventBus::local(qubit_event_bus::local::LocalEventBusConfig::default()).unwrap(),
            )
            .event_bus_buffer_capacity(NonZeroUsize::new(4).unwrap());
        let service = builder.build().await.unwrap();
        #[cfg(feature = "event-bus")]
        assert!(service.notification_stats().is_some());
        assert!(!service.capabilities().store.restart_recovery);
        assert_eq!(service.last_store_error(), None);
        assert!(service.get(TaskId::generate()).await.unwrap().is_none());

        let accepted = service
            .submit(
                crate::model::TaskRequest::new("builder-test", "1", Vec::new())
                    .with_idempotency_key("builder-test-submit"),
            )
            .await
            .unwrap();
        assert!(matches!(
            service.wait(accepted.id).await.unwrap().state,
            TaskState::Succeeded
        ));
        assert!(service.get(accepted.id).await.unwrap().is_some());
        assert_eq!(
            service
                .list(crate::model::TaskQuery::default())
                .await
                .unwrap()
                .records
                .len(),
            1
        );
        assert_eq!(service.stats().await.unwrap().terminal, 1);
        assert!(matches!(
            service.cancel(accepted.id).await.unwrap(),
            crate::service::CancelOutcome::AlreadyTerminal
        ));

        let local = service
            .submit_local(|_| crate::service::LocalTaskOutcome::<u8, String>::Succeeded {
                value: 7,
                summary: TaskOutput::default(),
            })
            .await
            .unwrap();
        assert_eq!(local.task_id(), service.get(local.task_id()).await.unwrap().unwrap().id);
        assert!(format!("{local:?}").contains("LocalTaskHandle"));
        assert_eq!(local.result().await.unwrap().unwrap(), 7);

        let retrying = service
            .submit(
                crate::model::TaskRequest::new("retry-once", "1", Vec::new())
                    .with_idempotency_key("builder-retry-once"),
            )
            .await
            .unwrap();
        let retried = service.wait(retrying.id).await.unwrap();
        assert_eq!(retried.attempt, 2);
        assert!(matches!(retried.state, TaskState::Succeeded));

        let blocked = service
            .submit(
                crate::model::TaskRequest::new("missing", "1", Vec::new())
                    .with_idempotency_key("builder-missing-handler"),
            )
            .await
            .unwrap();
        assert!(matches!(
            service.wait(blocked.id).await,
            Err(crate::service::TaskServiceError::Blocked)
        ));
        service.retry_blocked(blocked.id).await.unwrap();
        assert!(matches!(
            service.wait(blocked.id).await,
            Err(crate::service::TaskServiceError::Blocked)
        ));
        assert!(matches!(
            service.cancel(blocked.id).await.unwrap(),
            crate::service::CancelOutcome::CancelledBeforeStart
        ));
        assert!(matches!(
            service.cancel(TaskId::generate()).await,
            Err(crate::service::TaskServiceError::Store(
                crate::store::StoreError::NotFound
            ))
        ));
        service.shutdown().await.unwrap();

        let memory_service = TaskExecutionService::in_memory().await.unwrap();
        memory_service.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_builder_rejects_overflowing_recovery_capacity_configuration() {
        let result = TaskExecutionServiceBuilder::in_memory()
            .queue_capacity(usize::MAX)
            .max_running_tasks(NonZeroUsize::MIN)
            .build()
            .await;
        assert!(matches!(result, Err(TaskServiceBuildError::InvalidConfiguration(_))));
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
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
        let _ = std::fs::remove_file(path.with_extension("owner.lock"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }
}
