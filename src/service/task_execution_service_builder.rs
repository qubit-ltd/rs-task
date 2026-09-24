// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
#[cfg(feature = "event-bus")]
use std::num::NonZeroUsize;
use std::sync::Arc;

use super::admission_gate::AdmissionGate;
#[cfg(feature = "event-bus")]
use super::task_event_publisher::TaskEventPublisher;
use super::task_execution_service::ServiceCore;
use super::task_execution_service::TaskExecutionService;
use crate::engine::LocalTaskExecutionEngine;
use crate::engine::TaskExecutionEngine;
use crate::handler::TaskHandler;
use crate::handler::TaskHandlerRegistry;
use crate::model::ResourceCapacity;
use crate::model::TaskState;
use crate::scheduling::FairFifoPolicy;
use crate::scheduling::QueuedTask;
use crate::scheduling::SchedulingPolicy;
use crate::store::MemoryTaskStore;
use crate::store::StoreError;
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
    Store(#[from] StoreError),
    /// Two handlers claimed the same task type and version.
    #[error("{0}")]
    HandlerConflict(String),
    /// SQLite support is disabled for this crate build.
    #[error("SQLite support requires the `sqlite` feature")]
    SqliteFeatureDisabled,
    /// The dedicated lifecycle event publisher thread could not start.
    #[cfg(feature = "event-bus")]
    #[error("failed to start task event publisher thread: {0}")]
    EventPublisherThread(#[source] std::io::Error),
}

/// Explicit component assembly and resource policy for one task service.
pub struct TaskExecutionServiceBuilder {
    store: Option<Arc<dyn TaskStore>>,
    engine: Option<Arc<dyn TaskExecutionEngine>>,
    policy: Option<Arc<dyn SchedulingPolicy>>,
    handlers: TaskHandlerRegistry,
    capacity: ResourceCapacity,
    queue_capacity: usize,
    scan_budget: usize,
    max_attempts: u32,
    require_recovery: bool,
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
            scan_budget: 128,
            max_attempts: 3,
            require_recovery: false,
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

    /// Fails construction unless storage declares restart recovery.
    #[must_use]
    pub fn require_recovery(mut self, required: bool) -> Self {
        self.require_recovery = required;
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
        let owner = if store_capabilities.restart_recovery {
            Some(store.acquire_owner().await?)
        } else {
            None
        };
        let recovered_result = async {
            let mut recovered = Vec::new();
            if store_capabilities.restart_recovery {
                let mut cursor = None;
                loop {
                    let page = store.scan_unfinished(cursor).await?;
                    recovered.extend(page.tasks);
                    cursor = page.next;
                    if cursor.is_none() {
                        break;
                    }
                }
            }
            restore_tasks(&store, &self.handlers, recovered).await
        }
        .await;
        let queue = match recovered_result {
            Ok(queue) => queue,
            Err(error) => {
                if let Some(epoch) = owner {
                    let _ = store.release_owner(epoch).await;
                }
                return Err(error.into());
            }
        };
        let queue_count = queue.len();
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
            handlers: self.handlers,
            queue_capacity: self.queue_capacity,
            scan_budget: self.scan_budget,
            max_attempts: self.max_attempts,
            queue: parking_lot::Mutex::new(queue),
            queue_count: std::sync::atomic::AtomicUsize::new(queue_count),
            local_handlers: parking_lot::Mutex::new(Default::default()),
            local_finalizations: parking_lot::Mutex::new(Default::default()),
            cancellations: parking_lot::Mutex::new(Default::default()),
            changed: tokio::sync::Notify::new(),
            admission: AdmissionGate::new(),
            owner,
            store_fault: parking_lot::Mutex::new(None),
            #[cfg(feature = "event-bus")]
            event_bus,
        };
        let service = TaskExecutionService::start(core);
        Ok(service)
    }
}

async fn restore_tasks(
    store: &Arc<dyn TaskStore>,
    handlers: &TaskHandlerRegistry,
    recovered: Vec<crate::model::StoredTask>,
) -> Result<std::collections::VecDeque<QueuedTask>, StoreError> {
    let mut queue = std::collections::VecDeque::new();
    for stored in recovered {
        let mut record = stored.record;
        if matches!(record.state, TaskState::Running) {
            record = store
                .transition(crate::model::TransitionCommand {
                    id: record.id,
                    expected_version: record.state_version,
                    expected_attempt: record.attempt,
                    state: TaskState::Queued,
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
                        output: None,
                        assigned_resources: Vec::new(),
                        cancel_requested: false,
                    })
                    .await?;
            } else {
                queue.push_back(QueuedTask {
                    id: record.id,
                    request: record.request.clone(),
                    bypasses: 0,
                });
            }
        }
    }
    Ok(queue)
}
