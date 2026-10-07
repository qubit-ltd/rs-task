// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;

use qubit_codec::ValueBytesCodecRegistry;
#[cfg(feature = "event-bus")]
use qubit_event_bus::AsyncEventBus;
use qubit_id::Id;
use qubit_id::IdGenerationError;
use qubit_id::IdGenerator;

use super::typed_task_execution_service::TypedServiceOptions;
use super::typed_task_execution_service::TypedTaskExecutionService;
use crate::engine::LocalTaskExecutionEngine;
use crate::handler::typed::TypedTaskHandlerRegistry;
use crate::model::ResourceCapacity;
use crate::service::RetryPolicy;
use crate::service::TaskServiceError;
use crate::store::TaskStore;

/// Builder for the typed task service. ID generation is always explicit.
pub struct TypedTaskExecutionServiceBuilder {
    store: Arc<dyn TaskStore>,
    codecs: Arc<ValueBytesCodecRegistry>,
    id_generator: Arc<dyn IdGenerator<Id, IdGenerationError>>,
    capacity: ResourceCapacity,
    handlers: TypedTaskHandlerRegistry,
    max_running_tasks: usize,
    scan_page_size: usize,
    max_resource_bypasses: usize,
    max_attempts: u32,
    retry_policy: RetryPolicy,
    #[cfg(feature = "event-bus")]
    event_bus: Option<Arc<AsyncEventBus>>,
    #[cfg(feature = "event-bus")]
    notification_shutdown_timeout: std::time::Duration,
}

impl TypedTaskExecutionServiceBuilder {
    /// Creates a builder with a store, byte codec registry, and ID generator.
    ///
    /// Snowflake generators used across processes must have distinct node IDs
    /// and a shared, appropriate clock configuration.
    pub fn new(
        store: Arc<dyn TaskStore>,
        codecs: Arc<ValueBytesCodecRegistry>,
        id_generator: Arc<dyn IdGenerator<Id, IdGenerationError>>,
    ) -> Self {
        let cpu_slots = std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get) as u32;
        Self {
            store,
            codecs,
            id_generator,
            capacity: ResourceCapacity {
                cpu_slots,
                ..ResourceCapacity::default()
            },
            handlers: TypedTaskHandlerRegistry::new(),
            max_running_tasks: std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get),
            scan_page_size: 128,
            max_resource_bypasses: 32,
            max_attempts: 3,
            retry_policy: RetryPolicy::default(),
            #[cfg(feature = "event-bus")]
            event_bus: None,
            #[cfg(feature = "event-bus")]
            notification_shutdown_timeout: std::time::Duration::from_secs(5),
        }
    }

    /// Sets the local scheduler's CPU/GPU/memory/disk reservation capacity.
    pub fn capacity(mut self, capacity: ResourceCapacity) -> Self {
        self.capacity = capacity;
        self
    }

    /// Sets the maximum number of concurrently running handler tasks.
    pub fn max_running_tasks(mut self, limit: std::num::NonZeroUsize) -> Self {
        self.max_running_tasks = limit.get();
        self
    }

    /// Sets the scheduler's bounded queued-summary scan page size (1–256).
    pub fn scan_page_size(mut self, limit: std::num::NonZeroUsize) -> Self {
        self.scan_page_size = limit.get().min(256);
        self
    }

    /// Limits successful newer starts while an older ready task awaits
    /// resources. The default is 32. Once reached, the scheduler gives the
    /// older task priority until it starts or leaves the ready queue.
    pub fn max_resource_bypasses(mut self, limit: std::num::NonZeroUsize) -> Self {
        self.max_resource_bypasses = limit.get();
        self
    }

    /// Sets the maximum number of attempts for a retryable handler failure.
    pub fn max_attempts(mut self, limit: std::num::NonZeroU32) -> Self {
        self.max_attempts = limit.get();
        self
    }

    /// Sets the exponential delay used for persisted retries.
    pub fn retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.retry_policy = policy;
        self
    }

    /// Enables durable lifecycle publication to the `task.lifecycle` topic.
    /// The store must support a persistent outbox; register a `TaskEvent` codec
    /// in the supplied bus for encoded providers. Failed publications remain
    /// durable.
    #[cfg(feature = "event-bus")]
    pub fn event_bus(mut self, bus: Arc<AsyncEventBus>) -> Self {
        self.event_bus = Some(bus);
        self
    }

    /// Sets the maximum notification drain duration during shutdown (default 5
    /// seconds). Expiry leaves pending events durable and reports
    /// `NotificationClose`.
    #[cfg(feature = "event-bus")]
    pub fn notification_shutdown_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.notification_shutdown_timeout = timeout;
        self
    }

    /// Returns the handler registry being assembled.
    pub fn handlers_mut(&mut self) -> &mut TypedTaskHandlerRegistry {
        &mut self.handlers
    }

    /// Builds the service and begins recovering encoded queued tasks.
    pub async fn build(self) -> Result<TypedTaskExecutionService, TaskServiceError> {
        TypedTaskExecutionService::new(
            self.store,
            self.codecs,
            self.id_generator,
            Arc::new(LocalTaskExecutionEngine::new(self.capacity)),
            self.handlers,
            TypedServiceOptions {
                max_running_tasks: self.max_running_tasks,
                scan_page_size: self.scan_page_size,
                max_resource_bypasses: self.max_resource_bypasses,
                max_attempts: self.max_attempts,
                retry_policy: self.retry_policy,
                #[cfg(feature = "event-bus")]
                event_bus: self.event_bus,
                #[cfg(feature = "event-bus")]
                notification_shutdown_timeout: self.notification_shutdown_timeout,
            },
        )
        .await
    }
}
