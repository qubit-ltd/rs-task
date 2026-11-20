// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use qubit_codec::ValueBytesCodecRegistry;
#[cfg(feature = "event-bus")]
use qubit_event_bus::AsyncEventBus;
use qubit_id::Id;
use qubit_id::IdGenerationError;
use qubit_id::IdGenerator;

use super::typed_task_execution_service::TypedServiceOptions;
use super::typed_task_execution_service::TypedTaskExecutionService;
use crate::engine::LocalTaskExecutionEngine;
use crate::handler::HandlerRegistrationError;
use crate::handler::typed::HandlerOptions;
use crate::handler::typed::TaskHandlerDescriptor;
use crate::handler::typed::TypedTaskHandler;
use crate::handler::typed::TypedTaskHandlerRegistry;
use crate::model::ResourceCapacity;
use crate::service::RetryPolicy;
use crate::service::TaskServiceError;
use crate::store::MemoryTaskStore;
use crate::store::TaskStore;
use qubit_model_id::HasModelId;

struct InMemoryIdGenerator {
    sequence: AtomicU64,
}

impl InMemoryIdGenerator {
    fn new() -> Self {
        Self {
            sequence: AtomicU64::new(0),
        }
    }
}

impl IdGenerator<Id, IdGenerationError> for InMemoryIdGenerator {
    fn generate(&self) -> Result<Id, IdGenerationError> {
        self.sequence
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |sequence| {
                sequence.checked_add(1)
            })
            .map(|previous| Id::new(previous + 1))
            .map_err(|sequence| IdGenerationError::SequenceOverflow {
                sequence,
                max: u64::MAX - 1,
            })
    }
}

/// Builder for the typed task service.
///
/// The builder owns the dependencies and configuration used to create the
/// service. Its configuration methods consume and return the builder so they
/// can be chained before `build` starts service recovery.
///
/// Synchronous ID generation during submission runs on Tokio's blocking pool.
/// A cancelled submission does not stop a generator call that has already
/// started, so it may consume an ID without accepting a task. Uncancelled
/// submissions remain inside the service's shutdown admission boundary.
/// Custom generators should return eventually and must not depend on
/// executor-local state. Runtime shutdown can wait for an active blocking call.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
///
/// use qubit_codec::ValueBytesCodecRegistry;
/// use qubit_id::Id;
/// use qubit_id::IdGenerationError;
/// use qubit_id::IdGenerator;
/// use qubit_task::TaskExecutionServiceBuilder;
/// use qubit_task::service::TaskServiceError;
/// use qubit_task::TaskStore;
///
/// async fn start_service(
///     store: Arc<dyn TaskStore>,
///     codecs: Arc<ValueBytesCodecRegistry>,
///     id_generator: Arc<dyn IdGenerator<Id, IdGenerationError>>,
/// ) -> Result<(), TaskServiceError> {
///     let service = TaskExecutionServiceBuilder::new(store, codecs, id_generator)
///         .build()
///         .await?;
///     drop(service);
///     Ok(())
/// }
/// ```
pub struct TypedTaskExecutionServiceBuilder {
    /// Persistent task storage used by the service.
    store: Arc<dyn TaskStore>,
    /// Registry used to encode and decode task payloads.
    codecs: Arc<ValueBytesCodecRegistry>,
    /// Generator used to assign IDs to newly created tasks.
    id_generator: Arc<dyn IdGenerator<Id, IdGenerationError>>,
    /// Local CPU, GPU, memory, and disk reservation limits.
    capacity: ResourceCapacity,
    /// Handlers available to execute typed tasks.
    handlers: TypedTaskHandlerRegistry,
    /// Maximum number of handlers allowed to run concurrently.
    max_running_tasks: usize,
    /// Maximum number of queued summaries inspected in one scheduler scan.
    scan_page_size: usize,
    /// Maximum number of newer tasks allowed to bypass a resource-blocked task.
    max_resource_bypasses: usize,
    /// Maximum attempts made for a retryable handler failure.
    max_attempts: u32,
    /// Delay policy applied to persisted retries.
    retry_policy: RetryPolicy,
    /// Optional bus used to publish durable task lifecycle events.
    #[cfg(feature = "event-bus")]
    event_bus: Option<Arc<AsyncEventBus>>,
    /// Maximum time spent draining notifications during shutdown.
    #[cfg(feature = "event-bus")]
    notification_shutdown_timeout: std::time::Duration,
}

#[cfg(test)]
mod id_generator_tests {
    use super::InMemoryIdGenerator;
    use qubit_id::IdGenerator;

    #[test]
    fn sequence_never_wraps_or_repeats_after_overflow() {
        let generator = InMemoryIdGenerator {
            sequence: std::sync::atomic::AtomicU64::new(u64::MAX - 1),
        };
        assert_eq!(generator.generate().unwrap().value(), u64::MAX);
        assert!(matches!(
            generator.generate(),
            Err(qubit_id::IdGenerationError::SequenceOverflow { .. })
        ));
        assert!(matches!(
            generator.generate(),
            Err(qubit_id::IdGenerationError::SequenceOverflow { .. })
        ));
    }
}

impl TypedTaskExecutionServiceBuilder {
    /// Creates an in-memory builder with an independent monotonic ID sequence.
    #[must_use]
    pub fn in_memory(history_capacity: usize, codecs: ValueBytesCodecRegistry) -> Self {
        Self::new(
            Arc::new(MemoryTaskStore::new(history_capacity)),
            Arc::new(codecs),
            Arc::new(InMemoryIdGenerator::new()),
        )
    }

    /// Creates a builder with a store, byte codec registry, and ID generator.
    ///
    /// Snowflake generators used across processes must have distinct node IDs
    /// and a shared, appropriate clock configuration. The generator must keep
    /// task IDs unique across process restarts; SQLite rejects reuse while a
    /// terminal checkpoint for that ID remains.
    ///
    /// # Parameters
    ///
    /// * `store` - Durable task store used for acceptance and lifecycle state.
    /// * `codecs` - Registry for encoding and decoding typed request values.
    /// * `id_generator` - Generator used to assign task identifiers uniquely
    ///   across process restarts.
    #[must_use = "the builder must be configured and built to create a service"]
    pub fn new(
        store: Arc<dyn TaskStore>,
        codecs: Arc<ValueBytesCodecRegistry>,
        id_generator: Arc<dyn IdGenerator<Id, IdGenerationError>>,
    ) -> Self {
        let cpu_slots =
            std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get) as u32;
        Self {
            store,
            codecs,
            id_generator,
            capacity: ResourceCapacity {
                cpu_slots,
                ..ResourceCapacity::default()
            },
            handlers: TypedTaskHandlerRegistry::new(),
            max_running_tasks: std::thread::available_parallelism()
                .map_or(1, std::num::NonZeroUsize::get),
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
    ///
    /// # Parameters
    ///
    /// * `capacity` - Resource capacity available to local task attempts.
    #[must_use = "the returned builder contains the updated capacity"]
    #[inline]
    pub fn capacity(mut self, capacity: ResourceCapacity) -> Self {
        self.capacity = capacity;
        self
    }

    /// Sets the maximum number of concurrently running handler tasks.
    ///
    /// # Parameters
    ///
    /// * `limit` - Non-zero maximum number of concurrent handlers.
    #[must_use = "the returned builder contains the updated limit"]
    #[inline]
    pub fn max_running_tasks(mut self, limit: std::num::NonZeroUsize) -> Self {
        self.max_running_tasks = limit.get();
        self
    }

    /// Sets the scheduler's bounded queued-summary scan page size (1–256).
    /// Values above 256 are capped; the non-zero argument prevents an empty
    /// scan.
    ///
    /// # Parameters
    ///
    /// * `limit` - Non-zero queued-summary scan page size; values above 256 are
    ///   capped.
    #[must_use = "the returned builder contains the updated page size"]
    #[inline]
    pub fn scan_page_size(mut self, limit: std::num::NonZeroUsize) -> Self {
        self.scan_page_size = limit.get().min(256);
        self
    }

    /// Limits successful newer starts while an older ready task awaits
    /// resources. The default is 32. Once reached, the scheduler gives the
    /// older task priority until it starts or leaves the ready queue.
    ///
    /// # Parameters
    ///
    /// * `limit` - Non-zero count of successful newer starts allowed to bypass
    ///   an older resource-blocked task.
    #[must_use = "the returned builder contains the updated bypass limit"]
    #[inline]
    pub fn max_resource_bypasses(mut self, limit: std::num::NonZeroUsize) -> Self {
        self.max_resource_bypasses = limit.get();
        self
    }

    /// Sets the maximum number of attempts for a retryable handler failure.
    ///
    /// # Parameters
    ///
    /// * `limit` - Non-zero maximum attempt count.
    #[must_use = "the returned builder contains the updated attempt limit"]
    #[inline]
    pub fn max_attempts(mut self, limit: std::num::NonZeroU32) -> Self {
        self.max_attempts = limit.get();
        self
    }

    /// Sets the exponential delay used for persisted retries.
    ///
    /// # Parameters
    ///
    /// * `policy` - Retry delay policy persisted with retryable failures.
    #[must_use = "the returned builder contains the updated retry policy"]
    #[inline]
    pub fn retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.retry_policy = policy;
        self
    }

    /// Enables durable lifecycle publication to the `task.lifecycle` topic.
    /// The store must support a persistent outbox; register a `TaskEvent` codec
    /// in the supplied bus for encoded providers. Failed publications remain
    /// durable.
    ///
    /// # Parameters
    ///
    /// * `bus` - Event bus used to publish committed lifecycle events.
    #[cfg(feature = "event-bus")]
    #[must_use = "the returned builder has lifecycle publication enabled"]
    #[inline]
    pub fn event_bus(mut self, bus: Arc<AsyncEventBus>) -> Self {
        self.event_bus = Some(bus);
        self
    }

    /// Sets the maximum notification drain duration during shutdown (default 5
    /// seconds). Expiry leaves pending events durable and reports
    /// `NotificationClose`.
    ///
    /// # Parameters
    ///
    /// * `timeout` - Maximum duration allowed to drain notifications at
    ///   shutdown.
    #[cfg(feature = "event-bus")]
    #[must_use = "the returned builder contains the updated timeout"]
    #[inline]
    pub fn notification_shutdown_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.notification_shutdown_timeout = timeout;
        self
    }

    /// Registers a typed handler with default cancellation options.
    pub fn register_handler<T, H>(
        &mut self,
        descriptor: TaskHandlerDescriptor,
        handler: Arc<H>,
    ) -> Result<(), HandlerRegistrationError>
    where
        T: HasModelId + Send + Sync + 'static,
        H: TypedTaskHandler<T>,
    {
        self.handlers.register::<T, H>(descriptor, handler)
    }

    /// Registers a typed handler with explicit options.
    pub fn register_handler_with_options<T, H>(
        &mut self,
        descriptor: TaskHandlerDescriptor,
        handler: Arc<H>,
        options: HandlerOptions,
    ) -> Result<(), HandlerRegistrationError>
    where
        T: HasModelId + Send + Sync + 'static,
        H: TypedTaskHandler<T>,
    {
        self.handlers
            .register_with_options::<T, H>(descriptor, handler, options)
    }

    /// Registers an async function handler with default options.
    pub fn register_handler_fn<T, F, Fut>(
        &mut self,
        descriptor: TaskHandlerDescriptor,
        handler: F,
    ) -> Result<(), HandlerRegistrationError>
    where
        T: HasModelId + Send + Sync + 'static,
        F: Fn(T, crate::handler::TaskContext) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = crate::handler::TaskRunResult> + Send + 'static,
    {
        self.handlers.register_handler_fn(descriptor, handler)
    }

    /// Registers an async function handler with explicit options.
    pub fn register_handler_fn_with_options<T, F, Fut>(
        &mut self,
        descriptor: TaskHandlerDescriptor,
        handler: F,
        options: HandlerOptions,
    ) -> Result<(), HandlerRegistrationError>
    where
        T: HasModelId + Send + Sync + 'static,
        F: Fn(T, crate::handler::TaskContext) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = crate::handler::TaskRunResult> + Send + 'static,
    {
        self.handlers
            .register_handler_fn_with_options(descriptor, handler, options)
    }

    /// Removes a registered handler before the service is built.
    pub fn unregister_handler(&mut self, kind: &str) -> bool {
        self.handlers.unregister(kind)
    }

    /// Builds the service and begins recovering encoded queued tasks.
    ///
    /// # Errors
    ///
    /// Returns an error if service initialization or recovery fails.
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
