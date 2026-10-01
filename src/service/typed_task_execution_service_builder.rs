use std::sync::Arc;

use qubit_codec::ValueBytesCodecRegistry;
use qubit_id::Id;
use qubit_id::IdGenerationError;
use qubit_id::IdGenerator;

use super::typed_task_execution_service::TypedTaskExecutionService;
use crate::engine::LocalTaskExecutionEngine;
use crate::handler::typed::TypedTaskHandlerRegistry;
use crate::model::ResourceCapacity;
use crate::service::TaskServiceError;
use crate::store::TaskStore;

/// Builder for the typed task service. ID generation is always explicit.
pub struct TypedTaskExecutionServiceBuilder {
    store: Arc<dyn TaskStore>,
    codecs: Arc<ValueBytesCodecRegistry>,
    id_generator: Arc<dyn IdGenerator<Id, IdGenerationError>>,
    capacity: ResourceCapacity,
    handlers: TypedTaskHandlerRegistry,
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
        }
    }

    /// Sets the local scheduler's CPU/GPU/memory/disk reservation capacity.
    pub fn capacity(mut self, capacity: ResourceCapacity) -> Self {
        self.capacity = capacity;
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
        )
        .await
    }
}
