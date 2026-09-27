// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;

use qubit_spi::ProviderDescriptor;
use qubit_spi::ProviderMetadata;
use qubit_spi::ServiceProvider;
use qubit_spi::error::ProviderFailure;
use qubit_spi::provider_descriptor;

use crate::spi::TaskStoreConfig;
use crate::spi::TaskStoreSpec;
use crate::store::MemoryTaskStore;
use crate::store::StoreError;
use crate::store::TaskStore;

/// Supplies the built-in volatile in-memory task store.
pub(crate) struct MemoryStoreProvider;

impl ProviderMetadata for MemoryStoreProvider {
    /// Returns the stable provider identifier used by application assembly.
    ///
    /// # Returns
    ///
    /// The in-memory store provider descriptor.
    fn descriptor(&self) -> ProviderDescriptor {
        provider_descriptor!("qubit.task.store.memory")
    }
}

impl ServiceProvider<TaskStoreSpec> for MemoryStoreProvider {
    /// Creates the selected store or rejects a configuration for another
    /// backend.
    ///
    /// # Parameters
    ///
    /// * `config` - Store settings selected by the application.
    ///
    /// # Returns
    ///
    /// A shared in-memory store for `Memory` settings.
    ///
    /// # Errors
    ///
    /// Returns an unsupported-provider failure for SQLite or custom settings.
    fn create_configured(&self, config: &TaskStoreConfig) -> Result<Arc<dyn TaskStore>, ProviderFailure<StoreError>> {
        match config {
            TaskStoreConfig::Memory { history_capacity } => Ok(Arc::new(MemoryTaskStore::new(*history_capacity))),
            #[cfg(feature = "sqlite")]
            TaskStoreConfig::Sqlite { .. } => Err(ProviderFailure::unsupported(StoreError::Failure(
                "SQLite provider selected for a memory configuration".into(),
            ))),
            TaskStoreConfig::Custom(_) => Err(ProviderFailure::unsupported(StoreError::Failure(
                "memory provider selected for a custom configuration".into(),
            ))),
        }
    }
}
