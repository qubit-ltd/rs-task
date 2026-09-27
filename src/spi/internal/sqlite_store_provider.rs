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
use crate::store::SqliteTaskStore;
use crate::store::StoreError;
use crate::store::TaskStore;

/// Supplies the built-in recoverable SQLite task store.
pub(crate) struct SqliteStoreProvider;

impl ProviderMetadata for SqliteStoreProvider {
    /// Returns the stable provider identifier used by application assembly.
    ///
    /// # Returns
    ///
    /// The SQLite store provider descriptor.
    fn descriptor(&self) -> ProviderDescriptor {
        provider_descriptor!("qubit.task.store.sqlite")
    }
}

impl ServiceProvider<TaskStoreSpec> for SqliteStoreProvider {
    /// Opens the selected database or rejects a configuration for another
    /// backend.
    ///
    /// # Parameters
    ///
    /// * `config` - Store settings selected by the application.
    ///
    /// # Returns
    ///
    /// A shared SQLite store for a SQLite path configuration.
    ///
    /// # Errors
    ///
    /// Returns an unavailable-provider failure when opening the database fails,
    /// or an unsupported-provider failure for other configurations.
    fn create_configured(&self, config: &TaskStoreConfig) -> Result<Arc<dyn TaskStore>, ProviderFailure<StoreError>> {
        match config {
            TaskStoreConfig::Sqlite { path } => SqliteTaskStore::open(path)
                .map(|store| Arc::new(store) as Arc<dyn TaskStore>)
                .map_err(ProviderFailure::unavailable),
            TaskStoreConfig::Memory { .. } => Err(ProviderFailure::unsupported(StoreError::Failure(
                "SQLite provider selected for a memory configuration".into(),
            ))),
            TaskStoreConfig::Custom(_) => Err(ProviderFailure::unsupported(StoreError::Failure(
                "SQLite provider selected for a custom configuration".into(),
            ))),
        }
    }
}
