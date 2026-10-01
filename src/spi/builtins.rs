// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Built-in provider registry construction and inventory discovery.

use qubit_spi::ProviderRegistry;
#[cfg(feature = "inventory")]
use qubit_spi::error::ProviderInventoryBuildError;

use super::internal::FairFifoProvider;
use super::internal::LocalEngineProvider;
use super::internal::MemoryStoreProvider;
#[cfg(feature = "sqlite")]
use super::internal::SqliteStoreProvider;
use crate::spi::scheduling_policy_spec::SchedulingPolicySpec;
use crate::spi::task_execution_engine_spec::TaskExecutionEngineSpec;
use crate::spi::task_handler_spec::TaskHandlerSpec;
use crate::spi::task_store_spec::TaskStoreSpec;

/// Returns the built-in volatile store provider registry.
///
/// # Returns
///
/// A registry containing the memory store provider.
#[must_use]
pub fn memory_store_registry() -> ProviderRegistry<TaskStoreSpec> {
    let registry = ProviderRegistry::default();
    registry
        .register(MemoryStoreProvider)
        .expect("built-in provider ID is unique");
    registry
}

/// Returns the built-in SQLite store provider registry.
///
/// # Returns
///
/// A registry containing the SQLite store provider.
#[cfg(feature = "sqlite")]
#[must_use]
pub fn sqlite_store_registry() -> ProviderRegistry<TaskStoreSpec> {
    let registry = ProviderRegistry::default();
    registry
        .register(SqliteStoreProvider)
        .expect("built-in provider ID is unique");
    registry
}

/// Returns the built-in fair FIFO policy provider registry.
///
/// # Returns
///
/// A registry containing the fair FIFO scheduling provider.
#[must_use]
pub fn scheduling_policy_registry() -> ProviderRegistry<SchedulingPolicySpec> {
    let registry = ProviderRegistry::default();
    registry
        .register(FairFifoProvider)
        .expect("built-in provider ID is unique");
    registry
}

/// Returns the built-in local execution engine provider registry.
///
/// # Returns
///
/// A registry containing the local execution engine provider.
#[must_use]
pub fn task_execution_engine_registry() -> ProviderRegistry<TaskExecutionEngineSpec> {
    let registry = ProviderRegistry::default();
    registry
        .register(LocalEngineProvider)
        .expect("built-in provider ID is unique");
    registry
}

/// Returns an empty handler provider registry for explicit application
/// assembly.
///
/// # Returns
///
/// An empty registry ready for application-selected handler providers.
#[must_use]
pub fn task_handler_registry() -> ProviderRegistry<TaskHandlerSpec> {
    ProviderRegistry::default()
}

/// Builds a registry from linked store provider inventories when enabled.
///
/// # Returns
///
/// The discovered store providers registered under their declared IDs.
///
/// # Errors
///
/// Returns an inventory error for duplicate or invalid provider descriptors.
#[cfg(feature = "inventory")]
pub fn discovered_task_store_registry() -> Result<ProviderRegistry<TaskStoreSpec>, ProviderInventoryBuildError> {
    super::inventory::task_store_providers::build_registry()
}
