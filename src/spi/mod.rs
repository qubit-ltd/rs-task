// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
// qubit-style: allow multiple-public-types
//! Typed `qubit-spi` families for task stores, execution engines, schedulers,
//! and handlers.
//!
//! Applications choose provider IDs during startup and pass the created
//! components to `TaskExecutionServiceBuilder`. Enabling the `inventory`
//! feature includes providers submitted by linked crates.

use std::any::Any;
use std::convert::Infallible;
use std::sync::Arc;

use qubit_spi::ServiceSpec;
use qubit_spi::SyncServiceSpec;

use crate::engine::LocalTaskExecutionEngine;
use crate::engine::TaskExecutionEngine;
use crate::handler::TaskHandler;
use crate::model::ResourceCapacity;
use crate::scheduling::FairFifoPolicy;
use crate::scheduling::SchedulingPolicy;
use crate::store::MemoryTaskStore;
use crate::store::StoreError;
use crate::store::TaskStore;

/// Runtime configuration selected for a task store provider.
#[derive(Clone)]
pub enum TaskStoreConfig {
    /// Volatile records with a bounded terminal history.
    Memory {
        /// Maximum retained terminal records.
        history_capacity: usize,
    },
    /// SQLite records with restart recovery.
    #[cfg(feature = "sqlite")]
    Sqlite {
        /// Database path owned by the service.
        path: std::path::PathBuf,
    },
    /// Provider-specific runtime settings for an application or extension
    /// crate.
    Custom(Arc<dyn Any + Send + Sync>),
}
impl Default for TaskStoreConfig {
    fn default() -> Self {
        Self::Memory { history_capacity: 1024 }
    }
}

impl TaskStoreConfig {
    /// Wraps a typed extension configuration for one selected provider.
    #[must_use]
    pub fn custom<T: Any + Send + Sync>(config: T) -> Self {
        Self::Custom(Arc::new(config))
    }

    /// Borrows extension configuration when its concrete type matches `T`.
    #[must_use]
    pub fn downcast_ref<T: Any>(&self) -> Option<&T> {
        match self {
            Self::Custom(config) => config.downcast_ref(),
            _ => None,
        }
    }
}

/// Service family for a task history backend.
pub struct TaskStoreSpec;
impl ServiceSpec for TaskStoreSpec {
    type Config = TaskStoreConfig;
    type Error = StoreError;
}
impl SyncServiceSpec for TaskStoreSpec {
    type Output = Arc<dyn TaskStore>;
}

/// Service family for an ordering policy.
pub struct SchedulingPolicySpec;
impl ServiceSpec for SchedulingPolicySpec {
    type Config = ();
    type Error = Infallible;
}
impl SyncServiceSpec for SchedulingPolicySpec {
    type Output = Arc<dyn SchedulingPolicy>;
}

/// Service family for a local or future distributed execution engine.
pub struct TaskExecutionEngineSpec;
impl ServiceSpec for TaskExecutionEngineSpec {
    type Config = ResourceCapacity;
    type Error = Infallible;
}
impl SyncServiceSpec for TaskExecutionEngineSpec {
    type Output = Arc<dyn TaskExecutionEngine>;
}

/// Service family for task handlers. Applications may register many providers.
pub struct TaskHandlerSpec;
impl ServiceSpec for TaskHandlerSpec {
    type Config = Arc<dyn Any + Send + Sync>;
    type Error = std::io::Error;
}
impl SyncServiceSpec for TaskHandlerSpec {
    type Output = Arc<dyn TaskHandler>;
}

#[cfg(feature = "inventory")]
qubit_spi::declare_sync_provider_inventory! { pub mod task_store_providers { spec = TaskStoreSpec; } }
#[cfg(feature = "inventory")]
qubit_spi::declare_sync_provider_inventory! { pub mod scheduling_policy_providers { spec = SchedulingPolicySpec; } }
#[cfg(feature = "inventory")]
qubit_spi::declare_sync_provider_inventory! { pub mod task_execution_engine_providers { spec = TaskExecutionEngineSpec; } }
#[cfg(feature = "inventory")]
qubit_spi::declare_sync_provider_inventory! { pub mod task_handler_providers { spec = TaskHandlerSpec; } }

struct MemoryStoreProvider;
impl qubit_spi::ProviderMetadata for MemoryStoreProvider {
    fn descriptor(&self) -> qubit_spi::ProviderDescriptor {
        qubit_spi::provider_descriptor!("qubit.task.store.memory")
    }
}
impl qubit_spi::ServiceProvider<TaskStoreSpec> for MemoryStoreProvider {
    fn create_configured(
        &self,
        config: &TaskStoreConfig,
    ) -> Result<Arc<dyn TaskStore>, qubit_spi::error::ProviderFailure<StoreError>> {
        match config {
            TaskStoreConfig::Memory { history_capacity } => Ok(Arc::new(MemoryTaskStore::new(*history_capacity))),
            #[cfg(feature = "sqlite")]
            TaskStoreConfig::Sqlite { .. } => Err(qubit_spi::error::ProviderFailure::unsupported(StoreError::Failure(
                "SQLite provider selected for a memory configuration".into(),
            ))),
            TaskStoreConfig::Custom(_) => Err(qubit_spi::error::ProviderFailure::unsupported(StoreError::Failure(
                "memory provider selected for a custom configuration".into(),
            ))),
        }
    }
}

struct FairFifoProvider;
impl qubit_spi::ProviderMetadata for FairFifoProvider {
    fn descriptor(&self) -> qubit_spi::ProviderDescriptor {
        qubit_spi::provider_descriptor!("qubit.task.scheduler.fair-fifo")
    }
}
impl qubit_spi::ServiceProvider<SchedulingPolicySpec> for FairFifoProvider {
    fn create_configured(
        &self,
        _config: &(),
    ) -> Result<Arc<dyn SchedulingPolicy>, qubit_spi::error::ProviderFailure<Infallible>> {
        Ok(Arc::new(FairFifoPolicy::default()))
    }
}

struct LocalEngineProvider;
impl qubit_spi::ProviderMetadata for LocalEngineProvider {
    fn descriptor(&self) -> qubit_spi::ProviderDescriptor {
        qubit_spi::provider_descriptor!("qubit.task.engine.local")
    }
}
impl qubit_spi::ServiceProvider<TaskExecutionEngineSpec> for LocalEngineProvider {
    fn create_configured(
        &self,
        config: &ResourceCapacity,
    ) -> Result<Arc<dyn TaskExecutionEngine>, qubit_spi::error::ProviderFailure<Infallible>> {
        Ok(Arc::new(LocalTaskExecutionEngine::new(config.clone())))
    }
}

#[cfg(feature = "sqlite")]
struct SqliteStoreProvider;
#[cfg(feature = "sqlite")]
impl qubit_spi::ProviderMetadata for SqliteStoreProvider {
    fn descriptor(&self) -> qubit_spi::ProviderDescriptor {
        qubit_spi::provider_descriptor!("qubit.task.store.sqlite")
    }
}
#[cfg(feature = "sqlite")]
impl qubit_spi::ServiceProvider<TaskStoreSpec> for SqliteStoreProvider {
    fn create_configured(
        &self,
        config: &TaskStoreConfig,
    ) -> Result<Arc<dyn TaskStore>, qubit_spi::error::ProviderFailure<StoreError>> {
        match config {
            TaskStoreConfig::Sqlite { path } => crate::store::SqliteTaskStore::open(path)
                .map(|store| Arc::new(store) as Arc<dyn TaskStore>)
                .map_err(qubit_spi::error::ProviderFailure::unavailable),
            TaskStoreConfig::Memory { .. } => Err(qubit_spi::error::ProviderFailure::unsupported(StoreError::Failure(
                "SQLite provider selected for a memory configuration".into(),
            ))),
            TaskStoreConfig::Custom(_) => Err(qubit_spi::error::ProviderFailure::unsupported(StoreError::Failure(
                "SQLite provider selected for a custom configuration".into(),
            ))),
        }
    }
}

#[cfg(feature = "inventory")]
qubit_spi::submit_sync_provider! { inventory_entry = task_store_providers::Entry; spec = TaskStoreSpec; provider = MemoryStoreProvider; }
#[cfg(feature = "inventory")]
qubit_spi::submit_sync_provider! { inventory_entry = scheduling_policy_providers::Entry; spec = SchedulingPolicySpec; provider = FairFifoProvider; }
#[cfg(feature = "inventory")]
qubit_spi::submit_sync_provider! { inventory_entry = task_execution_engine_providers::Entry; spec = TaskExecutionEngineSpec; provider = LocalEngineProvider; }
#[cfg(all(feature = "inventory", feature = "sqlite"))]
qubit_spi::submit_sync_provider! { inventory_entry = task_store_providers::Entry; spec = TaskStoreSpec; provider = SqliteStoreProvider; }

/// Returns the built-in volatile store provider registry.
#[must_use]
pub fn memory_store_registry() -> qubit_spi::ProviderRegistry<TaskStoreSpec> {
    let registry = qubit_spi::ProviderRegistry::default();
    registry
        .register(MemoryStoreProvider)
        .expect("built-in provider ID is unique");
    registry
}

/// Returns the built-in fair FIFO policy provider registry.
#[must_use]
pub fn scheduling_policy_registry() -> qubit_spi::ProviderRegistry<SchedulingPolicySpec> {
    let registry = qubit_spi::ProviderRegistry::default();
    registry
        .register(FairFifoProvider)
        .expect("built-in provider ID is unique");
    registry
}

/// Returns the built-in local execution engine provider registry.
#[must_use]
pub fn task_execution_engine_registry() -> qubit_spi::ProviderRegistry<TaskExecutionEngineSpec> {
    let registry = qubit_spi::ProviderRegistry::default();
    registry
        .register(LocalEngineProvider)
        .expect("built-in provider ID is unique");
    registry
}

/// Returns an empty handler provider registry for explicit application
/// assembly.
#[must_use]
pub fn task_handler_registry() -> qubit_spi::ProviderRegistry<TaskHandlerSpec> {
    qubit_spi::ProviderRegistry::default()
}

/// Builds a registry from linked store provider inventories when enabled.
#[cfg(feature = "inventory")]
pub fn discovered_task_store_registry()
-> Result<qubit_spi::ProviderRegistry<TaskStoreSpec>, qubit_spi::error::ProviderInventoryBuildError> {
    task_store_providers::build_registry()
}

/// Builds a registry from linked scheduling policy provider inventories when
/// enabled.
#[cfg(feature = "inventory")]
pub fn discovered_scheduling_policy_registry()
-> Result<qubit_spi::ProviderRegistry<SchedulingPolicySpec>, qubit_spi::error::ProviderInventoryBuildError> {
    scheduling_policy_providers::build_registry()
}

/// Builds a registry from linked execution engine provider inventories when
/// enabled.
#[cfg(feature = "inventory")]
pub fn discovered_task_execution_engine_registry()
-> Result<qubit_spi::ProviderRegistry<TaskExecutionEngineSpec>, qubit_spi::error::ProviderInventoryBuildError> {
    task_execution_engine_providers::build_registry()
}

/// Builds a registry from linked handler provider inventories when enabled.
#[cfg(feature = "inventory")]
pub fn discovered_task_handler_registry()
-> Result<qubit_spi::ProviderRegistry<TaskHandlerSpec>, qubit_spi::error::ProviderInventoryBuildError> {
    task_handler_providers::build_registry()
}

/// Returns the stable ID for the default FIFO fair scheduling provider.
pub const FAIR_FIFO_PROVIDER_ID: &str = "qubit.task.scheduler.fair-fifo";

/// Returns the stable ID for the local execution engine provider.
pub const LOCAL_ENGINE_PROVIDER_ID: &str = "qubit.task.engine.local";

/// Returns the stable ID for the in-memory storage provider.
pub const MEMORY_STORE_PROVIDER_ID: &str = "qubit.task.store.memory";

/// Returns the stable ID reserved for the recoverable SQLite storage provider.
#[cfg(feature = "sqlite")]
pub const SQLITE_STORE_PROVIDER_ID: &str = "qubit.task.store.sqlite";
