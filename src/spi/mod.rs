// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Typed `qubit-spi` families for task stores, execution engines, schedulers,
//! and handlers.
//!
//! Applications choose provider IDs during startup and pass the created
//! components to `TaskExecutionServiceBuilder`. Enabling the `inventory`
//! feature includes providers submitted by linked crates.

mod builtins;
mod internal;
#[cfg(feature = "inventory")]
mod inventory;
mod provider_ids;
mod scheduling_policy_spec;
mod task_execution_engine_spec;
mod task_handler_spec;
mod task_store_config;
mod task_store_spec;

#[cfg(all(test, feature = "inventory"))]
pub(crate) use builtins::discovered_task_store_registry;
#[cfg(all(test, feature = "inventory"))]
pub(crate) use builtins::memory_store_registry;
#[cfg(all(test, feature = "inventory"))]
pub(crate) use builtins::scheduling_policy_registry;
#[cfg(all(test, feature = "sqlite"))]
pub(crate) use builtins::sqlite_store_registry;
#[cfg(all(test, feature = "inventory"))]
pub(crate) use builtins::task_execution_engine_registry;
#[cfg(all(test, feature = "inventory"))]
pub(crate) use builtins::task_handler_registry;
#[cfg(all(test, feature = "inventory"))]
pub(crate) use inventory::task_store_providers;
#[cfg(all(test, feature = "inventory"))]
pub(crate) use provider_ids::FAIR_FIFO_PROVIDER_ID;
#[cfg(all(test, feature = "inventory"))]
pub(crate) use provider_ids::LOCAL_ENGINE_PROVIDER_ID;
#[cfg(all(test, feature = "inventory"))]
pub(crate) use provider_ids::MEMORY_STORE_PROVIDER_ID;
#[cfg(all(test, feature = "sqlite"))]
pub(crate) use provider_ids::SQLITE_STORE_PROVIDER_ID;
pub use scheduling_policy_spec::SchedulingPolicySpec;
pub use task_execution_engine_spec::TaskExecutionEngineSpec;
pub use task_handler_spec::TaskHandlerSpec;
pub use task_store_config::TaskStoreConfig;
pub use task_store_spec::TaskStoreSpec;
