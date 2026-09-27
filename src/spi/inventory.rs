// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Inventory declarations and registrations for built-in SPI providers.

#[cfg(feature = "inventory")]
use qubit_spi::declare_sync_provider_inventory;
#[cfg(feature = "inventory")]
use qubit_spi::submit_sync_provider;

use super::SchedulingPolicySpec;
use super::TaskExecutionEngineSpec;
use super::TaskHandlerSpec;
use super::TaskStoreSpec;
use super::internal::FairFifoProvider;
use super::internal::LocalEngineProvider;
use super::internal::MemoryStoreProvider;
#[cfg(feature = "sqlite")]
use super::internal::SqliteStoreProvider;

#[cfg(feature = "inventory")]
declare_sync_provider_inventory! { pub mod task_store_providers { spec = TaskStoreSpec; } }
#[cfg(feature = "inventory")]
declare_sync_provider_inventory! { pub mod scheduling_policy_providers { spec = SchedulingPolicySpec; } }
#[cfg(feature = "inventory")]
declare_sync_provider_inventory! { pub mod task_execution_engine_providers { spec = TaskExecutionEngineSpec; } }
#[cfg(feature = "inventory")]
declare_sync_provider_inventory! { pub mod task_handler_providers { spec = TaskHandlerSpec; } }

#[cfg(feature = "inventory")]
submit_sync_provider! { inventory_entry = task_store_providers::Entry; spec = TaskStoreSpec; provider = MemoryStoreProvider; }
#[cfg(feature = "inventory")]
submit_sync_provider! { inventory_entry = scheduling_policy_providers::Entry; spec = SchedulingPolicySpec; provider = FairFifoProvider; }
#[cfg(feature = "inventory")]
submit_sync_provider! { inventory_entry = task_execution_engine_providers::Entry; spec = TaskExecutionEngineSpec; provider = LocalEngineProvider; }
#[cfg(all(feature = "inventory", feature = "sqlite"))]
submit_sync_provider! { inventory_entry = task_store_providers::Entry; spec = TaskStoreSpec; provider = SqliteStoreProvider; }
