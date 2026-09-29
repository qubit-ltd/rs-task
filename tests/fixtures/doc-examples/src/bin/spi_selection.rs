// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Selects built-in providers explicitly and assembles a task service.

use qubit_spi::ProviderSelection;
use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::model::ResourceCapacity;
use qubit_task::spi;
use qubit_task::spi::TaskStoreConfig;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = spi::memory_store_registry()
        .resolve_selected(&ProviderSelection::named(spi::MEMORY_STORE_PROVIDER_ID)?)?
        .create_configured(&TaskStoreConfig::Memory { history_capacity: 32 })?;
    let policy = spi::scheduling_policy_registry()
        .resolve_selected(&ProviderSelection::named(spi::FAIR_FIFO_PROVIDER_ID)?)?
        .create_configured(&())?;
    let engine = spi::task_execution_engine_registry()
        .resolve_selected(&ProviderSelection::named(spi::LOCAL_ENGINE_PROVIDER_ID)?)?
        .create_configured(&ResourceCapacity { cpu_slots: 2, ..ResourceCapacity::default() })?;

    let service = TaskExecutionServiceBuilder::from_components(store, engine, policy)
        .require_recovery(false)
        .build()
        .await?;
    service.shutdown().await?;
    Ok(())
}
