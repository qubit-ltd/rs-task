// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Injects a configured store into the typed task service builder.

use std::sync::Arc;

use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::model::ResourceCapacity;
use qubit_task::store::MemoryTaskStore;
use typed_support::SequentialIds;

#[path = "../typed_support.rs"]
mod typed_support;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut builder = TaskExecutionServiceBuilder::new(
        Arc::new(MemoryTaskStore::new(32)),
        Arc::new(typed_support::codecs()?),
        Arc::new(SequentialIds::new(900)),
    )
    .capacity(ResourceCapacity {
        cpu_slots: 2,
        ..ResourceCapacity::default()
    });
    typed_support::register_handler(&mut builder)?;
    let service = builder.build().await?;
    service.shutdown().await?;
    Ok(())
}
