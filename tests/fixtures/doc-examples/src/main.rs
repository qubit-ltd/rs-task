// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Compiles the documented Event Bus provider-selection and task-service setup.

use std::sync::Arc;

use qubit_event_bus::EventBusConfig;
use qubit_event_bus::EventBusRegistry;
use qubit_event_bus::codec::CodecRegistry;
use qubit_event_bus::facade::EventBusFacadeConfig;
use qubit_event_bus_redis as _;
use qubit_spi::ProviderSelection;
use qubit_task::event::TaskEvent;
use qubit_task::service::TaskExecutionServiceBuilder;

mod task_event_codec;

use task_event_codec::TaskEventJsonCodec;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let codec = TaskEventJsonCodec::new()?;
    let mut codecs = CodecRegistry::new();
    codecs.register::<TaskEvent>(Arc::new(codec));
    let facade = EventBusFacadeConfig::new().with_codec_registry(Arc::new(codecs));
    let config = EventBusConfig::default()
        .with_selection(ProviderSelection::named("redis-streams")?)
        .with_provider_options([
            ("redis.url".into(), "redis://127.0.0.1/".into()),
            ("redis.namespace".into(), "task-service".into()),
        ].into())
        .with_facade_config(facade);
    let bus = EventBusRegistry::discover()?.create(&config)?;
    let service = TaskExecutionServiceBuilder::in_memory()
        .event_bus(bus)
        .build()
        .await?;
    service.shutdown().await?;
    Ok(())
}
