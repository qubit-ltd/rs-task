// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Compiles the typed task setup and an explicit TaskEvent transport bridge.

use std::sync::Arc;

use qubit_event_bus::EventBusConfig;
use qubit_event_bus::EventBusRegistry;
use qubit_event_bus::codec::CodecRegistry;
use qubit_event_bus::facade::EventBusFacadeConfig;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus_redis as _;
use qubit_spi::ProviderSelection;
use qubit_task::event::TaskEvent;

mod task_event_codec;
mod typed_support;

use task_event_codec::TaskEventJsonCodec;
use typed_support::register_handler;
use typed_support::request;
use typed_support::service_builder;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let codec = TaskEventJsonCodec::new()?;
    let mut event_codecs = CodecRegistry::new();
    event_codecs.register::<TaskEvent>(Arc::new(codec));
    let facade = EventBusFacadeConfig::new().with_codec_registry(Arc::new(event_codecs));
    let config = EventBusConfig::default()
        .with_selection(ProviderSelection::named("redis-streams")?)
        .with_provider_options([
            ("redis.url".into(), "redis://127.0.0.1/".into()),
            ("redis.namespace".into(), "task-service".into()),
        ].into())
        .with_facade_config(facade);
    let bus = EventBusRegistry::discover()?.create(&config)?;

    let mut builder = service_builder()?;
    register_handler(&mut builder)?;
    let service = builder.build().await?;
    let accepted = service.submit(request(serde_json::json!({"source": "guide"}), "guide-1")).await?;

    // Typed lifecycle publication is not yet attached to the service. This
    // explicit bridge illustrates the TaskEvent wire contract for consumers.
    let _ = bus.publish(PublishRequest::new(
        Topic::<TaskEvent>::new("task.lifecycle")?,
        TaskEvent {
            task_id: accepted.id.to_string(),
            state_version: accepted.state_version,
            state: accepted.state.clone(),
            correlation_key: accepted.correlation_key.clone(),
        },
    )?)?;
    service.shutdown().await?;
    bus.shutdown(qubit_event_bus::spi::ShutdownMode::Immediate)?;
    Ok(())
}
