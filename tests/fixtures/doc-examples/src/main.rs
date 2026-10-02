// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Compiles typed task execution with automatic lifecycle publication.

use std::sync::Arc;

use qubit_event_bus::EventBusConfig;
use qubit_event_bus::AsyncEventBusRegistry;
use qubit_event_bus::codec::CodecRegistry;
use qubit_event_bus::facade::EventBusFacadeConfig;
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
    let bus = Arc::new(AsyncEventBusRegistry::discover()?.create(&config).await?);
    let topic = Topic::<TaskEvent>::new("task.lifecycle")?;

    let mut builder = service_builder()?;
    register_handler(&mut builder)?;
    builder = builder.event_notifications(bus.clone(), topic, std::num::NonZeroUsize::new(16).unwrap(), std::time::Duration::from_secs(3));
    let service = builder.build().await?;
    let accepted = service.submit(request(serde_json::json!({"source": "guide"}), "guide-1")).await?;
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if service.get(accepted.id).await?.is_some_and(|task| task.state.is_terminal()) { break; }
            tokio::task::yield_now().await;
        }
        Ok::<(), Box<dyn std::error::Error>>(())
    }).await??;
    service.shutdown().await?;
    let _ = bus.shutdown(qubit_event_bus::spi::ShutdownMode::Immediate).await?;
    Ok(())
}
