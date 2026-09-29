// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Verifies task lifecycle notifications through a real Redis event-bus
//! provider.

#![cfg(feature = "event-bus")]

use std::error::Error;
use std::net::TcpListener;
use std::process::Command;
use std::sync::Arc;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use qubit_event_bus::DeliveryError;
use qubit_event_bus::EventBus;
use qubit_event_bus::EventBusConfig;
use qubit_event_bus::EventBusRegistry;
use qubit_event_bus::codec::CodecRegistry;
use qubit_event_bus::codec::EventCodec;
use qubit_event_bus::error::CodecError;
use qubit_event_bus::facade::EventBusFacadeConfig;
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::ProviderOptions;
use qubit_event_bus::model::SchemaId;
use qubit_event_bus::model::StartPosition;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::SubscriberId;
use qubit_event_bus::model::SubscriptionDurability;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus_redis as _;
use qubit_spi::ProviderSelection;
use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::event::TaskEvent;
use qubit_task::model::TaskOutput;
use qubit_task::model::TaskState;
use qubit_task::service::LocalTaskOutcome;
use redis::Client;
use serde_json as json;
use tokio::runtime::Handle;
use tokio::test as tokio_test;

struct RedisContainer {
    container_id: String,
    url: String,
}

impl RedisContainer {
    fn start() -> Result<Self, Box<dyn Error>> {
        let port = TcpListener::bind(("127.0.0.1", 0))?.local_addr()?.port();
        let port_mapping = format!("127.0.0.1:{port}:6379");
        let output = Command::new("docker")
            .args([
                "run",
                "-d",
                "-p",
                port_mapping.as_str(),
                "redis:7-alpine",
                "redis-server",
                "--appendonly",
                "yes",
            ])
            .output()?;
        if !output.status.success() {
            return Err(format!("docker run failed: {}", String::from_utf8_lossy(&output.stderr)).into());
        }
        let container_id = String::from_utf8(output.stdout)?.trim().to_owned();
        let server = Self {
            container_id,
            url: format!("redis://127.0.0.1:{port}/"),
        };
        for _ in 0..50 {
            if Client::open(server.url.as_str())
                .and_then(|client| client.get_connection())
                .is_ok()
            {
                return Ok(server);
            }
            thread::sleep(Duration::from_millis(100));
        }
        Err("Redis container did not become ready".into())
    }
}

impl Drop for RedisContainer {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "-f", self.container_id.as_str()])
            .output();
    }
}

struct TaskEventJsonCodec {
    content_type: ContentType,
}

impl EventCodec<TaskEvent> for TaskEventJsonCodec {
    fn content_type(&self) -> &ContentType {
        &self.content_type
    }

    fn schema_id(&self) -> Option<&SchemaId> {
        None
    }

    fn encode(&self, value: &TaskEvent) -> Result<Arc<[u8]>, CodecError> {
        json::to_vec(value)
            .map(Arc::<[u8]>::from)
            .map_err(|source| CodecError::Encode {
                source: Box::new(source),
            })
    }

    fn decode(&self, bytes: &[u8]) -> Result<TaskEvent, CodecError> {
        json::from_slice(bytes).map_err(|source| CodecError::Decode {
            source: Box::new(source),
        })
    }
}

fn create_event_bus(url: &str, namespace: &str, register_task_codec: bool) -> Result<EventBus, Box<dyn Error>> {
    let mut codecs = CodecRegistry::new();
    if register_task_codec {
        codecs.register::<TaskEvent>(Arc::new(TaskEventJsonCodec {
            content_type: ContentType::new("application/json")?,
        }));
    }
    let options: ProviderOptions = [
        ("redis.url".into(), url.into()),
        ("redis.namespace".into(), namespace.into()),
        ("redis.claim_min_idle_ms".into(), "60000".into()),
    ]
    .into();
    let config = EventBusConfig::default()
        .with_selection(ProviderSelection::named("redis-streams")?)
        .with_provider_options(options)
        .with_facade_config(EventBusFacadeConfig::new().with_codec_registry(Arc::new(codecs)));
    Ok(EventBusRegistry::discover()?.create(&config)?)
}

#[tokio_test(flavor = "multi_thread")]
async fn test_task_lifecycle_notifications_publish_and_consume_via_redis() -> Result<(), Box<dyn Error>> {
    let redis = RedisContainer::start()?;
    let bus = create_event_bus(&redis.url, "task-redis-integration", true)?;
    let topic = Topic::<TaskEvent>::new("task.lifecycle")?;
    let (sender, receiver) = mpsc::channel();
    let subscription = bus.subscribe(
        SubscribeRequest::builder()
            .subscriber_id(SubscriberId::new("task-redis-integration")?)
            .topic(topic)
            .start_position(StartPosition::Earliest)
            .durability(SubscriptionDurability::Durable)
            .build()?,
        move |delivery| {
            let payload = delivery.payload();
            let _ = sender.send((payload.task_id, payload.state_version, payload.state.clone()));
            Ok::<(), DeliveryError>(())
        },
    )?;

    let service = TaskExecutionServiceBuilder::in_memory()
        .runtime_handle(Handle::current())
        .event_bus(bus.clone())
        .build()
        .await?;
    let id = service
        .submit_local(|_| LocalTaskOutcome::<(), std::io::Error>::Succeeded {
            value: (),
            summary: TaskOutput::default(),
        })
        .await?
        .task_id();
    assert_eq!(service.wait(id).await?.state, TaskState::Succeeded);
    service.shutdown().await?;

    let mut observed = Vec::new();
    for _ in 0..3 {
        observed.push(receiver.recv_timeout(Duration::from_secs(5))?);
    }
    observed.sort_by_key(|(_, state_version, _)| *state_version);
    assert_eq!(observed.len(), 3);
    assert!(observed.iter().all(|(task_id, _, _)| *task_id == id));
    assert_eq!(
        observed.iter().map(|(_, version, _)| *version).collect::<Vec<_>>(),
        [0, 1, 2]
    );
    assert_eq!(
        observed.iter().map(|(_, _, state)| state).collect::<Vec<_>>(),
        [&TaskState::Queued, &TaskState::Running, &TaskState::Succeeded]
    );
    let stats = service.notification_stats().expect("notification counters");
    assert!(stats.accepted + stats.opaque_accepted >= 3, "{stats:?}");
    assert_eq!(stats.publish_error, 0, "{stats:?}");

    subscription.cancel()?;
    bus.shutdown(ShutdownMode::Immediate)?;
    Ok(())
}

#[test]
fn test_redis_facade_rejects_task_event_subscription_without_codec() -> Result<(), Box<dyn Error>> {
    let redis = RedisContainer::start()?;
    let bus = create_event_bus(&redis.url, "task-redis-no-codec", false)?;
    let topic = Topic::<TaskEvent>::new("task.lifecycle")?;
    let result = bus.subscribe(
        SubscribeRequest::builder()
            .subscriber_id(SubscriberId::new("task-redis-no-codec")?)
            .topic(topic)
            .build()?,
        |_| Ok::<(), DeliveryError>(()),
    );
    assert!(result.is_err(), "typed subscription without a codec must fail");
    bus.shutdown(ShutdownMode::Immediate)?;
    Ok(())
}
