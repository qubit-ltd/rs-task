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

use std::collections::HashMap;
use std::error::Error;
use std::net::TcpListener;
use std::process::Command;
use std::sync::Arc;
use std::sync::Mutex;
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
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::ConsumerGroup;
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::ProviderOptions;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SchemaId;
use qubit_event_bus::model::StartPosition;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::SubscriberId;
use qubit_event_bus::model::SubscriptionDurability;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::EncodedPayload;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus_redis as _;
use qubit_id::Id;
use qubit_spi::ProviderSelection;
use qubit_task::event::TaskEvent;
use qubit_task::model::TaskId;
use qubit_task::model::TaskState;
use redis::Client;
use serde_json as json;
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

/// JSON v1 codec accepting only v1 and historical absent schema identifiers.
struct TaskEventJsonCodec {
    content_type: ContentType,
    schema_id: SchemaId,
}

impl EventCodec<TaskEvent> for TaskEventJsonCodec {
    fn content_type(&self) -> &ContentType {
        &self.content_type
    }

    fn schema_id(&self) -> Option<&SchemaId> {
        Some(&self.schema_id)
    }

    /// Accepts the v1 JSON contract and its historical schema-less encoding.
    /// Other MIME texts and schema identifiers are configuration mismatches.
    fn validate_metadata(&self, payload: &EncodedPayload) -> Result<(), CodecError> {
        if payload.content_type() == &self.content_type
            && (payload.schema_id().is_none() || payload.schema_id() == Some(&self.schema_id))
        {
            return Ok(());
        }
        Err(CodecError::MetadataMismatch {
            expected_content_type: self.content_type.clone(),
            actual_content_type: payload.content_type().clone(),
            expected_schema_id: Some(self.schema_id.clone()),
            actual_schema_id: payload.schema_id().cloned(),
        })
    }

    fn encode(&self, value: &TaskEvent) -> Result<Arc<[u8]>, CodecError> {
        json::to_vec(value)
            .map(Arc::<[u8]>::from)
            .map_err(|source| CodecError::Encode {
                source: Box::new(source),
            })
    }

    fn decode(&self, payload: &EncodedPayload) -> Result<TaskEvent, CodecError> {
        json::from_slice(payload.bytes()).map_err(|source| CodecError::Decode {
            source: Box::new(source),
        })
    }
}

fn create_event_bus(url: &str, namespace: &str, register_task_codec: bool) -> Result<EventBus, Box<dyn Error>> {
    let mut codecs = CodecRegistry::new();
    if register_task_codec {
        codecs.register::<TaskEvent>(Arc::new(TaskEventJsonCodec {
            content_type: ContentType::new("application/json")?,
            schema_id: SchemaId::new("task-event-v1")?,
        }))?;
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
async fn test_task_lifecycle_snapshots_publish_and_consume_via_redis() -> Result<(), Box<dyn Error>> {
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

    let id = TaskId::from_id(Id::new(42));
    for (state_version, state) in [
        (0, TaskState::Queued),
        (1, TaskState::Running),
        (2, TaskState::Succeeded),
    ] {
        let _ = bus.publish(PublishRequest::new(
            Topic::<TaskEvent>::new("task.lifecycle")?,
            TaskEvent {
                schema_version: 1,
                task_id: id,
                state_version,
                state,
                correlation_key: None,
            },
        )?)?;
    }

    let mut observed = Vec::new();
    for _ in 0..3 {
        observed.push(receiver.recv_timeout(Duration::from_secs(5))?);
    }
    // Check delivered snapshots without asserting a transport arrival order.
    assert_eq!(observed.len(), 3);
    assert!(observed.iter().all(|(task_id, _, _)| task_id == &id));
    for (version, expected_state) in [
        (0, TaskState::Queued),
        (1, TaskState::Running),
        (2, TaskState::Succeeded),
    ] {
        assert!(
            observed
                .iter()
                .any(|(_, actual_version, state)| *actual_version == version && state == &expected_state)
        );
    }
    subscription.cancel()?;
    let report = bus.shutdown(ShutdownMode::Immediate)?;
    assert_eq!(report.outcome, qubit_event_bus::spi::ShutdownOutcome::Complete);
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
    let report = bus.shutdown(ShutdownMode::Immediate)?;
    assert_eq!(report.outcome, qubit_event_bus::spi::ShutdownOutcome::Complete);
    Ok(())
}

/// Checks the explicit typed lifecycle event JSON contract.
#[test]
fn test_task_event_codec_accepts_typed_event_json() -> Result<(), Box<dyn Error>> {
    let codec = TaskEventJsonCodec {
        content_type: ContentType::new("application/json")?,
        schema_id: SchemaId::new("task-event-v1")?,
    };
    let bytes =
        br#"{"schema_version":1,"task_id":1,"state_version":2,"state":"Succeeded","correlation_key":"typed-task"}"#;
    let payload = EncodedPayload::new(Arc::from(bytes.as_slice()), codec.content_type.clone(), None);
    codec.validate_metadata(&payload)?;
    let versioned = EncodedPayload::new(
        Arc::from(bytes.as_slice()),
        codec.content_type.clone(),
        Some(codec.schema_id.clone()),
    );
    codec.validate_metadata(&versioned)?;
    assert_eq!(codec.schema_id(), Some(&codec.schema_id));
    assert_eq!(codec.decode(&versioned)?.state_version, 2);
    let event = codec.decode(&payload)?;
    assert_eq!(event.schema_version, 1);
    assert_eq!(event.task_id, TaskId::from_id(Id::new(1)));
    assert_eq!(event.state_version, 2);
    assert_eq!(event.state, TaskState::Succeeded);
    assert_eq!(event.correlation_key.as_deref(), Some("typed-task"));
    Ok(())
}

/// Rejects future schemas and MIME changes before decoding.
#[test]
fn test_task_event_codec_rejects_unknown_metadata() -> Result<(), Box<dyn Error>> {
    let codec = TaskEventJsonCodec {
        content_type: ContentType::new("application/json")?,
        schema_id: SchemaId::new("task-event-v1")?,
    };
    for (content_type, schema_id) in [
        ("application/json", Some("task-event-v2")),
        ("text/plain", None),
        ("application/JSON", Some("task-event-v1")),
    ] {
        let payload = EncodedPayload::new(
            Arc::from([]),
            ContentType::new(content_type)?,
            schema_id.map(SchemaId::new).transpose()?,
        );
        assert!(matches!(
            codec.validate_metadata(&payload),
            Err(CodecError::MetadataMismatch { .. })
        ));
    }
    Ok(())
}

/// Applies task snapshots at the consumer, independently of transport order.
#[derive(Default)]
struct TaskProjection {
    latest: HashMap<TaskId, TaskEvent>,
    applied: usize,
}

impl TaskProjection {
    /// Applies a delivered snapshot; repeated or stale revisions must be
    /// ignored.
    fn consume(&mut self, event: &TaskEvent) {
        if self
            .latest
            .get(&event.task_id)
            .is_some_and(|current| current.state_version >= event.state_version)
        {
            return;
        }
        self.latest.insert(event.task_id, event.clone());
        self.applied += 1;
    }
}

/// Sends duplicates and stale snapshots through the real facade and handler.
/// The caller supplies the provider-supported durability and consumer group.
fn assert_consumer_convergence(bus: &EventBus, request: SubscribeRequest<TaskEvent>) -> Result<(), Box<dyn Error>> {
    let topic = request.topic().clone();
    let projection = Arc::new(Mutex::new(TaskProjection::default()));
    let captured = Arc::clone(&projection);
    let (sender, received) = mpsc::channel();
    let subscription = bus.subscribe(request, move |delivery| {
        captured.lock().expect("projection lock").consume(delivery.payload());
        sender.send(()).expect("consumer signal receiver");
    })?;
    let task_id = TaskId::from_id(Id::new(43));
    for (state_version, state) in [
        (2, TaskState::Running),
        (2, TaskState::Running),
        (1, TaskState::Queued),
        (3, TaskState::Succeeded),
        (1, TaskState::Queued),
    ] {
        let _ = bus.publish(PublishRequest::new(
            topic.clone(),
            TaskEvent {
                schema_version: 1,
                task_id,
                state_version,
                state,
                correlation_key: None,
            },
        )?)?;
        received.recv_timeout(Duration::from_secs(5))?;
    }
    let projection = projection.lock().expect("projection lock");
    let latest = projection.latest.get(&task_id).expect("consumer projected task");
    assert_eq!(latest.state_version, 3, "stale event cannot replace newer state");
    assert_eq!(latest.state, TaskState::Succeeded);
    assert_eq!(
        projection.applied, 2,
        "duplicate and stale events have no business effect"
    );
    subscription.cancel()?;
    Ok(())
}

/// Verifies the consumer policy without external IO.
#[test]
fn test_task_event_consumer_duplicate_and_stale_versions_converge_locally() -> Result<(), Box<dyn Error>> {
    let bus = EventBus::local(LocalEventBusConfig::default())?;
    let request = SubscribeRequest::new("projection-consumer", Topic::<TaskEvent>::new("task.lifecycle")?)?;
    assert_consumer_convergence(&bus, request)?;
    let report = bus.shutdown(ShutdownMode::Immediate)?;
    assert_eq!(report.outcome, qubit_event_bus::spi::ShutdownOutcome::Complete);
    Ok(())
}

/// Verifies the identical consumer policy through the Redis transport.
#[test]
fn test_task_event_consumer_duplicate_and_stale_versions_converge_via_redis() -> Result<(), Box<dyn Error>> {
    let redis = RedisContainer::start()?;
    let bus = create_event_bus(&redis.url, "task-consumer-convergence", true)?;
    let request = SubscribeRequest::builder()
        .subscriber_id(SubscriberId::new("projection-consumer")?)
        .topic(Topic::<TaskEvent>::new("task.lifecycle")?)
        .consumer_group(ConsumerGroup::new("projection-consumer-group")?)
        .durability(SubscriptionDurability::Durable)
        .start_position(StartPosition::Earliest)
        .build()?;
    assert_consumer_convergence(&bus, request)?;
    let report = bus.shutdown(ShutdownMode::Immediate)?;
    assert_eq!(report.outcome, qubit_event_bus::spi::ShutdownOutcome::Complete);
    Ok(())
}
