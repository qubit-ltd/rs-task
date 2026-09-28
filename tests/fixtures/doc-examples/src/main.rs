use std::sync::Arc;

use qubit_event_bus::CodecError;
use qubit_event_bus::EventBusConfig;
use qubit_event_bus::EventBusRegistry;
use qubit_event_bus::codec::CodecRegistry;
use qubit_event_bus::codec::EventCodec;
use qubit_event_bus::facade::EventBusFacadeConfig;
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::SchemaId;
use qubit_event_bus_redis as _;
use qubit_spi::ProviderSelection;
use qubit_task::event::TaskEvent;
use qubit_task::service::TaskExecutionServiceBuilder;

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

struct TaskEventJsonCodec {
    content_type: ContentType,
    schema_id: SchemaId,
}

impl TaskEventJsonCodec {
    fn new() -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self {
            content_type: ContentType::new("application/json")?,
            schema_id: SchemaId::new("task-event-v1")?,
        })
    }
}

impl EventCodec<TaskEvent> for TaskEventJsonCodec {
    fn content_type(&self) -> &ContentType {
        &self.content_type
    }

    fn schema_id(&self) -> Option<&SchemaId> {
        Some(&self.schema_id)
    }

    fn encode(&self, value: &TaskEvent) -> Result<Arc<[u8]>, CodecError> {
        serde_json::to_vec(value)
            .map(Arc::from)
            .map_err(|source| CodecError::Encode { source: Box::new(source) })
    }

    fn decode(&self, bytes: &[u8]) -> Result<TaskEvent, CodecError> {
        serde_json::from_slice(bytes).map_err(|source| CodecError::Decode { source: Box::new(source) })
    }
}
