// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use qubit_codec::ValueBytesCodecDescriptor;
use qubit_codec::ValueBytesCodecRegistration;
use qubit_codec::ValueBytesCodecRegistry;
use qubit_codec::ValueCodecId;
use qubit_codec::ValueCodecRegistration;
use qubit_codec::ValueCodecRegistrationSource;
use qubit_codec::ValueDecoder;
use qubit_codec::ValueEncoder;
use qubit_id::Id;
use qubit_id::IdGenerationError;
use qubit_id::IdGenerator;
use qubit_model_id::HasModelId;
use qubit_model_id::ModelId;
use qubit_model_id::ModelIdBuf;
use qubit_task::CancellationMode;
use qubit_task::TaskContext;
use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::TaskHandler;
use qubit_task::TaskHandlerDescriptor;
use qubit_task::handler::TaskRunOutcome;
use qubit_task::handler::TaskRunResult;
use qubit_task::model::ResourceRequest;
use qubit_task::model::TaskOutput;
use qubit_task::model::TaskRequest;
use qubit_task::store::MemoryTaskStore;
use qubit_task::store::TaskFuture;
use serde_json::Error as JsonError;
use serde_json::Value;
use serde_json::from_slice;
use serde_json::to_vec;

/// JSON-backed payload used by the typed task examples.
#[derive(Clone)]
pub struct ExamplePayload(
    /// The JSON value serialized by [`JsonValueCodec`].
    pub Value,
);

impl HasModelId for ExamplePayload {
    const MODEL_ID: ModelId = ModelId::new("example.TaskPayload");
}

/// JSON encoder and decoder for [`ExamplePayload`].
#[derive(Default)]
pub struct JsonValueCodec;

impl ValueEncoder<ExamplePayload> for JsonValueCodec {
    type Output = Vec<u8>;
    type Error = JsonError;

    fn encode(&mut self, value: &ExamplePayload) -> Result<Vec<u8>, Self::Error> {
        to_vec(&value.0)
    }
}

impl ValueDecoder<[u8]> for JsonValueCodec {
    type Output = ExamplePayload;
    type Error = JsonError;

    fn decode(&mut self, bytes: &[u8]) -> Result<Self::Output, Self::Error> {
        from_slice(bytes).map(ExamplePayload)
    }
}

/// Describes the byte codec used to encode and decode example payloads.
pub static JSON_DESCRIPTOR: ValueBytesCodecDescriptor =
    ValueBytesCodecDescriptor::of::<JsonValueCodec, ExamplePayload>();
/// Registration for the example payload's JSON byte codec.
pub static JSON_CODEC: ValueBytesCodecRegistration = ValueCodecRegistration::new(
    ValueCodecId::new("example.task.json"),
    &JSON_DESCRIPTOR,
    ValueCodecRegistrationSource::new("qubit-task", "doc-examples", "typed-api", 1),
);

/// Builds a registry containing the codec required by the typed examples.
///
/// Returns an error if the codec registration cannot be added to the registry.
pub fn codecs() -> Result<ValueBytesCodecRegistry, Box<dyn std::error::Error>> {
    Ok(ValueBytesCodecRegistry::from_registrations([&JSON_CODEC])?)
}

/// Generates deterministic, sequential IDs for repeatable examples.
pub struct SequentialIds(AtomicU64);

impl SequentialIds {
    /// Creates a generator whose first ID has the supplied numeric value.
    #[must_use]
    pub fn new(first: u64) -> Self {
        Self(AtomicU64::new(first))
    }
}

impl IdGenerator<Id, IdGenerationError> for SequentialIds {
    fn generate(&self) -> Result<Id, IdGenerationError> {
        Ok(Id::new(self.0.fetch_add(1, Ordering::Relaxed)))
    }
}

/// Creates a service builder wired to the in-memory example store and codecs.
///
/// The builder starts generated IDs at 100. Returns an error if codec setup
/// fails.
pub fn service_builder() -> Result<TaskExecutionServiceBuilder, Box<dyn std::error::Error>> {
    Ok(TaskExecutionServiceBuilder::new(
        Arc::new(MemoryTaskStore::new(256)),
        Arc::new(codecs()?),
        Arc::new(SequentialIds::new(100)),
    ))
}

/// Creates a typed request for the example handler with an idempotency key.
///
/// `data` becomes the JSON payload and `key` is used to identify retries.
#[must_use]
pub fn request(data: Value, key: &str) -> TaskRequest<ExamplePayload> {
    let mut request = TaskRequest::new(
        "example.process",
        1,
        ValueCodecId::new("example.task.json"),
        ExamplePayload(data),
    );
    request.category = Some("example".into());
    request.idempotency_key = Some(key.into());
    request.resource_limit = ResourceRequest::default();
    request
}

/// Completes each example task successfully without additional work.
pub struct ExampleHandler;

impl TaskHandler<ExamplePayload> for ExampleHandler {
    fn run<'a>(&'a self, _input: ExamplePayload, _context: TaskContext) -> TaskFuture<'a, TaskRunResult> {
        Box::pin(async { Ok(TaskRunOutcome::Succeeded(TaskOutput::default())) })
    }
}

/// Registers [`ExampleHandler`] for the example task kind in the builder.
///
/// Returns an error if the handler descriptor cannot be registered.
pub fn register_handler(builder: &mut TaskExecutionServiceBuilder) -> Result<(), Box<dyn std::error::Error>> {
    builder.handlers_mut().register::<ExamplePayload, _>(
        TaskHandlerDescriptor {
            kind_id: "example.process".into(),
            payload_type_id: ModelIdBuf::try_from("example.TaskPayload")?,
            accepted_schema_versions: vec![1],
            cancellation_mode: CancellationMode::Unsupported,
        },
        Arc::new(ExampleHandler),
    )?;
    Ok(())
}
