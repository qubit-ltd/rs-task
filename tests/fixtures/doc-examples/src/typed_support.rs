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

#[derive(Clone)]
pub struct ExamplePayload(pub serde_json::Value);

impl HasModelId for ExamplePayload {
    const MODEL_ID: ModelId = ModelId::new("example.TaskPayload");
}

#[derive(Default)]
pub struct JsonValueCodec;

impl qubit_codec::ValueEncoder<ExamplePayload> for JsonValueCodec {
    type Output = Vec<u8>;
    type Error = serde_json::Error;

    fn encode(&mut self, value: &ExamplePayload) -> Result<Vec<u8>, Self::Error> {
        serde_json::to_vec(&value.0)
    }
}

impl qubit_codec::ValueDecoder<[u8]> for JsonValueCodec {
    type Output = ExamplePayload;
    type Error = serde_json::Error;

    fn decode(&mut self, bytes: &[u8]) -> Result<Self::Output, Self::Error> {
        serde_json::from_slice(bytes).map(ExamplePayload)
    }
}

pub static JSON_DESCRIPTOR: ValueBytesCodecDescriptor =
    ValueBytesCodecDescriptor::of::<JsonValueCodec, ExamplePayload>();
pub static JSON_CODEC: ValueBytesCodecRegistration = ValueCodecRegistration::new(
    ValueCodecId::new("example.task.json"),
    &JSON_DESCRIPTOR,
    ValueCodecRegistrationSource::new("qubit-task", "doc-examples", "typed-api", 1),
);

pub fn codecs() -> Result<ValueBytesCodecRegistry, Box<dyn std::error::Error>> {
    Ok(ValueBytesCodecRegistry::from_registrations([&JSON_CODEC])?)
}

pub struct SequentialIds(AtomicU64);

impl SequentialIds {
    pub fn new(first: u64) -> Self {
        Self(AtomicU64::new(first))
    }
}

impl IdGenerator<Id, IdGenerationError> for SequentialIds {
    fn generate(&self) -> Result<Id, IdGenerationError> {
        Ok(Id::new(self.0.fetch_add(1, Ordering::Relaxed)))
    }
}

pub fn service_builder() -> Result<TaskExecutionServiceBuilder, Box<dyn std::error::Error>> {
    Ok(TaskExecutionServiceBuilder::new(
        Arc::new(MemoryTaskStore::new(256)),
        Arc::new(codecs()?),
        Arc::new(SequentialIds::new(100)),
    ))
}

pub fn request(data: serde_json::Value, key: &str) -> TaskRequest<ExamplePayload> {
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

pub struct ExampleHandler;

impl TaskHandler<ExamplePayload> for ExampleHandler {
    fn run<'a>(&'a self, _input: ExamplePayload, _context: TaskContext) -> TaskFuture<'a, TaskRunResult> {
        Box::pin(async { Ok(TaskRunOutcome::Succeeded(TaskOutput::default())) })
    }
}

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
