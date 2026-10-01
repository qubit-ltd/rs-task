// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;

use qubit_codec::ValueBytesCodecDescriptor;
use qubit_codec::ValueBytesCodecRegistration;
use qubit_codec::ValueBytesCodecRegistry;
use qubit_codec::ValueCodecId;
use qubit_codec::ValueCodecRegistration;
use qubit_codec::ValueCodecRegistrationSource;
use qubit_model_metadata::metadata::ModelId;
use qubit_model_metadata::metadata::ModelIdBuf;
use qubit_task::handler::TaskRunOutcome;
use qubit_task::handler::TaskRunResult;
use qubit_task::model::TaskOutput;
use qubit_task::model::TaskRequest;
use qubit_task::CancellationMode;
use qubit_task::TaskContext;
use qubit_task::TaskHandlerDescriptor;
use qubit_task::TaskHandler;
use qubit_task::store::TaskFuture;

/// Minimal handler implemented in a crate separate from the service consumer.
pub struct FixtureHandler;

impl TaskHandler<serde_json::Value> for FixtureHandler {
    fn run<'a>(&'a self, _payload: serde_json::Value, _context: TaskContext) -> TaskFuture<'a, TaskRunResult> {
        Box::pin(async { Ok(TaskRunOutcome::Succeeded(TaskOutput::default())) })
    }
}

#[derive(Default)]
pub struct JsonValueCodec;

impl qubit_codec::ValueEncoder<serde_json::Value> for JsonValueCodec {
    type Output = Vec<u8>;
    type Error = serde_json::Error;

    fn encode(&mut self, value: &serde_json::Value) -> Result<Vec<u8>, Self::Error> {
        serde_json::to_vec(value)
    }
}

impl qubit_codec::ValueDecoder<[u8]> for JsonValueCodec {
    type Output = serde_json::Value;
    type Error = serde_json::Error;

    fn decode(&mut self, bytes: &[u8]) -> Result<Self::Output, Self::Error> {
        serde_json::from_slice(bytes)
    }
}

pub static JSON_DESCRIPTOR: ValueBytesCodecDescriptor = ValueBytesCodecDescriptor::of::<JsonValueCodec, serde_json::Value>();
pub static JSON_CODEC: ValueBytesCodecRegistration = ValueCodecRegistration::new(
    ValueCodecId::new("fixture.task.json"),
    &JSON_DESCRIPTOR,
    ValueCodecRegistrationSource::new("qubit-task", "fixture-provider", "fixture", 1),
);

pub fn codec_registry() -> Result<Arc<ValueBytesCodecRegistry>, Box<dyn std::error::Error>> {
    Ok(Arc::new(ValueBytesCodecRegistry::from_registrations([&JSON_CODEC])?))
}

pub fn descriptor(kind_id: &str) -> Result<TaskHandlerDescriptor, Box<dyn std::error::Error>> {
    Ok(TaskHandlerDescriptor {
        kind_id: kind_id.into(),
        payload_type_id: ModelIdBuf::try_from("fixture.TaskPayload")?,
        accepted_schema_versions: vec![1],
        cancellation_mode: CancellationMode::Unsupported,
    })
}

pub fn request(kind_id: &str, payload: serde_json::Value, idempotency_key: String) -> TaskRequest<serde_json::Value> {
    let mut request = TaskRequest::new(
        kind_id,
        ModelId::new("fixture.TaskPayload"),
        1,
        ValueCodecId::new("fixture.task.json"),
        payload,
    );
    request.idempotency_key = Some(idempotency_key);
    request
}
