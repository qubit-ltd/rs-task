// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::error::Error;
use std::sync::Arc;

use qubit_codec::ValueBytesCodecDescriptor;
use qubit_codec::ValueBytesCodecRegistration;
use qubit_codec::ValueBytesCodecRegistry;
use qubit_codec::ValueDecoder;
use qubit_codec::ValueEncoder;
use qubit_codec::ValueCodecId;
use qubit_codec::ValueCodecRegistration;
use qubit_codec::ValueCodecRegistrationSource;
use qubit_model_id::HasModelId;
use qubit_model_id::ModelId;
use qubit_model_id::ModelIdBuf;
use qubit_task::handler::TaskRunOutcome;
use qubit_task::handler::TaskRunResult;
use qubit_task::model::TaskOutput;
use qubit_task::model::TaskRequest;
use qubit_task::CancellationMode;
use qubit_task::TaskContext;
use qubit_task::TaskHandler;
use qubit_task::TaskHandlerDescriptor;
use qubit_task::store::TaskFuture;
use serde_json::Error as JsonError;
use serde_json::Value;
use serde_json::from_slice;
use serde_json::to_vec;

pub struct FixturePayload(pub Value);

impl HasModelId for FixturePayload {
    const MODEL_ID: ModelId = ModelId::new("fixture.TaskPayload");
}

/// Minimal handler implemented in a crate separate from the service consumer.
pub struct FixtureHandler;

impl TaskHandler<FixturePayload> for FixtureHandler {
    fn run<'a>(&'a self, _payload: FixturePayload, _context: TaskContext) -> TaskFuture<'a, TaskRunResult> {
        Box::pin(async { Ok(TaskRunOutcome::Succeeded(TaskOutput::default())) })
    }
}

#[derive(Default)]
pub struct JsonValueCodec;

impl ValueEncoder<FixturePayload> for JsonValueCodec {
    type Output = Vec<u8>;
    type Error = JsonError;

    fn encode(&mut self, value: &FixturePayload) -> Result<Vec<u8>, Self::Error> {
        to_vec(&value.0)
    }
}

impl ValueDecoder<[u8]> for JsonValueCodec {
    type Output = FixturePayload;
    type Error = JsonError;

    fn decode(&mut self, bytes: &[u8]) -> Result<Self::Output, Self::Error> {
        from_slice(bytes).map(FixturePayload)
    }
}

pub static JSON_DESCRIPTOR: ValueBytesCodecDescriptor = ValueBytesCodecDescriptor::of::<JsonValueCodec, FixturePayload>();
pub static JSON_CODEC: ValueBytesCodecRegistration = ValueCodecRegistration::new(
    ValueCodecId::new("fixture.task.json"),
    &JSON_DESCRIPTOR,
    ValueCodecRegistrationSource::new("qubit-task", "fixture-provider", "fixture", 1),
);

pub fn codec_registry() -> Result<Arc<ValueBytesCodecRegistry>, Box<dyn Error>> {
    Ok(Arc::new(ValueBytesCodecRegistry::from_registrations([&JSON_CODEC])?))
}

pub fn descriptor(kind_id: &str) -> Result<TaskHandlerDescriptor, Box<dyn Error>> {
    Ok(TaskHandlerDescriptor {
        kind_id: kind_id.into(),
        payload_type_id: ModelIdBuf::try_from("fixture.TaskPayload")?,
        accepted_schema_versions: vec![1],
        cancellation_mode: CancellationMode::Unsupported,
    })
}

pub fn request(kind_id: &str, payload: Value, idempotency_key: String) -> TaskRequest<FixturePayload> {
    let mut request = TaskRequest::new(
        kind_id,
        1,
        ValueCodecId::new("fixture.task.json"),
        FixturePayload(payload),
    );
    request.idempotency_key = Some(idempotency_key);
    request
}
