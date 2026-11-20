// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::error::Error;
use std::time::Duration;

use qubit_codec::ValueBytesCodecDescriptor;
use qubit_codec::ValueBytesCodecRegistration;
use qubit_codec::ValueBytesCodecRegistry;
use qubit_codec::ValueCodecId;
use qubit_codec::ValueCodecRegistration;
use qubit_codec::ValueCodecRegistrationSource;
use qubit_codec::ValueDecoder;
use qubit_codec::ValueEncoder;
use qubit_model_id::HasModelId;
use qubit_model_id::ModelId;
use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::TaskKind;
use qubit_task::TaskState;
use qubit_task::handler::TaskContext;
use qubit_task::handler::TaskRunOutcome;
use qubit_task::handler::TaskRunResult;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Deserialize, Serialize)]
struct EchoPayload {
    message: String,
}

impl HasModelId for EchoPayload {
    const MODEL_ID: ModelId = ModelId::new("example.EchoPayload");
}

#[derive(Default)]
struct JsonCodec;

impl ValueEncoder<EchoPayload> for JsonCodec {
    type Output = Vec<u8>;
    type Error = serde_json::Error;

    fn encode(&mut self, value: &EchoPayload) -> Result<Self::Output, Self::Error> {
        serde_json::to_vec(value)
    }
}

impl ValueDecoder<[u8]> for JsonCodec {
    type Output = EchoPayload;
    type Error = serde_json::Error;

    fn decode(&mut self, bytes: &[u8]) -> Result<Self::Output, Self::Error> {
        serde_json::from_slice(bytes)
    }
}

static JSON_DESCRIPTOR: ValueBytesCodecDescriptor = ValueBytesCodecDescriptor::of::<JsonCodec, EchoPayload>();
static JSON_CODEC: ValueBytesCodecRegistration = ValueCodecRegistration::new(
    ValueCodecId::new("example.echo.json"),
    &JSON_DESCRIPTOR,
    ValueCodecRegistrationSource::new("example", "rs-task", "readme_quick_start", 1),
);

async fn run_echo(payload: EchoPayload, _context: TaskContext) -> TaskRunResult {
    println!("{}", payload.message);
    Ok(TaskRunOutcome::success())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let codecs = ValueBytesCodecRegistry::from_registrations([&JSON_CODEC])?;
    let echo = TaskKind::<EchoPayload>::new("example.echo", 1, ValueCodecId::new("example.echo.json"));
    let mut builder = TaskExecutionServiceBuilder::in_memory(256, codecs);
    builder.register_handler_fn(echo.descriptor(), run_echo)?;

    let service = builder.build().await?;
    let accepted = service
        .submit(echo.request(EchoPayload {
            message: "typed task completed".into(),
        }))
        .await?;
    println!("accepted={} task={}", accepted.created, accepted.summary.id);

    let terminal = service
        .wait_terminal(accepted.summary.id, Duration::from_secs(5))
        .await?;
    assert_eq!(terminal.state, TaskState::Succeeded);
    println!("task {} is {:?}", terminal.id, terminal.state);

    service.shutdown().await?;
    Ok(())
}
