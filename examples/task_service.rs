// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Submits a typed payload and waits for its task summary.

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
use qubit_task::HandlerOptions;
use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::TaskKind;
use qubit_task::handler::TaskContext;
use qubit_task::handler::TaskRunOutcome;
use qubit_task::model::ResourceCapacity;
use qubit_task::model::TaskOutput;
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
    ValueCodecRegistrationSource::new("example", "rs-task", "task_service", 1),
);

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    runtime.block_on(async {
        let kind = TaskKind::<EchoPayload>::new("example.echo", 1, ValueCodecId::new("example.echo.json"));
        let mut builder =
            TaskExecutionServiceBuilder::in_memory(256, ValueBytesCodecRegistry::from_registrations([&JSON_CODEC])?)
                .capacity(ResourceCapacity {
                    cpu_slots: 4,
                    ..ResourceCapacity::default()
                });

        builder.register_handler_fn_with_options(
            kind.descriptor(),
            |payload: EchoPayload, _context: TaskContext| async move {
                println!("{}", payload.message);
                Ok(TaskRunOutcome::Succeeded(TaskOutput::default()))
            },
            HandlerOptions::new().with_cooperative_cancellation(),
        )?;

        let service = builder.build().await?;
        let request = kind.request(EchoPayload {
            message: "typed task completed".into(),
        });
        let accepted = service.submit(request).await?;
        let checkpoint = service
            .wait_terminal(accepted.summary.id, std::time::Duration::from_secs(5))
            .await?;
        println!("task {} is {:?}", accepted.summary.id, checkpoint.state);
        service.shutdown().await?;
        Ok::<(), Box<dyn std::error::Error>>(())
    })
}
