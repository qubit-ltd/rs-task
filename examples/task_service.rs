// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Submits a typed payload and waits for its task summary.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use qubit_codec::ValueBytesCodecDescriptor;
use qubit_codec::ValueBytesCodecRegistration;
use qubit_codec::ValueBytesCodecRegistry;
use qubit_codec::ValueCodecId;
use qubit_codec::ValueCodecRegistration;
use qubit_codec::ValueCodecRegistrationSource;
use qubit_model_metadata::metadata::ModelId;
use qubit_model_metadata::metadata::ModelIdBuf;
use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::TaskHandler;
use qubit_task::handler::CancellationMode;
use qubit_task::handler::TaskContext;
use qubit_task::handler::TaskHandlerDescriptor;
use qubit_task::handler::TaskRunOutcome;
use qubit_task::handler::TaskRunResult;
use qubit_task::model::ResourceCapacity;
use qubit_task::model::TaskOutput;
use qubit_task::model::TaskRequest;
use qubit_task::store::MemoryTaskStore;
use qubit_task::store::TaskFuture;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Deserialize, Serialize)]
struct EchoPayload {
    message: String,
}

#[derive(Default)]
struct JsonCodec;

impl qubit_codec::ValueEncoder<EchoPayload> for JsonCodec {
    type Output = Vec<u8>;
    type Error = serde_json::Error;

    fn encode(&mut self, value: &EchoPayload) -> Result<Self::Output, Self::Error> {
        serde_json::to_vec(value)
    }
}

impl qubit_codec::ValueDecoder<[u8]> for JsonCodec {
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

struct EchoHandler;

impl TaskHandler<EchoPayload> for EchoHandler {
    fn run<'a>(&'a self, payload: EchoPayload, _context: TaskContext) -> TaskFuture<'a, TaskRunResult> {
        Box::pin(async move {
            println!("{}", payload.message);
            Ok(TaskRunOutcome::Succeeded(TaskOutput::default()))
        })
    }
}

struct SequentialIds(AtomicU64);

impl qubit_id::IdGenerator for SequentialIds {
    fn generate(&self) -> Result<qubit_id::Id, qubit_id::IdGenerationError> {
        Ok(qubit_id::Id::new(self.0.fetch_add(1, Ordering::Relaxed)))
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    runtime.block_on(async {
        let codec_registry = Arc::new(ValueBytesCodecRegistry::from_registrations([&JSON_CODEC])?);
        let mut builder = TaskExecutionServiceBuilder::new(
            Arc::new(MemoryTaskStore::new(256)),
            codec_registry,
            Arc::new(SequentialIds(AtomicU64::new(1))),
        )
        .capacity(ResourceCapacity {
            cpu_slots: 4,
            ..ResourceCapacity::default()
        });

        builder.handlers_mut().register::<EchoPayload, _>(
            TaskHandlerDescriptor {
                kind_id: "example.echo".into(),
                payload_type_id: ModelIdBuf::try_from("example.EchoPayload")?,
                accepted_schema_versions: vec![1],
                cancellation_mode: CancellationMode::Cooperative,
            },
            Arc::new(EchoHandler),
        )?;

        let service = builder.build().await?;
        let request = TaskRequest::new(
            "example.echo",
            ModelId::new("example.EchoPayload"),
            1,
            ValueCodecId::new("example.echo.json"),
            EchoPayload {
                message: "typed task completed".into(),
            },
        );
        let accepted = service.submit(request).await?;
        let summary = service.get(accepted.id).await?.expect("accepted task exists");
        println!("task {} is {:?}", accepted.id, summary.state);
        service.shutdown().await?;
        Ok::<(), Box<dyn std::error::Error>>(())
    })
}
