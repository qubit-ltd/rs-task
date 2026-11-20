// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Reference integration of a typed task handler with managed operations.
//!
//! This example uses a controlled local Tokio job. It demonstrates lifecycle
//! ownership and release evidence; it is not a production provider adapter.

use std::num::NonZeroU32;
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
use qubit_operation::AsyncOperationManager;
use qubit_operation::ManagerOptions;
use qubit_operation::ShutdownMode;
use qubit_task::handler::CancellationMode;
use qubit_task::handler::TaskHandlerDescriptor;
use qubit_task::model::ResourceCapacity;
use qubit_task::model::TaskOutput;
use qubit_task::model::TaskRequest;
use qubit_task::service::RetryPolicy;
use qubit_task::service::TaskExecutionService;
use qubit_task::service::TaskExecutionServiceBuilder;
use qubit_task::store::MemoryTaskStore;
use serde::Deserialize;
use serde::Serialize;

#[path = "operation_task/external_job.rs"]
mod external_job;
#[path = "operation_task/managed_handler.rs"]
mod managed_handler;
#[cfg(test)]
#[path = "operation_task/scenarios.rs"]
mod scenarios;

use external_job::AttemptGates;
use managed_handler::ManagedHandler;

const MANAGED_KIND: &str = "example.operation.managed";
const PROBE_KIND: &str = "example.operation.probe";
const CODEC_ID: &str = "example.operation.json";
const PAYLOAD_MODEL_ID: &str = "example.ManagedPayload";

#[derive(Debug, Deserialize, Serialize)]
struct ManagedPayload;

impl HasModelId for ManagedPayload {
    const MODEL_ID: ModelId = ModelId::new(PAYLOAD_MODEL_ID);
}

#[derive(Default)]
struct JsonCodec;

impl ValueEncoder<ManagedPayload> for JsonCodec {
    type Output = Vec<u8>;
    type Error = serde_json::Error;

    fn encode(&mut self, value: &ManagedPayload) -> Result<Self::Output, Self::Error> {
        serde_json::to_vec(value)
    }
}

impl ValueDecoder<[u8]> for JsonCodec {
    type Output = ManagedPayload;
    type Error = serde_json::Error;

    fn decode(&mut self, bytes: &[u8]) -> Result<Self::Output, Self::Error> {
        serde_json::from_slice(bytes)
    }
}

static JSON_DESCRIPTOR: ValueBytesCodecDescriptor = ValueBytesCodecDescriptor::of::<JsonCodec, ManagedPayload>();
static JSON_CODEC: ValueBytesCodecRegistration = ValueCodecRegistration::new(
    ValueCodecId::new(CODEC_ID),
    &JSON_DESCRIPTOR,
    ValueCodecRegistrationSource::new("example", "rs-task", "operation_task", 1),
);

struct SequentialIds(AtomicU64);

impl IdGenerator for SequentialIds {
    fn generate(&self) -> Result<Id, IdGenerationError> {
        Ok(Id::new(self.0.fetch_add(1, Ordering::Relaxed)))
    }
}

/// Builds a real typed service and returns the controlled task request.
///
/// Each gate belongs to one attempt. The managed handler indexes this vector
/// by the one-based task attempt number.
async fn fixture(
    manager: AsyncOperationManager,
    gates: Vec<Arc<AttemptGates>>,
    max_attempts: NonZeroU32,
) -> Result<(TaskExecutionService, TaskRequest<ManagedPayload>), Box<dyn std::error::Error>> {
    let codecs = Arc::new(ValueBytesCodecRegistry::from_registrations([&JSON_CODEC])?);
    let attempts = Arc::new(std::sync::Mutex::new(gates));
    let mut builder = TaskExecutionServiceBuilder::new(
        Arc::new(MemoryTaskStore::new(256)),
        codecs,
        Arc::new(SequentialIds(AtomicU64::new(1))),
    )
    .capacity(ResourceCapacity {
        cpu_slots: 1,
        ..ResourceCapacity::default()
    })
    .max_running_tasks(std::num::NonZeroUsize::new(2).expect("two running slots"))
    .max_attempts(max_attempts)
    .retry_policy(RetryPolicy::new(
        std::time::Duration::from_millis(1),
        std::time::Duration::from_millis(1),
    )?);

    builder.handlers_mut().register::<ManagedPayload, _>(
        TaskHandlerDescriptor {
            kind_id: MANAGED_KIND.into(),
            payload_type_id: ModelIdBuf::try_from(PAYLOAD_MODEL_ID)?,
            accepted_schema_versions: vec![1],
            cancellation_mode: CancellationMode::Cooperative,
        },
        Arc::new(ManagedHandler::new(manager, attempts)),
    )?;
    builder.handlers_mut().register::<ManagedPayload, _>(
        TaskHandlerDescriptor {
            kind_id: PROBE_KIND.into(),
            payload_type_id: ModelIdBuf::try_from(PAYLOAD_MODEL_ID)?,
            accepted_schema_versions: vec![1],
            cancellation_mode: CancellationMode::Cooperative,
        },
        Arc::new(ProbeHandler),
    )?;

    let service = builder.build().await?;
    let mut request = TaskRequest::new(MANAGED_KIND, 1, ValueCodecId::new(CODEC_ID), ManagedPayload);
    request.resource_limit.cpu_slots = 1;
    Ok((service, request))
}

/// Creates a one-CPU request for the immediately successful probe handler.
#[cfg(test)]
fn probe_request() -> TaskRequest<ManagedPayload> {
    let mut request = TaskRequest::new(PROBE_KIND, 1, ValueCodecId::new(CODEC_ID), ManagedPayload);
    request.resource_limit.cpu_slots = 1;
    request
}

struct ProbeHandler;

impl qubit_task::handler::TaskHandler<ManagedPayload> for ProbeHandler {
    fn run<'a>(
        &'a self,
        _payload: ManagedPayload,
        _context: qubit_task::handler::TaskContext,
    ) -> qubit_task::store::TaskFuture<'a, qubit_task::handler::TaskRunResult> {
        Box::pin(async { Ok(qubit_task::handler::TaskRunOutcome::Succeeded(TaskOutput::default())) })
    }
}

fn new_operation_manager() -> Result<AsyncOperationManager, qubit_operation::ManagerCreateError> {
    AsyncOperationManager::new(ManagerOptions::default())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
    runtime.block_on(async {
        let manager = new_operation_manager()?;
        let gates = AttemptGates::new();
        let (service, request) = fixture(
            manager.clone(),
            vec![Arc::clone(&gates)],
            NonZeroU32::new(1).expect("one attempt"),
        )
        .await?;
        let accepted = service.submit(request).await?;
        AttemptGates::wait(&gates.activated).await;
        let _cancel = service.cancel(accepted.id).await?;
        AttemptGates::wait(&gates.stop_seen).await;
        println!("已请求停止");
        AttemptGates::wait(&gates.parent_outcome_seen).await;
        AttemptGates::open(&gates.allow_input_release);
        AttemptGates::open(&gates.allow_domain_settlement);
        AttemptGates::wait(&gates.input_released).await;
        println!("输入已释放");
        AttemptGates::open(&gates.allow_cleanup);
        service.shutdown().await?;
        let summary = service.get(accepted.id).await?.expect("task summary");
        println!("任务 {:?}", summary.state);
        let drain = manager.request_shutdown(ShutdownMode::Drain).wait().await?;
        assert_eq!(drain.snapshot.active_operations, 0);
        assert_eq!(drain.snapshot.queued_operations, 0);
        assert_eq!(drain.snapshot.unconfirmed_users, 0);
        assert_eq!(drain.snapshot.cleaning_resources, 0);
        assert_eq!(drain.snapshot.recovery_groups, 0);
        assert_eq!(drain.snapshot.service_resources, 0);
        Ok::<(), Box<dyn std::error::Error>>(())
    })
}
