// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Service-level coverage for the operation feature's cooperative cancel bridge.

#![cfg(feature = "operation")]

use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use qubit_codec::{
    ValueBytesCodecDescriptor, ValueBytesCodecRegistry, ValueCodecId, ValueCodecRegistration,
    ValueCodecRegistrationSource, ValueDecoder, ValueEncoder,
};
use qubit_id::{Id, IdGenerationError, IdGenerator};
use qubit_model_id::{HasModelId, ModelId, ModelIdBuf};
use qubit_task::handler::{TaskRunOutcome, TaskRunResult};
use qubit_task::model::ResourceCapacity;
use qubit_task::service::CancelOutcome;
use qubit_task::store::{MemoryTaskStore, TaskFuture};
use qubit_task::{
    CancellationMode, TaskContext, TaskExecutionServiceBuilder, TaskHandler, TaskHandlerDescriptor,
};
use tokio::sync::Notify;

#[derive(Clone)]
struct Payload;

impl HasModelId for Payload {
    const MODEL_ID: ModelId = ModelId::new("test.OperationCancelPayload");
}

#[derive(Default)]
struct PayloadCodec;

impl ValueEncoder<Payload> for PayloadCodec {
    type Output = Vec<u8>;
    type Error = Infallible;
    fn encode(&mut self, _: &Payload) -> Result<Vec<u8>, Self::Error> {
        Ok(Vec::new())
    }
}

impl ValueDecoder<[u8]> for PayloadCodec {
    type Output = Payload;
    type Error = Infallible;
    fn decode(&mut self, _: &[u8]) -> Result<Payload, Self::Error> {
        Ok(Payload)
    }
}

static CODEC: ValueBytesCodecDescriptor = ValueBytesCodecDescriptor::of::<PayloadCodec, Payload>();
static CODEC_REGISTRATION: qubit_codec::ValueBytesCodecRegistration = ValueCodecRegistration::new(
    ValueCodecId::new("test.operation_cancel"),
    &CODEC,
    ValueCodecRegistrationSource::new("qubit-task", "operation_cancel_tests", file!(), 1),
);

struct Ids(AtomicU64);

impl IdGenerator<Id, IdGenerationError> for Ids {
    fn generate(&self) -> Result<Id, IdGenerationError> {
        Ok(Id::new(self.0.fetch_add(1, Ordering::Relaxed)))
    }
}

struct WaitingHandler(Arc<Notify>);

impl TaskHandler<Payload> for WaitingHandler {
    fn run<'a>(&'a self, _: Payload, context: TaskContext) -> TaskFuture<'a, TaskRunResult> {
        let entered = Arc::clone(&self.0);
        Box::pin(async move {
            entered.notify_one();
            tokio::join!(
                context.wait_cancel_requested(),
                context.wait_cancel_requested()
            );
            Ok(TaskRunOutcome::Cancelled)
        })
    }
}

#[tokio::test]
async fn cooperative_cancellation_reaches_multiple_waiters_and_repeated_cancel() {
    let codecs = Arc::new(
        ValueBytesCodecRegistry::from_registrations([&CODEC_REGISTRATION])
            .expect("payload codec registers"),
    );
    let store = Arc::new(MemoryTaskStore::new(8));
    let entered = Arc::new(Notify::new());
    let mut builder =
        TaskExecutionServiceBuilder::new(store, codecs, Arc::new(Ids(AtomicU64::new(81_000))))
            .capacity(ResourceCapacity::default());
    builder
        .handlers_mut()
        .register::<Payload, _>(
            TaskHandlerDescriptor {
                kind_id: "test.operation_cancel".into(),
                payload_type_id: ModelIdBuf::try_from("test.OperationCancelPayload").unwrap(),
                accepted_schema_versions: vec![1],
                cancellation_mode: CancellationMode::Cooperative,
            },
            Arc::new(WaitingHandler(Arc::clone(&entered))),
        )
        .expect("handler registers");
    let service = builder.build().await.expect("service starts");
    let request = qubit_task::model::TaskRequest::new(
        "test.operation_cancel",
        1,
        ValueCodecId::new("test.operation_cancel"),
        Payload,
    );
    let accepted = service.submit(request).await.expect("task accepted");

    tokio::time::timeout(Duration::from_secs(3), entered.notified())
        .await
        .expect("handler starts");
    assert_eq!(
        service.cancel(accepted.id).await.unwrap(),
        CancelOutcome::CancellationRequested
    );
    assert_eq!(
        service.cancel(accepted.id).await.unwrap(),
        CancelOutcome::CancellationRequested
    );
    let terminal = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Some(summary) = service.get(accepted.id).await.unwrap()
                && summary.state.is_terminal()
            {
                break summary;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("both cancellation waiters complete");
    assert!(matches!(
        terminal.state,
        qubit_task::model::TaskState::Cancelled
    ));
    service.shutdown().await.expect("service shuts down");
}
