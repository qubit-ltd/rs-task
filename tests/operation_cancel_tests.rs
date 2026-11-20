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
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use qubit_codec::{
    ValueBytesCodecDescriptor, ValueBytesCodecRegistry, ValueCodecId, ValueCodecRegistration,
    ValueCodecRegistrationSource, ValueDecoder, ValueEncoder,
};
use qubit_id::{Id, IdGenerationError, IdGenerator};
use qubit_model_id::{HasModelId, ModelId, ModelIdBuf};
use qubit_task::handler::{TaskRunOutcome, TaskRunResult};
use qubit_task::model::{ResourceCapacity, TaskRunError};
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

struct ModeProbeHandler {
    entered: Arc<Notify>,
    release: Arc<Notify>,
    wait_completed: Arc<AtomicBool>,
}

impl TaskHandler<Payload> for ModeProbeHandler {
    fn run<'a>(&'a self, _: Payload, context: TaskContext) -> TaskFuture<'a, TaskRunResult> {
        let entered = Arc::clone(&self.entered);
        let release = Arc::clone(&self.release);
        let wait_completed = Arc::clone(&self.wait_completed);
        Box::pin(async move {
            entered.notify_one();
            tokio::select! {
                _ = context.wait_cancel_requested() => {
                    wait_completed.store(true, Ordering::Release);
                }
                _ = release.notified() => {}
            }
            Ok(TaskRunOutcome::Succeeded(Default::default()))
        })
    }
}

struct AttemptHandler {
    entered: Arc<Notify>,
    contexts: Arc<Mutex<Vec<TaskContext>>>,
}

impl TaskHandler<Payload> for AttemptHandler {
    fn run<'a>(&'a self, _: Payload, context: TaskContext) -> TaskFuture<'a, TaskRunResult> {
        let entered = Arc::clone(&self.entered);
        let contexts = Arc::clone(&self.contexts);
        Box::pin(async move {
            let attempt = context.attempt();
            contexts
                .lock()
                .expect("contexts mutex")
                .push(context.clone());
            if attempt == 1 {
                return Err(TaskRunError {
                    category: "retryable".into(),
                    message: "advance to the next attempt".into(),
                    retryable: true,
                });
            }
            entered.notify_one();
            context.wait_cancel_requested().await;
            Ok(TaskRunOutcome::Cancelled)
        })
    }
}

async fn build_service<H: TaskHandler<Payload> + 'static>(
    mode: CancellationMode,
    handler: Arc<H>,
    with_hook: bool,
) -> qubit_task::service::TaskExecutionService {
    let codecs = Arc::new(
        ValueBytesCodecRegistry::from_registrations([&CODEC_REGISTRATION])
            .expect("payload codec registers"),
    );
    let store = Arc::new(MemoryTaskStore::new(8));
    let mut builder =
        TaskExecutionServiceBuilder::new(store, codecs, Arc::new(Ids(AtomicU64::new(81_000))))
            .capacity(ResourceCapacity::default());
    let descriptor = TaskHandlerDescriptor {
        kind_id: "test.operation_cancel".into(),
        payload_type_id: ModelIdBuf::try_from("test.OperationCancelPayload").unwrap(),
        accepted_schema_versions: vec![1],
        cancellation_mode: mode,
    };
    if with_hook {
        builder
            .handlers_mut()
            .register_with_cancellation_hook::<Payload, _>(
                descriptor,
                handler,
                "operation cancellation test",
                Arc::new(|_, _| Box::pin(async { Ok(()) })),
            )
            .expect("external hook handler registers");
    } else {
        builder
            .handlers_mut()
            .register::<Payload, _>(descriptor, handler)
            .expect("handler registers");
    }
    builder.build().await.expect("service starts")
}

fn request() -> qubit_task::model::TaskRequest<Payload> {
    qubit_task::model::TaskRequest::new(
        "test.operation_cancel",
        1,
        ValueCodecId::new("test.operation_cancel"),
        Payload,
    )
}

#[tokio::test]
async fn cooperative_cancellation_reaches_multiple_waiters_and_repeated_cancel() {
    let entered = Arc::new(Notify::new());
    let service = build_service(
        CancellationMode::Cooperative,
        Arc::new(WaitingHandler(Arc::clone(&entered))),
        false,
    )
    .await;
    let accepted = service.submit(request()).await.expect("task accepted");

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

async fn cancellation_mode_does_not_publish_operation_wait(
    mode: CancellationMode,
    with_hook: bool,
    expected: CancelOutcome,
) {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let wait_completed = Arc::new(AtomicBool::new(false));
    let service = build_service(
        mode,
        Arc::new(ModeProbeHandler {
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
            wait_completed: Arc::clone(&wait_completed),
        }),
        with_hook,
    )
    .await;
    let accepted = service.submit(request()).await.expect("task accepted");
    tokio::time::timeout(Duration::from_secs(3), entered.notified())
        .await
        .expect("handler starts and polls operation wait");

    assert_eq!(service.cancel(accepted.id).await.unwrap(), expected);
    tokio::task::yield_now().await;
    assert!(!wait_completed.load(Ordering::Acquire));
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if service
                .get(accepted.id)
                .await
                .unwrap()
                .is_some_and(|summary| summary.state.is_terminal())
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("manual release lets handler finish");
    assert!(!wait_completed.load(Ordering::Acquire));
    service.shutdown().await.expect("service shuts down");
}

#[tokio::test]
async fn unsupported_cancellation_does_not_publish_operation_wait() {
    cancellation_mode_does_not_publish_operation_wait(
        CancellationMode::Unsupported,
        false,
        CancelOutcome::CancellationUnsupported,
    )
    .await;
}

#[tokio::test]
async fn external_hook_cancellation_does_not_publish_operation_wait() {
    cancellation_mode_does_not_publish_operation_wait(
        CancellationMode::ExternalHook,
        true,
        CancelOutcome::CancellationRequested,
    )
    .await;
}

#[tokio::test]
async fn cancellation_is_isolated_between_attempts() {
    let entered = Arc::new(Notify::new());
    let contexts = Arc::new(Mutex::new(Vec::new()));
    let service = build_service(
        CancellationMode::Cooperative,
        Arc::new(AttemptHandler {
            entered: Arc::clone(&entered),
            contexts: Arc::clone(&contexts),
        }),
        false,
    )
    .await;
    let accepted = service.submit(request()).await.expect("task accepted");
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .expect("second attempt starts");
    let attempt_contexts = contexts.lock().expect("contexts mutex").clone();
    assert_eq!(attempt_contexts.len(), 2);
    assert_eq!(attempt_contexts[0].attempt(), 1);
    assert_eq!(attempt_contexts[1].attempt(), 2);

    assert_eq!(
        service.cancel(accepted.id).await.unwrap(),
        CancelOutcome::CancellationRequested
    );
    tokio::time::timeout(
        Duration::from_secs(1),
        attempt_contexts[1].wait_cancel_requested(),
    )
    .await
    .expect("current attempt observes cancellation");
    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            attempt_contexts[0].wait_cancel_requested()
        )
        .await
        .is_err()
    );
    service.shutdown().await.expect("service shuts down");
}
