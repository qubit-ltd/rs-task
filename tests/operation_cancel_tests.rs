// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Service-level coverage for the operation feature's cooperative cancel
//! bridge.

use std::convert::Infallible;
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use qubit_codec::ValueBytesCodecDescriptor;
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
use qubit_task::CancellationMode;
use qubit_task::HandlerOptions;
use qubit_task::TaskAttempt;
use qubit_task::TaskCancellation;
use qubit_task::TaskContext;
use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::TaskHandler;
use qubit_task::TaskHandlerDescriptor;
use qubit_task::handler::TaskRunOutcome;
use qubit_task::handler::TaskRunResult;
use qubit_task::model::ResourceCapacity;
use qubit_task::model::TaskRunError;
use qubit_task::service::CancelOutcome;
use qubit_task::store::MemoryTaskStore;
use qubit_task::store::TaskFuture;
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
            tokio::join!(context.wait_cancel_requested(), context.wait_cancel_requested());
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
    contexts: Arc<Mutex<Vec<(TaskAttempt, TaskCancellation)>>>,
}

struct ExternalIntentHandler {
    entered: Arc<Notify>,
    check_intent: Arc<Notify>,
    release: Arc<Notify>,
    requested: Arc<AtomicBool>,
    requested_after_hook: Arc<AtomicBool>,
    cooperative_wait_pending: Arc<AtomicBool>,
}

impl TaskHandler<Payload> for ExternalIntentHandler {
    fn run<'a>(&'a self, _: Payload, context: TaskContext) -> TaskFuture<'a, TaskRunResult> {
        let entered = Arc::clone(&self.entered);
        let check_intent = Arc::clone(&self.check_intent);
        let release = Arc::clone(&self.release);
        let requested = Arc::clone(&self.requested);
        let requested_after_hook = Arc::clone(&self.requested_after_hook);
        let cooperative_wait_pending = Arc::clone(&self.cooperative_wait_pending);
        Box::pin(async move {
            entered.notify_one();
            check_intent.notified().await;
            let cancellation = context.cancellation();
            requested.store(cancellation.is_requested(), Ordering::Release);
            let mut cooperative_wait = std::pin::pin!(cancellation.wait_requested());
            let pending =
                std::future::poll_fn(|cx| std::task::Poll::Ready(cooperative_wait.as_mut().poll(cx).is_pending()))
                    .await;
            cooperative_wait_pending.store(pending, Ordering::Release);
            entered.notify_one();
            release.notified().await;
            requested_after_hook.store(cancellation.is_requested(), Ordering::Release);
            Ok(TaskRunOutcome::Succeeded(Default::default()))
        })
    }
}

impl TaskHandler<Payload> for AttemptHandler {
    fn run<'a>(&'a self, _: Payload, context: TaskContext) -> TaskFuture<'a, TaskRunResult> {
        let entered = Arc::clone(&self.entered);
        let contexts = Arc::clone(&self.contexts);
        Box::pin(async move {
            let attempt = context.task_attempt();
            contexts
                .lock()
                .expect("contexts mutex")
                .push((attempt, context.cancellation()));
            if attempt.attempt() == 1 {
                return Err(TaskRunError {
                    category: "retryable".into(),
                    message: "advance to the next attempt".into(),
                    retryable: true,
                });
            }
            entered.notify_one();
            context.cancellation().wait_requested().await;
            Ok(TaskRunOutcome::Cancelled)
        })
    }
}

async fn build_service<H: TaskHandler<Payload> + 'static>(
    mode: CancellationMode,
    handler: Arc<H>,
    with_hook: bool,
) -> qubit_task::service::TaskExecutionService {
    let codecs =
        Arc::new(ValueBytesCodecRegistry::from_registrations([&CODEC_REGISTRATION]).expect("payload codec registers"));
    let store = Arc::new(MemoryTaskStore::new(8));
    let mut builder = TaskExecutionServiceBuilder::new(store, codecs, Arc::new(Ids(AtomicU64::new(81_000))))
        .capacity(ResourceCapacity::default());
    let descriptor = TaskHandlerDescriptor::for_payload::<Payload>("test.operation_cancel", 1);
    let options = match mode {
        CancellationMode::Cooperative => HandlerOptions::new().with_cooperative_cancellation(),
        CancellationMode::ExternalHook if with_hook => HandlerOptions::new()
            .with_source("operation cancellation test")
            .with_external_cancellation(|_attempt| async { Ok(()) }),
        _ => HandlerOptions::new(),
    };
    builder
        .register_handler_with_options::<Payload, _>(descriptor, handler, options)
        .expect("handler registers");
    builder.build().await.expect("service starts")
}

async fn build_slow_failing_external_service<H: TaskHandler<Payload> + 'static>(
    handler: Arc<H>,
    hook_entered: Arc<Notify>,
    hook_release: Arc<Notify>,
) -> qubit_task::service::TaskExecutionService {
    let codecs =
        Arc::new(ValueBytesCodecRegistry::from_registrations([&CODEC_REGISTRATION]).expect("payload codec registers"));
    let store = Arc::new(MemoryTaskStore::new(8));
    let mut builder = TaskExecutionServiceBuilder::new(store, codecs, Arc::new(Ids(AtomicU64::new(82_000))))
        .capacity(ResourceCapacity::default());
    let descriptor = TaskHandlerDescriptor::for_payload::<Payload>("test.operation_cancel", 1);
    builder
        .register_handler_with_options::<Payload, _>(
            descriptor,
            handler,
            HandlerOptions::new().with_external_cancellation(move |_attempt| {
                let entered = Arc::clone(&hook_entered);
                let release = Arc::clone(&hook_release);
                async move {
                    entered.notify_one();
                    release.notified().await;
                    Err(TaskRunError {
                        category: "external-cancel".into(),
                        message: "hook failed after intent publication".into(),
                        retryable: false,
                    })
                }
            }),
        )
        .expect("handler registers");
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
async fn test_cooperative_cancellation_reaches_multiple_waiters_and_repeated_cancel() {
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
        service.request_cancel(accepted.summary.id).await.unwrap(),
        CancelOutcome::CancellationRequested
    );
    assert_eq!(
        service.request_cancel(accepted.summary.id).await.unwrap(),
        CancelOutcome::CancellationRequested
    );
    let terminal = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Some(summary) = service.get_summary(accepted.summary.id).await.unwrap()
                && summary.state.is_terminal()
            {
                break summary;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("both cancellation waiters complete");
    assert!(matches!(terminal.state, qubit_task::model::TaskState::Cancelled));
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

    assert_eq!(service.request_cancel(accepted.summary.id).await.unwrap(), expected);
    tokio::task::yield_now().await;
    assert!(!wait_completed.load(Ordering::Acquire));
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if service
                .get_summary(accepted.summary.id)
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
async fn test_unsupported_cancellation_does_not_publish_operation_wait() {
    cancellation_mode_does_not_publish_operation_wait(
        CancellationMode::Unsupported,
        false,
        CancelOutcome::CancellationUnsupported,
    )
    .await;
}

#[tokio::test]
async fn test_external_hook_cancellation_does_not_publish_operation_wait() {
    cancellation_mode_does_not_publish_operation_wait(
        CancellationMode::ExternalHook,
        true,
        CancelOutcome::CancellationRequested,
    )
    .await;
}

#[tokio::test]
async fn test_external_intent_is_published_before_slow_hook_and_survives_failure() {
    let handler_entered = Arc::new(Notify::new());
    let check_intent = Arc::new(Notify::new());
    let handler_release = Arc::new(Notify::new());
    let hook_entered = Arc::new(Notify::new());
    let hook_release = Arc::new(Notify::new());
    let requested = Arc::new(AtomicBool::new(false));
    let requested_after_hook = Arc::new(AtomicBool::new(false));
    let cooperative_wait_pending = Arc::new(AtomicBool::new(false));
    let service = build_slow_failing_external_service(
        Arc::new(ExternalIntentHandler {
            entered: Arc::clone(&handler_entered),
            check_intent: Arc::clone(&check_intent),
            release: Arc::clone(&handler_release),
            requested: Arc::clone(&requested),
            requested_after_hook: Arc::clone(&requested_after_hook),
            cooperative_wait_pending: Arc::clone(&cooperative_wait_pending),
        }),
        Arc::clone(&hook_entered),
        Arc::clone(&hook_release),
    )
    .await;
    let accepted = service.submit(request()).await.expect("task accepted");
    tokio::time::timeout(Duration::from_secs(3), handler_entered.notified())
        .await
        .expect("handler starts");

    let service_for_cancel = service.clone();
    let cancel = tokio::spawn(async move { service_for_cancel.request_cancel(accepted.summary.id).await });
    tokio::time::timeout(Duration::from_secs(3), hook_entered.notified())
        .await
        .expect("external hook starts");
    check_intent.notify_one();
    tokio::time::timeout(Duration::from_secs(3), handler_entered.notified())
        .await
        .expect("handler observed cancellation while hook is blocked");
    assert!(requested.load(Ordering::Acquire));
    assert!(cooperative_wait_pending.load(Ordering::Acquire));

    hook_release.notify_one();
    let error = tokio::time::timeout(Duration::from_secs(3), cancel)
        .await
        .expect("hook result returned")
        .expect("cancel task joins")
        .expect_err("hook failure is reported");
    assert!(matches!(
        error,
        qubit_task::service::TaskServiceError::ExternalCancellationFailed { .. }
    ));
    assert!(
        requested.load(Ordering::Acquire),
        "failed hooks leave the persisted intent visible"
    );

    handler_release.notify_one();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if service
                .get_summary(accepted.summary.id)
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
    .expect("handler completes");
    assert!(requested_after_hook.load(Ordering::Acquire));
    service.shutdown().await.expect("service shuts down");
}

#[tokio::test]
async fn test_cancellation_is_isolated_between_attempts() {
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
    assert_eq!(attempt_contexts[0].0.attempt(), 1);
    assert_eq!(attempt_contexts[1].0.attempt(), 2);

    assert_eq!(
        service.request_cancel(accepted.summary.id).await.unwrap(),
        CancelOutcome::CancellationRequested
    );
    tokio::time::timeout(Duration::from_secs(1), attempt_contexts[1].1.wait_requested())
        .await
        .expect("current attempt observes cancellation");
    assert!(
        tokio::time::timeout(Duration::from_millis(20), attempt_contexts[0].1.wait_requested())
            .await
            .is_err()
    );
    service.shutdown().await.expect("service shuts down");
}
