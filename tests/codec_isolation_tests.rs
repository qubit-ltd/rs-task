// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Ensures synchronous task codecs do not block a current-thread Tokio runtime.

use std::convert::Infallible;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use futures::FutureExt;
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
use qubit_task::CancellationMode;
use qubit_task::TaskContext;
use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::TaskHandler;
use qubit_task::TaskHandlerDescriptor;
use qubit_task::handler::TaskRunOutcome;
use qubit_task::handler::TaskRunResult;
use qubit_task::model::TaskOutput;
use qubit_task::model::TaskRequest;
use qubit_task::store::MemoryTaskStore;
use qubit_task::store::TaskFuture;

#[derive(Clone)]
struct Payload(bool);

impl HasModelId for Payload {
    const MODEL_ID: ModelId = ModelId::new("test.CodecIsolationPayload");
}

#[derive(Default)]
struct Gate {
    state: Mutex<(bool, bool)>,
    changed: Condvar,
    reached: tokio::sync::Notify,
}

impl Gate {
    fn block(&self) {
        let mut state = self.state.lock().expect("gate lock");
        state.0 = true;
        self.changed.notify_all();
        self.reached.notify_one();
        while !state.1 {
            state = self.changed.wait(state).expect("gate wait");
        }
    }

    fn wait_until_entered(&self) -> bool {
        let state = self.state.lock().expect("gate lock");
        let (state, timeout) = self
            .changed
            .wait_timeout_while(state, Duration::from_secs(2), |state| !state.0)
            .expect("gate wait");
        state.0 && !timeout.timed_out()
    }

    fn release(&self) {
        let mut state = self.state.lock().expect("gate lock");
        state.1 = true;
        self.changed.notify_all();
    }
}

#[derive(Default)]
struct BlockingCodec;

static ENCODER_GATE: std::sync::OnceLock<Arc<Gate>> = std::sync::OnceLock::new();
static CANCEL_ENCODER_GATE: std::sync::OnceLock<Arc<Gate>> = std::sync::OnceLock::new();
static PANIC_ENCODER: AtomicBool = AtomicBool::new(false);
static DECODER_GATE: std::sync::OnceLock<Arc<Gate>> = std::sync::OnceLock::new();

impl ValueEncoder<Payload> for BlockingCodec {
    type Output = Vec<u8>;
    type Error = Infallible;

    fn encode(&mut self, value: &Payload) -> Result<Self::Output, Self::Error> {
        assert!(
            !PANIC_ENCODER.swap(false, Ordering::AcqRel),
            "intentional encoder panic"
        );
        let gate = if value.0 {
            CANCEL_ENCODER_GATE
                .get()
                .expect("cancel encoder gate installed")
        } else {
            ENCODER_GATE.get().expect("encoder gate installed")
        };
        gate.block();
        Ok(vec![1])
    }
}

impl ValueDecoder<[u8]> for BlockingCodec {
    type Output = Payload;
    type Error = Infallible;

    fn decode(&mut self, _bytes: &[u8]) -> Result<Self::Output, Self::Error> {
        DECODER_GATE.get().expect("decoder gate installed").block();
        Ok(Payload(false))
    }
}

static DESCRIPTOR: ValueBytesCodecDescriptor =
    ValueBytesCodecDescriptor::of::<BlockingCodec, Payload>();
static REGISTRATION: ValueBytesCodecRegistration = ValueCodecRegistration::new(
    ValueCodecId::new("test.codec_isolation"),
    &DESCRIPTOR,
    ValueCodecRegistrationSource::new(
        "qubit-task",
        "codec_isolation_tests",
        "tests/codec_isolation_tests.rs",
        1,
    ),
);

struct Handler;

impl TaskHandler<Payload> for Handler {
    fn run<'a>(
        &'a self,
        _payload: Payload,
        _context: TaskContext,
    ) -> TaskFuture<'a, TaskRunResult> {
        Box::pin(async { Ok(TaskRunOutcome::Succeeded(TaskOutput::default())) })
    }
}

struct Ids(AtomicU64);

impl IdGenerator for Ids {
    fn generate(&self) -> Result<Id, IdGenerationError> {
        Ok(Id::new(self.0.fetch_add(1, Ordering::Relaxed)))
    }
}

#[tokio::test(flavor = "current_thread")]
async fn submit_codec_does_not_block_current_thread_runtime() {
    let gate = Arc::new(Gate::default());
    let decoder_gate = Arc::new(Gate::default());
    let cancel_encoder_gate = Arc::new(Gate::default());
    assert!(
        ENCODER_GATE.set(Arc::clone(&gate)).is_ok(),
        "one codec test"
    );
    assert!(
        DECODER_GATE.set(Arc::clone(&decoder_gate)).is_ok(),
        "one codec test"
    );
    assert!(
        CANCEL_ENCODER_GATE
            .set(Arc::clone(&cancel_encoder_gate))
            .is_ok(),
        "one codec test"
    );
    let codecs = Arc::new(
        ValueBytesCodecRegistry::from_registrations([&REGISTRATION])
            .expect("codec registry builds"),
    );
    let ids = Arc::new(Ids(AtomicU64::new(1)));
    let mut builder = TaskExecutionServiceBuilder::new(
        Arc::new(MemoryTaskStore::new(100)),
        codecs,
        Arc::clone(&ids) as Arc<dyn IdGenerator<Id, IdGenerationError>>,
    );
    builder
        .handlers_mut()
        .register::<Payload, _>(
            TaskHandlerDescriptor {
                kind_id: "test.codec".into(),
                payload_type_id: ModelIdBuf::parse("test.CodecIsolationPayload")
                    .expect("valid model id"),
                accepted_schema_versions: vec![1],
                cancellation_mode: CancellationMode::Cooperative,
            },
            Arc::new(Handler),
        )
        .expect("handler registers");
    let service = builder.build().await.expect("service builds");

    let heartbeats = Arc::new(AtomicUsize::new(0));
    let heartbeat_count = Arc::clone(&heartbeats);
    let heartbeat = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(2)).await;
            heartbeat_count.fetch_add(1, Ordering::Relaxed);
        }
    });
    tokio::task::yield_now().await;

    let release_gate = Arc::clone(&gate);
    let observed_heartbeats = Arc::clone(&heartbeats);
    let releaser = std::thread::spawn(move || {
        let entered = release_gate.wait_until_entered();
        if entered {
            std::thread::sleep(Duration::from_millis(60));
        }
        let count = observed_heartbeats.load(Ordering::Relaxed);
        release_gate.release();
        (entered, count)
    });

    let request = TaskRequest::new(
        "test.codec",
        1,
        ValueCodecId::new("test.codec_isolation"),
        Payload(false),
    );
    let result = service.submit(request).await;
    let (encoder_entered, count_at_release) =
        releaser.join().expect("encoder releaser thread joins");
    assert!(encoder_entered, "encoder gate was reached");
    let accepted = result.expect("submit finishes after encoder release");

    let cancel_release_gate = Arc::clone(&cancel_encoder_gate);
    let cancel_observed_heartbeats = Arc::clone(&heartbeats);
    let cancel_releaser = std::thread::spawn(move || {
        let entered = cancel_release_gate.wait_until_entered();
        if entered {
            std::thread::sleep(Duration::from_millis(60));
        }
        let count = cancel_observed_heartbeats.load(Ordering::Relaxed);
        cancel_release_gate.release();
        (entered, count)
    });
    let cancel_service = service.clone();
    let cancelled_submit = tokio::spawn(async move {
        cancel_service
            .submit(TaskRequest::new(
                "test.codec",
                1,
                ValueCodecId::new("test.codec_isolation"),
                Payload(true),
            ))
            .await
    });
    tokio::time::timeout(
        Duration::from_secs(2),
        cancel_encoder_gate.reached.notified(),
    )
    .await
    .expect("second encoder reaches cancellation gate");
    cancelled_submit.abort();
    assert!(
        cancelled_submit
            .await
            .expect_err("cancelled submit task")
            .is_cancelled()
    );
    let (cancel_encoder_entered, cancel_encoder_heartbeats) = cancel_releaser
        .join()
        .expect("cancel encoder releaser joins");
    assert!(cancel_encoder_entered, "cancel encoder gate was reached");
    assert!(
        cancel_encoder_heartbeats > 0,
        "runtime heartbeat advances during cancelled encoding"
    );
    assert_eq!(
        ids.0.load(Ordering::Relaxed),
        2,
        "cancelled encoding does not allocate an ID"
    );

    let decoder_release_gate = Arc::clone(&decoder_gate);
    let observed_heartbeats = Arc::clone(&heartbeats);
    let decoder_releaser = std::thread::spawn(move || {
        let entered = decoder_release_gate.wait_until_entered();
        if entered {
            std::thread::sleep(Duration::from_millis(60));
        }
        let count = observed_heartbeats.load(Ordering::Relaxed);
        decoder_release_gate.release();
        (entered, count)
    });
    // A bounded wait proves shutdown retains the in-flight task while decode is blocked.
    tokio::time::timeout(Duration::from_secs(2), decoder_gate.reached.notified())
        .await
        .expect("decoder enters before cancellation and shutdown checks");
    let _ = service
        .cancel(accepted.id)
        .await
        .expect("cancel during preparation");
    let summary = service
        .get(accepted.id)
        .await
        .expect("read cancellation state")
        .expect("task is retained");
    assert!(
        summary.cancel_requested,
        "cancellation intent remains set during decoder preparation"
    );
    PANIC_ENCODER.store(true, Ordering::Release);
    let panic_submit = std::panic::AssertUnwindSafe(service.submit(TaskRequest::new(
        "test.codec",
        1,
        ValueCodecId::new("test.codec_isolation"),
        Payload(false),
    )))
    .catch_unwind()
    .await;
    assert!(
        panic_submit.is_err(),
        "encoder panic continues to unwind through submit"
    );

    let shutdown_early = tokio::time::timeout(Duration::from_millis(20), service.shutdown()).await;
    let (decoder_entered, decoder_count_at_release) = decoder_releaser
        .join()
        .expect("decoder releaser thread joins");
    let shutdown = service.shutdown().await;
    heartbeat.abort();

    assert!(
        count_at_release > 0,
        "runtime heartbeat must advance while encoder is blocked"
    );
    assert!(
        shutdown_early.is_err(),
        "shutdown waits for in-flight decoder preparation"
    );
    assert!(decoder_entered, "decoder gate was reached");
    assert!(
        decoder_count_at_release > 0,
        "runtime heartbeat must advance while decoder is blocked"
    );
    assert!(
        shutdown.is_ok(),
        "shutdown completes after decoder returns: {shutdown:?}"
    );
}
