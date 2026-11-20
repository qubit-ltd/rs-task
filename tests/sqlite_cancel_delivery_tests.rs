// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
#![cfg(feature = "sqlite")]

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use qubit_codec::ValueBytesCodecDescriptor;
use qubit_codec::ValueBytesCodecRegistration;
use qubit_codec::ValueBytesCodecRegistry;
use qubit_codec::ValueCodecId;
use qubit_codec::ValueCodecRegistrationSource;
use qubit_id::Id;
use qubit_id::IdGenerationError;
use qubit_id::IdGenerator;
use qubit_model_id::HasModelId;
use qubit_model_id::ModelId;
use qubit_task::HandlerOptions;
use qubit_task::TaskContext;
use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::TaskHandlerDescriptor;
use qubit_task::handler::TaskRunOutcome;
use qubit_task::model::TaskRequest;
use qubit_task::model::TaskState;
use qubit_task::model::TransitionCommand;
use qubit_task::service::TaskServiceError;
use qubit_task::store::SqliteTaskStore;
use qubit_task::store::StoreError;
use qubit_task::store::TaskFuture;
use qubit_task::store::TaskStore;
use tokio::sync::Notify;

#[derive(Clone)]
struct Payload;
impl HasModelId for Payload {
    const MODEL_ID: ModelId = ModelId::new("tests.CancelPayload");
}

#[derive(Default)]
struct Codec;
impl qubit_codec::ValueEncoder<Payload> for Codec {
    type Output = Vec<u8>;
    type Error = std::convert::Infallible;
    fn encode(&mut self, _: &Payload) -> Result<Vec<u8>, Self::Error> {
        Ok(vec![])
    }
}
impl qubit_codec::ValueDecoder<[u8]> for Codec {
    type Output = Payload;
    type Error = std::io::Error;
    fn decode(&mut self, _: &[u8]) -> Result<Payload, Self::Error> {
        Ok(Payload)
    }
}
static CODEC_DESC: ValueBytesCodecDescriptor = ValueBytesCodecDescriptor::of::<Codec, Payload>();
static CODEC: ValueBytesCodecRegistration = ValueBytesCodecRegistration::new(
    ValueCodecId::new("tests.cancel.codec"),
    &CODEC_DESC,
    ValueCodecRegistrationSource::new(
        "qubit-task",
        "sqlite_cancel_delivery_tests",
        "tests/sqlite_cancel_delivery_tests.rs",
        1,
    ),
);

struct Ids(AtomicU64);
impl IdGenerator for Ids {
    fn generate(&self) -> Result<Id, IdGenerationError> {
        Ok(Id::new(self.0.fetch_add(1, Ordering::Relaxed)))
    }
}

struct GateStore {
    inner: Arc<SqliteTaskStore>,
    cancelled: Notify,
    release: Notify,
    owner_released: AtomicBool,
    gate_used: AtomicBool,
    conflict_next_cancel: AtomicBool,
    block_before_commit: bool,
    cancel_committed: AtomicBool,
    panic_next_cancel: AtomicBool,
}

struct OpenGateOnDrop(Arc<GateStore>);

impl Drop for OpenGateOnDrop {
    fn drop(&mut self) {
        self.0.release.notify_waiters();
        self.0.release.notify_one();
    }
}

impl TaskStore for GateStore {
    qubit_task::delegate_task_store!(inner; except [transition_encoded, release_owner]);
    fn transition_encoded<'a>(
        &'a self,
        command: TransitionCommand,
    ) -> TaskFuture<'a, Result<qubit_task::model::TaskSummary, qubit_task::store::StoreError>> {
        Box::pin(async move {
            let is_cancel_transition = command.state == TaskState::Cancelled
                || (command.state == TaskState::Running && command.cancel_requested);
            if is_cancel_transition && self.panic_next_cancel.swap(false, Ordering::AcqRel) {
                panic!("controlled cancellation transition panic");
            }
            if command.state == TaskState::Running
                && command.cancel_requested
                && self.conflict_next_cancel.swap(false, Ordering::AcqRel)
            {
                return Err(StoreError::Conflict);
            }
            let is_cancelled = is_cancel_transition && !self.gate_used.swap(true, Ordering::AcqRel);
            if is_cancelled && self.block_before_commit {
                self.cancelled.notify_one();
                self.release.notified().await;
            }
            let result = self.inner.transition_encoded(command).await;
            if is_cancelled && result.is_ok() {
                self.cancel_committed.store(true, Ordering::Release);
            }
            if is_cancelled && !self.block_before_commit && result.is_ok() {
                self.cancelled.notify_one();
                self.release.notified().await;
            }
            result
        })
    }
    fn release_owner<'a>(
        &'a self,
        epoch: qubit_task::model::OwnerEpoch,
    ) -> TaskFuture<'a, Result<(), qubit_task::store::StoreError>> {
        Box::pin(async move {
            self.inner.release_owner(epoch).await?;
            self.owner_released.store(true, Ordering::Release);
            Ok(())
        })
    }
}

fn temp_db() -> std::path::PathBuf {
    std::env::temp_dir().join(format!("sqlite-cancel-delivery-{}.sqlite", uuid::Uuid::new_v4()))
}

#[tokio::test]
async fn test_blocked_task_cancel_write_keeps_store_owner_until_cas_returns() {
    let path = temp_db();
    let sqlite = Arc::new(SqliteTaskStore::open(&path).expect("sqlite opens"));
    let gate = Arc::new(GateStore {
        inner: sqlite.clone(),
        cancelled: Notify::new(),
        release: Notify::new(),
        owner_released: AtomicBool::new(false),
        gate_used: AtomicBool::new(false),
        conflict_next_cancel: AtomicBool::new(false),
        block_before_commit: true,
        cancel_committed: AtomicBool::new(false),
        panic_next_cancel: AtomicBool::new(false),
    });
    let _gate_cleanup = OpenGateOnDrop(Arc::clone(&gate));
    let registry = Arc::new(ValueBytesCodecRegistry::from_registrations([&CODEC]).unwrap());
    let ids = Arc::new(Ids(AtomicU64::new(7)));
    // No handler is registered, so the accepted task becomes Blocked.
    let service = TaskExecutionServiceBuilder::new(gate.clone(), registry, ids)
        .build()
        .await
        .unwrap();
    let accepted = service
        .submit(TaskRequest::new(
            "missing.handler",
            1,
            ValueCodecId::new("tests.cancel.codec"),
            Payload,
        ))
        .await
        .unwrap();
    let id = accepted.summary.id;
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if matches!(
                service.get_summary(id).await.unwrap().unwrap().state,
                TaskState::Blocked { .. }
            ) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("task becomes blocked");

    let cancel_service = service.clone();
    let cancel = tokio::spawn(async move { cancel_service.request_cancel(id).await });
    tokio::time::timeout(Duration::from_secs(3), gate.cancelled.notified())
        .await
        .expect("cancel CAS is held before commit");
    assert!(matches!(
        sqlite.get_summary(id).await.unwrap().unwrap().state,
        TaskState::Blocked { .. }
    ));
    cancel.abort();
    let _ = cancel.await;

    let shutdown_service = service.clone();
    let shutdown = tokio::spawn(async move { shutdown_service.shutdown().await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !gate.owner_released.load(Ordering::Acquire),
        "shutdown must not release ownership while the cancellation CAS is held"
    );
    gate.release.notify_one();
    tokio::time::timeout(Duration::from_secs(3), shutdown)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(gate.cancel_committed.load(Ordering::Acquire));
    assert!(gate.owner_released.load(Ordering::Acquire));
    drop(service);
    drop(gate);
    drop(sqlite);
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn test_committed_running_cancel_survives_caller_abort_and_reaches_handler() {
    let path = temp_db();
    let sqlite = Arc::new(SqliteTaskStore::open(&path).expect("sqlite opens"));
    let gate = Arc::new(GateStore {
        inner: sqlite.clone(),
        cancelled: Notify::new(),
        release: Notify::new(),
        owner_released: AtomicBool::new(false),
        gate_used: AtomicBool::new(false),
        conflict_next_cancel: AtomicBool::new(true),
        block_before_commit: false,
        cancel_committed: AtomicBool::new(false),
        panic_next_cancel: AtomicBool::new(false),
    });
    let _gate_cleanup = OpenGateOnDrop(Arc::clone(&gate));
    let registry = Arc::new(ValueBytesCodecRegistry::from_registrations([&CODEC]).unwrap());
    let mut builder = TaskExecutionServiceBuilder::new(gate.clone(), registry, Arc::new(Ids(AtomicU64::new(17))));
    let handler_started = Arc::new(Notify::new());
    let handler_observed_cancel = Arc::clone(&handler_started);
    let handler_cancelled = Arc::new(Notify::new());
    let handler_observed_signal = Arc::clone(&handler_cancelled);
    builder
        .register_handler_fn_with_options::<Payload, _, _>(
            TaskHandlerDescriptor::for_payload::<Payload>("cancel.test", 1),
            move |_, context: TaskContext| {
                let started = Arc::clone(&handler_observed_cancel);
                let cancelled = Arc::clone(&handler_observed_signal);
                async move {
                    started.notify_one();
                    while !context.is_cancelled() {
                        tokio::task::yield_now().await;
                    }
                    cancelled.notify_one();
                    Ok(TaskRunOutcome::Cancelled)
                }
            },
            HandlerOptions::new().with_cooperative_cancellation(),
        )
        .expect("cooperative handler registers");
    let service = builder.build().await.unwrap();
    let accepted = service
        .submit(TaskRequest::new(
            "cancel.test",
            1,
            ValueCodecId::new("tests.cancel.codec"),
            Payload,
        ))
        .await
        .unwrap();
    let id = accepted.summary.id;
    tokio::time::timeout(Duration::from_secs(3), handler_started.notified())
        .await
        .expect("handler starts");

    let conflict = service.request_cancel(id).await;
    assert!(matches!(conflict, Err(TaskServiceError::Store(StoreError::Conflict))));
    assert!(
        tokio::time::timeout(Duration::from_millis(50), handler_cancelled.notified())
            .await
            .is_err(),
        "a conflicting CAS must not signal the current attempt"
    );

    let cancel_service = service.clone();
    let cancel = tokio::spawn(async move { cancel_service.request_cancel(id).await });
    tokio::time::timeout(Duration::from_secs(3), gate.cancelled.notified())
        .await
        .expect("running cancel CAS commits");
    let summary = sqlite.get_summary(id).await.unwrap().unwrap();
    assert!(summary.cancel_requested, "intent is visible before the wrapper returns");
    cancel.abort();
    let _ = cancel.await;
    gate.release.notify_one();
    tokio::time::timeout(Duration::from_secs(3), handler_cancelled.notified())
        .await
        .expect("successful CAS signals the handler");

    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if matches!(
                service.get_summary(id).await.unwrap().unwrap().state,
                TaskState::Cancelled
            ) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("handler receives cancellation after caller abort");
    service.shutdown().await.unwrap();
    drop(service);
    drop(gate);
    drop(sqlite);
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn test_cancellation_store_panic_is_latched_and_propagated() {
    let path = temp_db();
    let sqlite = Arc::new(SqliteTaskStore::open(&path).expect("sqlite opens"));
    let gate = Arc::new(GateStore {
        inner: sqlite.clone(),
        cancelled: Notify::new(),
        release: Notify::new(),
        owner_released: AtomicBool::new(false),
        gate_used: AtomicBool::new(false),
        conflict_next_cancel: AtomicBool::new(false),
        block_before_commit: false,
        cancel_committed: AtomicBool::new(false),
        panic_next_cancel: AtomicBool::new(true),
    });
    let _gate_cleanup = OpenGateOnDrop(Arc::clone(&gate));
    let registry = Arc::new(ValueBytesCodecRegistry::from_registrations([&CODEC]).unwrap());
    let service = TaskExecutionServiceBuilder::new(gate.clone(), registry, Arc::new(Ids(AtomicU64::new(27))))
        .build()
        .await
        .unwrap();
    let accepted = service
        .submit(TaskRequest::new(
            "missing.handler",
            1,
            ValueCodecId::new("tests.cancel.codec"),
            Payload,
        ))
        .await
        .unwrap();
    let id = accepted.summary.id;
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if matches!(
                service.get_summary(id).await.unwrap().unwrap().state,
                TaskState::Blocked { .. }
            ) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("task becomes blocked");

    let cancel_service = service.clone();
    let cancel = tokio::spawn(async move { cancel_service.request_cancel(id).await });
    let join_error = tokio::time::timeout(Duration::from_secs(3), cancel)
        .await
        .expect("cancel caller observes worker panic")
        .expect_err("worker panic is propagated through the waiting caller");
    assert!(join_error.is_panic());
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(3), service.shutdown())
            .await
            .expect("faulted service shutdown completes"),
        Err(TaskServiceError::StoreUnavailable(_))
    ));
    drop(service);
    drop(gate);
    drop(sqlite);
    let _ = std::fs::remove_file(&path);
}
