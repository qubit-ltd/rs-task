// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use qubit_codec::ValueBytesCodecDescriptor;
use qubit_codec::ValueBytesCodecRegistration;
use qubit_codec::ValueBytesCodecRegistry;
use qubit_codec::ValueCodecId;
use qubit_codec::ValueCodecRegistration;
use qubit_codec::ValueCodecRegistrationSource;
use qubit_model_id::ModelId;
use qubit_model_id::ModelIdBuf;
use qubit_task::CancellationMode;
use qubit_task::TaskContext;
use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::TaskHandler;
use qubit_task::TaskHandlerDescriptor;
use qubit_task::handler::TaskRunOutcome;
use qubit_task::model::AcceptOutcome;
use qubit_task::model::ProgressCommand;
use qubit_task::model::ResourceCapacity;
use qubit_task::model::ResourceRequest;
use qubit_task::model::StartCommand;
use qubit_task::model::StoredTask;
use qubit_task::model::StoredTaskRequest;
use qubit_task::model::TaskCursor;
use qubit_task::model::TaskOutput;
use qubit_task::model::TaskPage;
use qubit_task::model::TaskQuery;
use qubit_task::model::TaskRequest;
use qubit_task::model::TaskState;
use qubit_task::model::TaskSummary;
use qubit_task::model::TransitionCommand;
use qubit_task::store::MemoryTaskStore;
use qubit_task::store::StoreError;
use qubit_task::store::TaskFuture;
use qubit_task::store::TaskStore;

#[derive(Default)]
struct U32Codec;

impl qubit_codec::ValueEncoder<u32> for U32Codec {
    type Output = Vec<u8>;
    type Error = std::convert::Infallible;

    fn encode(&mut self, value: &u32) -> Result<Vec<u8>, Self::Error> {
        Ok(value.to_le_bytes().to_vec())
    }
}

impl qubit_codec::ValueDecoder<[u8]> for U32Codec {
    type Output = u32;
    type Error = std::array::TryFromSliceError;

    fn decode(&mut self, bytes: &[u8]) -> Result<u32, Self::Error> {
        let raw: [u8; 4] = bytes.try_into()?;
        Ok(u32::from_le_bytes(raw))
    }
}

static CODEC_DESCRIPTOR: ValueBytesCodecDescriptor = ValueBytesCodecDescriptor::of::<U32Codec, u32>();
static CODEC_REGISTRATION: ValueBytesCodecRegistration = ValueCodecRegistration::new(
    ValueCodecId::new("qubit_task.typed_resource_tests.u32"),
    &CODEC_DESCRIPTOR,
    ValueCodecRegistrationSource::new(
        "qubit-task",
        "typed_resource_reservation_tests",
        "tests/service/typed_resource_reservation_tests.rs",
        1,
    ),
);

struct Ids(AtomicU64);

impl qubit_id::IdGenerator for Ids {
    fn generate(&self) -> Result<qubit_id::Id, qubit_id::IdGenerationError> {
        Ok(qubit_id::Id::new(self.0.fetch_add(1, Ordering::Relaxed)))
    }
}

fn descriptor() -> TaskHandlerDescriptor {
    TaskHandlerDescriptor {
        kind_id: "test.resource-reservation".into(),
        payload_type_id: ModelIdBuf::parse("test.ResourceReservationPayload").unwrap(),
        accepted_schema_versions: vec![1],
        cancellation_mode: CancellationMode::Unsupported,
    }
}

fn request(cpu_slots: u32) -> TaskRequest<u32> {
    let mut request = TaskRequest::new(
        "test.resource-reservation",
        ModelId::new("test.ResourceReservationPayload"),
        1,
        ValueCodecId::new("qubit_task.typed_resource_tests.u32"),
        7,
    );
    request.resource_limit = ResourceRequest {
        cpu_slots,
        ..ResourceRequest::default()
    };
    request
}

fn request_resources(cpu_slots: u32, gpu_count: u32) -> TaskRequest<u32> {
    let mut task = request(cpu_slots);
    task.resource_limit.gpu_count = gpu_count;
    task
}

struct ReadyScanGate {
    armed: AtomicBool,
    reached: tokio::sync::Notify,
    release: tokio::sync::Semaphore,
}

struct StartCasGate {
    armed: AtomicBool,
    reached: tokio::sync::Notify,
    release: tokio::sync::Semaphore,
}

struct GatedReadyStore {
    inner: MemoryTaskStore,
    gate: Arc<ReadyScanGate>,
    start_gate: Option<Arc<StartCasGate>>,
}

impl TaskStore for GatedReadyStore {
    fn capabilities(&self) -> qubit_task::model::StoreCapabilities {
        self.inner.capabilities()
    }
    fn accept_encoded<'a>(
        &'a self,
        id: qubit_task::model::TaskId,
        request: StoredTaskRequest,
    ) -> TaskFuture<'a, Result<AcceptOutcome, StoreError>> {
        self.inner.accept_encoded(id, request)
    }
    fn get_encoded_task<'a>(
        &'a self,
        id: qubit_task::model::TaskId,
    ) -> TaskFuture<'a, Result<Option<StoredTask>, StoreError>> {
        self.inner.get_encoded_task(id)
    }
    fn start_encoded<'a>(&'a self, command: StartCommand) -> TaskFuture<'a, Result<TaskSummary, StoreError>> {
        Box::pin(async move {
            if let Some(gate) = &self.start_gate
                && gate.armed.swap(false, Ordering::AcqRel)
            {
                gate.reached.notify_one();
                gate.release.acquire().await.expect("start gate stays open").forget();
            }
            self.inner.start_encoded(command).await
        })
    }
    fn transition_encoded<'a>(&'a self, command: TransitionCommand) -> TaskFuture<'a, Result<TaskSummary, StoreError>> {
        self.inner.transition_encoded(command)
    }
    fn update_progress<'a>(&'a self, command: ProgressCommand) -> TaskFuture<'a, Result<TaskSummary, StoreError>> {
        self.inner.update_progress(command)
    }
    fn list_encoded<'a>(&'a self, query: TaskQuery) -> TaskFuture<'a, Result<TaskPage, StoreError>> {
        self.inner.list_encoded(query)
    }
    fn list_ready_queued<'a>(
        &'a self,
        after: Option<TaskCursor>,
        limit: std::num::NonZeroUsize,
        now_ms: u64,
    ) -> TaskFuture<'a, Result<TaskPage, StoreError>> {
        Box::pin(async move {
            if self.gate.armed.swap(false, Ordering::AcqRel) {
                self.gate.reached.notify_one();
                self.gate
                    .release
                    .acquire()
                    .await
                    .expect("scan gate stays open")
                    .forget();
            }
            self.inner.list_ready_queued(after, limit, now_ms).await
        })
    }
    fn next_retry_deadline<'a>(&'a self, now_ms: u64) -> TaskFuture<'a, Result<Option<u64>, StoreError>> {
        self.inner.next_retry_deadline(now_ms)
    }
    fn prune_terminal_before<'a>(
        &'a self,
        cutoff: u64,
        max_rows: std::num::NonZeroUsize,
    ) -> TaskFuture<'a, Result<usize, StoreError>> {
        self.inner.prune_terminal_before(cutoff, max_rows)
    }
    fn acquire_owner<'a>(&'a self) -> TaskFuture<'a, Result<qubit_task::model::OwnerEpoch, StoreError>> {
        self.inner.acquire_owner()
    }
    fn release_owner<'a>(&'a self, epoch: qubit_task::model::OwnerEpoch) -> TaskFuture<'a, Result<(), StoreError>> {
        self.inner.release_owner(epoch)
    }
}

async fn wait_for(
    service: &qubit_task::TaskExecutionService,
    id: qubit_task::model::TaskId,
    predicate: impl Fn(&TaskState) -> bool,
) -> TaskState {
    for _ in 0..1000 {
        if let Some(summary) = service.get(id).await.unwrap()
            && predicate(&summary.state)
        {
            return summary.state;
        }
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    panic!("typed task did not reach the expected state")
}

struct GatedHandler {
    started: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Semaphore>,
    calls: Arc<AtomicUsize>,
}

impl TaskHandler<u32> for GatedHandler {
    fn run<'a>(&'a self, _value: u32, _context: TaskContext) -> TaskFuture<'a, qubit_task::handler::TaskRunResult> {
        Box::pin(async move {
            if self.calls.fetch_add(1, Ordering::Relaxed) == 0 {
                self.started.notify_one();
                self.release
                    .acquire()
                    .await
                    .expect("release permit remains open")
                    .forget();
            }
            Ok(TaskRunOutcome::Succeeded(TaskOutput::default()))
        })
    }
}

#[tokio::test]
async fn typed_resource_request_above_capacity_is_blocked() {
    let codecs = Arc::new(ValueBytesCodecRegistry::from_registrations([&CODEC_REGISTRATION]).unwrap());
    let mut builder = TaskExecutionServiceBuilder::new(
        Arc::new(MemoryTaskStore::new(8)),
        codecs,
        Arc::new(Ids(AtomicU64::new(1))),
    )
    .capacity(ResourceCapacity {
        cpu_slots: 1,
        ..ResourceCapacity::default()
    });
    builder
        .handlers_mut()
        .register::<u32, _>(
            descriptor(),
            Arc::new(GatedHandler {
                started: Arc::new(tokio::sync::Notify::new()),
                release: Arc::new(tokio::sync::Semaphore::new(0)),
                calls: Arc::new(AtomicUsize::new(0)),
            }),
        )
        .unwrap();
    let service = builder.build().await.unwrap();

    let accepted = service.submit(request(2)).await.unwrap();
    let state = wait_for(&service, accepted.id, |state| {
        matches!(state, TaskState::Blocked { .. })
    })
    .await;
    assert!(matches!(state, TaskState::Blocked { .. }));
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn typed_resource_reservation_waits_until_capacity_is_released() {
    let codecs = Arc::new(ValueBytesCodecRegistry::from_registrations([&CODEC_REGISTRATION]).unwrap());
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let mut builder = TaskExecutionServiceBuilder::new(
        Arc::new(MemoryTaskStore::new(8)),
        codecs,
        Arc::new(Ids(AtomicU64::new(10))),
    )
    .capacity(ResourceCapacity {
        cpu_slots: 1,
        ..ResourceCapacity::default()
    });
    builder
        .handlers_mut()
        .register::<u32, _>(
            descriptor(),
            Arc::new(GatedHandler {
                started: Arc::clone(&started),
                release: Arc::clone(&release),
                calls,
            }),
        )
        .unwrap();
    let service = builder.build().await.unwrap();

    let first = service.submit(request(1)).await.unwrap();
    started.notified().await;
    assert!(matches!(
        wait_for(&service, first.id, |state| matches!(state, TaskState::Running)).await,
        TaskState::Running
    ));

    let second = service.submit(request(1)).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(matches!(
        service.get(second.id).await.unwrap().unwrap().state,
        TaskState::Queued
    ));

    release.add_permits(1);
    assert_eq!(
        wait_for(&service, first.id, TaskState::is_terminal).await,
        TaskState::Succeeded
    );
    assert_eq!(
        wait_for(&service, second.id, TaskState::is_terminal).await,
        TaskState::Succeeded
    );
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn scheduler_skips_resource_blocked_task_without_consuming_run_slot() {
    let codecs = Arc::new(ValueBytesCodecRegistry::from_registrations([&CODEC_REGISTRATION]).unwrap());
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let mut builder = TaskExecutionServiceBuilder::new(
        Arc::new(MemoryTaskStore::new(8)),
        codecs,
        Arc::new(Ids(AtomicU64::new(100))),
    )
    .capacity(ResourceCapacity {
        cpu_slots: 2,
        gpus: [("gpu-0".to_owned(), vec!["test".to_owned()])].into(),
        ..ResourceCapacity::default()
    })
    .max_running_tasks(std::num::NonZeroUsize::new(2).unwrap());
    builder
        .handlers_mut()
        .register::<u32, _>(
            descriptor(),
            Arc::new(GatedHandler {
                started: Arc::clone(&started),
                release: Arc::clone(&release),
                calls: Arc::clone(&calls),
            }),
        )
        .unwrap();
    let service = builder.build().await.unwrap();

    let first = service.submit(request_resources(1, 1)).await.unwrap();
    started.notified().await;
    let blocked = service.submit(request_resources(0, 1)).await.unwrap();
    let bypass = service.submit(request_resources(1, 0)).await.unwrap();
    assert_eq!(
        wait_for(&service, bypass.id, TaskState::is_terminal).await,
        TaskState::Succeeded
    );
    assert!(matches!(
        service.get(blocked.id).await.unwrap().unwrap().state,
        TaskState::Queued
    ));
    assert!(matches!(
        service.get(first.id).await.unwrap().unwrap().state,
        TaskState::Running
    ));

    release.add_permits(1);
    assert_eq!(
        wait_for(&service, first.id, TaskState::is_terminal).await,
        TaskState::Succeeded
    );
    assert_eq!(
        wait_for(&service, blocked.id, TaskState::is_terminal).await,
        TaskState::Succeeded
    );
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn resource_release_during_ready_scan_is_not_lost() {
    let codecs = Arc::new(ValueBytesCodecRegistry::from_registrations([&CODEC_REGISTRATION]).unwrap());
    let started = Arc::new(tokio::sync::Notify::new());
    let release_handler = Arc::new(tokio::sync::Semaphore::new(0));
    let gate = Arc::new(ReadyScanGate {
        armed: AtomicBool::new(false),
        reached: tokio::sync::Notify::new(),
        release: tokio::sync::Semaphore::new(0),
    });
    let store = Arc::new(GatedReadyStore {
        inner: MemoryTaskStore::new(8),
        gate: Arc::clone(&gate),
        start_gate: None,
    });
    let mut builder = TaskExecutionServiceBuilder::new(store, codecs, Arc::new(Ids(AtomicU64::new(200))))
        .capacity(ResourceCapacity {
            cpu_slots: 1,
            ..ResourceCapacity::default()
        })
        .max_running_tasks(std::num::NonZeroUsize::new(2).unwrap());
    builder
        .handlers_mut()
        .register::<u32, _>(
            descriptor(),
            Arc::new(GatedHandler {
                started: Arc::clone(&started),
                release: Arc::clone(&release_handler),
                calls: Arc::new(AtomicUsize::new(0)),
            }),
        )
        .unwrap();
    let service = builder.build().await.unwrap();
    let first = service.submit(request(1)).await.unwrap();
    started.notified().await;
    let scan_reached = gate.reached.notified();
    tokio::pin!(scan_reached);
    scan_reached.as_mut().enable();
    gate.armed.store(true, Ordering::Release);
    let waiting = service.submit(request(1)).await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), scan_reached)
        .await
        .unwrap();

    release_handler.add_permits(1);
    assert_eq!(
        wait_for(&service, first.id, TaskState::is_terminal).await,
        TaskState::Succeeded
    );
    gate.release.add_permits(1);
    assert_eq!(
        wait_for(&service, waiting.id, TaskState::is_terminal).await,
        TaskState::Succeeded
    );
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancel_during_start_cas_releases_reservation_without_running_handler() {
    let codecs = Arc::new(ValueBytesCodecRegistry::from_registrations([&CODEC_REGISTRATION]).unwrap());
    let ready_gate = Arc::new(ReadyScanGate {
        armed: AtomicBool::new(false),
        reached: tokio::sync::Notify::new(),
        release: tokio::sync::Semaphore::new(0),
    });
    let start_gate = Arc::new(StartCasGate {
        armed: AtomicBool::new(true),
        reached: tokio::sync::Notify::new(),
        release: tokio::sync::Semaphore::new(0),
    });
    let store = Arc::new(GatedReadyStore {
        inner: MemoryTaskStore::new(8),
        gate: ready_gate,
        start_gate: Some(Arc::clone(&start_gate)),
    });
    let started = Arc::new(tokio::sync::Notify::new());
    let release_handler = Arc::new(tokio::sync::Semaphore::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let mut builder = TaskExecutionServiceBuilder::new(store, codecs, Arc::new(Ids(AtomicU64::new(900)))).capacity(
        ResourceCapacity {
            cpu_slots: 1,
            ..ResourceCapacity::default()
        },
    );
    builder
        .handlers_mut()
        .register::<u32, _>(
            descriptor(),
            Arc::new(GatedHandler {
                started: Arc::clone(&started),
                release: Arc::clone(&release_handler),
                calls: Arc::clone(&calls),
            }),
        )
        .unwrap();
    let service = builder.build().await.unwrap();

    let start_reached = start_gate.reached.notified();
    tokio::pin!(start_reached);
    start_reached.as_mut().enable();
    let cancelled = service.submit(request(1)).await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), start_reached)
        .await
        .unwrap();

    assert_eq!(
        service.cancel(cancelled.id).await.unwrap(),
        qubit_task::service::CancelOutcome::CancelledBeforeStart
    );
    start_gate.release.add_permits(1);
    assert_eq!(
        wait_for(&service, cancelled.id, TaskState::is_terminal).await,
        TaskState::Cancelled
    );
    assert_eq!(calls.load(Ordering::Relaxed), 0);

    release_handler.add_permits(1);
    let next = service.submit(request(1)).await.unwrap();
    assert_eq!(
        wait_for(&service, next.id, TaskState::is_terminal).await,
        TaskState::Succeeded
    );
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    service.shutdown().await.unwrap();
}
