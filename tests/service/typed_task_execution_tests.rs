// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use qubit_codec::ValueBytesCodecDescriptor;
use qubit_codec::ValueBytesCodecRegistry;
use qubit_codec::ValueCodecId;
use qubit_codec::ValueCodecRegistration;
use qubit_codec::ValueCodecRegistrationSource;
use qubit_model_id::ModelIdBuf;
use qubit_task::CancellationMode;
use qubit_task::TaskContext;
use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::TaskHandler;
use qubit_task::TaskHandlerDescriptor;
use qubit_task::handler::TaskRunOutcome;
use qubit_task::model::ResourceCapacity;
use qubit_task::model::ResourceRequest;
use qubit_task::model::StoredPayload;
use qubit_task::model::StoredTaskRequest;
use qubit_task::model::TaskId;
use qubit_task::model::TaskQuery;
use qubit_task::model::TaskRequest;
use qubit_task::model::TaskRunError;
use qubit_task::model::TaskState;
use qubit_task::service::CancelOutcome;
use qubit_task::store::MemoryTaskStore;
use qubit_task::store::TaskFuture;
use qubit_task::store::TaskStore;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Counter(u32);

impl qubit_model_id::HasModelId for Counter {
    const MODEL_ID: qubit_model_id::ModelId = qubit_model_id::ModelId::new("test.TypedServicePayload");
}

#[derive(Default)]
struct U32Codec;

impl qubit_codec::ValueEncoder<Counter> for U32Codec {
    type Output = Vec<u8>;
    type Error = std::convert::Infallible;
    fn encode(&mut self, value: &Counter) -> Result<Vec<u8>, Self::Error> {
        Ok(value.0.to_le_bytes().to_vec())
    }
}

impl qubit_codec::ValueDecoder<[u8]> for U32Codec {
    type Output = Counter;
    type Error = std::array::TryFromSliceError;
    fn decode(&mut self, bytes: &[u8]) -> Result<Counter, Self::Error> {
        let raw: [u8; 4] = bytes.try_into()?;
        Ok(Counter(u32::from_le_bytes(raw)))
    }
}

static CODEC_DESCRIPTOR: ValueBytesCodecDescriptor = ValueBytesCodecDescriptor::of::<U32Codec, Counter>();
static CODEC_REGISTRATION: qubit_codec::ValueBytesCodecRegistration = ValueCodecRegistration::new(
    ValueCodecId::new("qubit_task.typed_service.u32"),
    &CODEC_DESCRIPTOR,
    ValueCodecRegistrationSource::new(
        "qubit-task",
        "typed_service_tests",
        "tests/service/typed_task_execution_tests.rs",
        1,
    ),
);

struct Ids(AtomicU64);

impl qubit_id::IdGenerator for Ids {
    fn generate(&self) -> Result<qubit_id::Id, qubit_id::IdGenerationError> {
        Ok(qubit_id::Id::new(self.0.fetch_add(1, Ordering::Relaxed)))
    }
}

struct FailingIds;

struct ConstantId;

impl qubit_id::IdGenerator for ConstantId {
    fn generate(&self) -> Result<qubit_id::Id, qubit_id::IdGenerationError> {
        Ok(qubit_id::Id::new(2101))
    }
}

impl qubit_id::IdGenerator for FailingIds {
    fn generate(&self) -> Result<qubit_id::Id, qubit_id::IdGenerationError> {
        Err(qubit_id::IdGenerationError::NodeOutOfRange { node_id: 4, max: 3 })
    }
}

struct Handler {
    cooperative_cancel: bool,
}

impl TaskHandler<Counter> for Handler {
    fn run<'a>(&'a self, value: Counter, context: TaskContext) -> TaskFuture<'a, qubit_task::handler::TaskRunResult> {
        let cancel = self.cooperative_cancel;
        Box::pin(async move {
            assert_eq!(value, Counter(42));
            if cancel {
                while !context.is_cancelled() {
                    tokio::task::yield_now().await;
                }
                Ok(TaskRunOutcome::Cancelled)
            } else {
                Ok(TaskRunOutcome::Succeeded(qubit_task::model::TaskOutput {
                    summary: b"typed-result".to_vec(),
                }))
            }
        })
    }
}

struct PendingHandler;

impl TaskHandler<Counter> for PendingHandler {
    fn run<'a>(&'a self, _value: Counter, _context: TaskContext) -> TaskFuture<'a, qubit_task::handler::TaskRunResult> {
        Box::pin(std::future::pending())
    }
}

struct RetryOnceHandler(AtomicU64);

impl TaskHandler<Counter> for RetryOnceHandler {
    fn run<'a>(&'a self, _value: Counter, _context: TaskContext) -> TaskFuture<'a, qubit_task::handler::TaskRunResult> {
        let attempt = self.0.fetch_add(1, Ordering::AcqRel);
        Box::pin(async move {
            if attempt == 0 {
                Err(TaskRunError {
                    category: "temporary".into(),
                    message: "try again".into(),
                    retryable: true,
                })
            } else {
                Ok(TaskRunOutcome::Succeeded(qubit_task::model::TaskOutput {
                    summary: b"retried".to_vec(),
                }))
            }
        })
    }
}

struct ParallelismHandler {
    active: AtomicU64,
    maximum: AtomicU64,
    started: Arc<tokio::sync::Semaphore>,
    release: Arc<tokio::sync::Semaphore>,
}

struct GatedHandler {
    started: Arc<tokio::sync::Semaphore>,
    release: Arc<tokio::sync::Semaphore>,
}

impl TaskHandler<Counter> for GatedHandler {
    fn run<'a>(&'a self, _value: Counter, _context: TaskContext) -> TaskFuture<'a, qubit_task::handler::TaskRunResult> {
        let started = Arc::clone(&self.started);
        let release = Arc::clone(&self.release);
        Box::pin(async move {
            started.add_permits(1);
            release
                .acquire()
                .await
                .expect("handler release gate remains open")
                .forget();
            Ok(TaskRunOutcome::Succeeded(qubit_task::model::TaskOutput {
                summary: Vec::new(),
            }))
        })
    }
}

struct FailingListStore {
    inner: Arc<MemoryTaskStore>,
    fail_next_list: std::sync::atomic::AtomicBool,
    fail_next_query: std::sync::atomic::AtomicBool,
    fail_next_get: std::sync::atomic::AtomicBool,
    fail_next_start: std::sync::atomic::AtomicBool,
    fail_next_transition: std::sync::atomic::AtomicBool,
    fail_next_release: std::sync::atomic::AtomicBool,
    panic_next_release: std::sync::atomic::AtomicBool,
}

fn failing_store() -> Arc<FailingListStore> {
    Arc::new(FailingListStore {
        inner: Arc::new(MemoryTaskStore::new(32)),
        fail_next_list: std::sync::atomic::AtomicBool::new(false),
        fail_next_query: std::sync::atomic::AtomicBool::new(false),
        fail_next_get: std::sync::atomic::AtomicBool::new(false),
        fail_next_start: std::sync::atomic::AtomicBool::new(false),
        fail_next_transition: std::sync::atomic::AtomicBool::new(false),
        fail_next_release: std::sync::atomic::AtomicBool::new(false),
        panic_next_release: std::sync::atomic::AtomicBool::new(false),
    })
}

async fn wait_for_latched_store_fault(service: &qubit_task::service::TaskExecutionService) {
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if matches!(
                service.submit(request()).await,
                Err(qubit_task::service::TaskServiceError::StoreUnavailable(_))
            ) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("background store failure is latched before further writes");
}

impl TaskStore for FailingListStore {
    fn capabilities(&self) -> qubit_task::model::StoreCapabilities {
        self.inner.capabilities()
    }
    fn accept_encoded<'a>(
        &'a self,
        id: TaskId,
        request: qubit_task::model::StoredTaskRequest,
    ) -> TaskFuture<'a, Result<qubit_task::model::AcceptOutcome, qubit_task::store::StoreError>> {
        self.inner.accept_encoded(id, request)
    }
    fn get_encoded_task<'a>(
        &'a self,
        id: TaskId,
    ) -> TaskFuture<'a, Result<Option<qubit_task::model::StoredTask>, qubit_task::store::StoreError>> {
        if self.fail_next_get.swap(false, Ordering::AcqRel) {
            Box::pin(async { Err(qubit_task::store::StoreError::Failure("injected get failure".into())) })
        } else {
            self.inner.get_encoded_task(id)
        }
    }
    fn start_encoded<'a>(
        &'a self,
        command: qubit_task::model::StartCommand,
    ) -> TaskFuture<'a, Result<qubit_task::model::TaskSummary, qubit_task::store::StoreError>> {
        if self.fail_next_start.swap(false, Ordering::AcqRel) {
            Box::pin(async { Err(qubit_task::store::StoreError::Failure("injected start failure".into())) })
        } else {
            self.inner.start_encoded(command)
        }
    }
    fn transition_encoded<'a>(
        &'a self,
        command: qubit_task::model::TransitionCommand,
    ) -> TaskFuture<'a, Result<qubit_task::model::TaskSummary, qubit_task::store::StoreError>> {
        if self.fail_next_transition.swap(false, Ordering::AcqRel) {
            Box::pin(async {
                Err(qubit_task::store::StoreError::Failure(
                    "injected transition failure".into(),
                ))
            })
        } else {
            self.inner.transition_encoded(command)
        }
    }
    fn update_progress<'a>(
        &'a self,
        command: qubit_task::model::ProgressCommand,
    ) -> TaskFuture<'a, Result<qubit_task::model::TaskSummary, qubit_task::store::StoreError>> {
        self.inner.update_progress(command)
    }
    fn list_encoded<'a>(
        &'a self,
        query: TaskQuery,
    ) -> TaskFuture<'a, Result<qubit_task::model::TaskPage, qubit_task::store::StoreError>> {
        if self.fail_next_query.swap(false, Ordering::AcqRel) {
            Box::pin(async { Err(qubit_task::store::StoreError::Failure("injected list failure".into())) })
        } else {
            self.inner.list_encoded(query)
        }
    }
    fn list_ready_queued<'a>(
        &'a self,
        after: Option<qubit_task::model::TaskCursor>,
        limit: std::num::NonZeroUsize,
        now_ms: u64,
    ) -> TaskFuture<'a, Result<qubit_task::model::TaskPage, qubit_task::store::StoreError>> {
        if self.fail_next_list.swap(false, Ordering::AcqRel) {
            Box::pin(async { Err(qubit_task::store::StoreError::Failure("injected list failure".into())) })
        } else {
            self.inner.list_ready_queued(after, limit, now_ms)
        }
    }
    fn next_retry_deadline<'a>(
        &'a self,
        now_ms: u64,
    ) -> TaskFuture<'a, Result<Option<u64>, qubit_task::store::StoreError>> {
        self.inner.next_retry_deadline(now_ms)
    }
    fn prune_terminal_before<'a>(
        &'a self,
        finished_before_ms: u64,
        max_rows: std::num::NonZeroUsize,
    ) -> TaskFuture<'a, Result<usize, qubit_task::store::StoreError>> {
        self.inner.prune_terminal_before(finished_before_ms, max_rows)
    }
    fn acquire_owner<'a>(
        &'a self,
    ) -> TaskFuture<'a, Result<qubit_task::model::OwnerEpoch, qubit_task::store::StoreError>> {
        self.inner.acquire_owner()
    }
    fn release_owner<'a>(
        &'a self,
        epoch: qubit_task::model::OwnerEpoch,
    ) -> TaskFuture<'a, Result<(), qubit_task::store::StoreError>> {
        if self.panic_next_release.swap(false, Ordering::AcqRel) {
            panic!("injected release panic");
        }
        if self.fail_next_release.swap(false, Ordering::AcqRel) {
            return Box::pin(async {
                Err(qubit_task::store::StoreError::Failure(
                    "injected release failure".into(),
                ))
            });
        }
        self.inner.release_owner(epoch)
    }
}

struct BuildGateStore {
    inner: Arc<dyn TaskStore>,
    entered: tokio::sync::Notify,
    resume: tokio::sync::Semaphore,
    first_list: std::sync::atomic::AtomicBool,
}

impl BuildGateStore {
    fn new(inner: Arc<dyn TaskStore>) -> Self {
        Self {
            inner,
            entered: tokio::sync::Notify::new(),
            resume: tokio::sync::Semaphore::new(0),
            first_list: std::sync::atomic::AtomicBool::new(true),
        }
    }
}

impl TaskStore for BuildGateStore {
    fn capabilities(&self) -> qubit_task::model::StoreCapabilities {
        self.inner.capabilities()
    }
    fn accept_encoded<'a>(
        &'a self,
        id: TaskId,
        request: StoredTaskRequest,
    ) -> TaskFuture<'a, Result<qubit_task::model::AcceptOutcome, qubit_task::store::StoreError>> {
        self.inner.accept_encoded(id, request)
    }
    fn get_encoded_task<'a>(
        &'a self,
        id: TaskId,
    ) -> TaskFuture<'a, Result<Option<qubit_task::model::StoredTask>, qubit_task::store::StoreError>> {
        self.inner.get_encoded_task(id)
    }
    fn start_encoded<'a>(
        &'a self,
        command: qubit_task::model::StartCommand,
    ) -> TaskFuture<'a, Result<qubit_task::model::TaskSummary, qubit_task::store::StoreError>> {
        self.inner.start_encoded(command)
    }
    fn transition_encoded<'a>(
        &'a self,
        command: qubit_task::model::TransitionCommand,
    ) -> TaskFuture<'a, Result<qubit_task::model::TaskSummary, qubit_task::store::StoreError>> {
        self.inner.transition_encoded(command)
    }
    fn update_progress<'a>(
        &'a self,
        command: qubit_task::model::ProgressCommand,
    ) -> TaskFuture<'a, Result<qubit_task::model::TaskSummary, qubit_task::store::StoreError>> {
        self.inner.update_progress(command)
    }
    fn list_encoded<'a>(
        &'a self,
        query: TaskQuery,
    ) -> TaskFuture<'a, Result<qubit_task::model::TaskPage, qubit_task::store::StoreError>> {
        Box::pin(async move {
            if self.first_list.swap(false, Ordering::AcqRel) {
                self.entered.notify_one();
                let permit = self
                    .resume
                    .acquire()
                    .await
                    .map_err(|error| qubit_task::store::StoreError::Failure(error.to_string()))?;
                permit.forget();
            }
            self.inner.list_encoded(query).await
        })
    }
    fn list_ready_queued<'a>(
        &'a self,
        after: Option<qubit_task::model::TaskCursor>,
        limit: std::num::NonZeroUsize,
        now_ms: u64,
    ) -> TaskFuture<'a, Result<qubit_task::model::TaskPage, qubit_task::store::StoreError>> {
        self.inner.list_ready_queued(after, limit, now_ms)
    }
    fn next_retry_deadline<'a>(
        &'a self,
        now_ms: u64,
    ) -> TaskFuture<'a, Result<Option<u64>, qubit_task::store::StoreError>> {
        self.inner.next_retry_deadline(now_ms)
    }
    fn prune_terminal_before<'a>(
        &'a self,
        finished_before_ms: u64,
        max_rows: std::num::NonZeroUsize,
    ) -> TaskFuture<'a, Result<usize, qubit_task::store::StoreError>> {
        self.inner.prune_terminal_before(finished_before_ms, max_rows)
    }
    fn acquire_owner<'a>(
        &'a self,
    ) -> TaskFuture<'a, Result<qubit_task::model::OwnerEpoch, qubit_task::store::StoreError>> {
        self.inner.acquire_owner()
    }
    fn release_owner<'a>(
        &'a self,
        epoch: qubit_task::model::OwnerEpoch,
    ) -> TaskFuture<'a, Result<(), qubit_task::store::StoreError>> {
        self.inner.release_owner(epoch)
    }
}

#[tokio::test]
async fn build_cancellation_releases_memory_owner() {
    let inner: Arc<dyn TaskStore> = Arc::new(MemoryTaskStore::new(32));
    let gate = Arc::new(BuildGateStore::new(Arc::clone(&inner)));
    let entered = gate.entered.notified();
    tokio::pin!(entered);
    entered.as_mut().enable();
    let first_store: Arc<dyn TaskStore> = gate.clone();
    let build = tokio::spawn(async move {
        TaskExecutionServiceBuilder::new(first_store, registry(), Arc::new(Ids(AtomicU64::new(1))))
            .build()
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(2), entered)
        .await
        .expect("first build enters recovery");
    build.abort();
    let _ = build.await;
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            match TaskExecutionServiceBuilder::new(Arc::clone(&inner), registry(), Arc::new(Ids(AtomicU64::new(2))))
                .build()
                .await
            {
                Ok(service) => {
                    service.shutdown().await.expect("second service shuts down");
                    break;
                }
                Err(qubit_task::service::TaskServiceError::Store(qubit_task::store::StoreError::OwnerConflict)) => {
                    tokio::task::yield_now().await
                }
                Err(error) => panic!("unexpected second build error: {error}"),
            }
        }
    })
    .await
    .expect("cancelled build eventually releases memory owner");
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn build_cancellation_releases_sqlite_file_lock() {
    let path = std::env::temp_dir().join(format!(
        "qubit-task-build-cancel-{}.sqlite",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time after epoch")
            .as_nanos()
    ));
    let inner: Arc<dyn TaskStore> =
        Arc::new(qubit_task::store::SqliteTaskStore::open(&path).expect("first SQLite store opens"));
    let gate = Arc::new(BuildGateStore::new(Arc::clone(&inner)));
    let build = {
        let entered = gate.entered.notified();
        tokio::pin!(entered);
        entered.as_mut().enable();
        let first_store: Arc<dyn TaskStore> = gate.clone();
        let build = tokio::spawn(async move {
            TaskExecutionServiceBuilder::new(first_store, registry(), Arc::new(Ids(AtomicU64::new(1))))
                .build()
                .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(2), entered)
            .await
            .expect("first SQLite build enters recovery");
        build
    };
    build.abort();
    let _ = build.await;
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            match qubit_task::store::SqliteTaskStore::open(&path) {
                Ok(second) => {
                    let second: Arc<dyn TaskStore> = Arc::new(second);
                    let service =
                        TaskExecutionServiceBuilder::new(second, registry(), Arc::new(Ids(AtomicU64::new(2))))
                            .build()
                            .await
                            .expect("second SQLite service builds");
                    service.shutdown().await.expect("second SQLite service shuts down");
                    break;
                }
                Err(qubit_task::store::StoreError::OwnerConflict) => tokio::task::yield_now().await,
                Err(error) => panic!("unexpected second SQLite open error: {error}"),
            }
        }
    })
    .await
    .expect("cancelled build eventually releases SQLite file lock");
    drop(gate);
    drop(inner);
    let mut lock_path = path.as_os_str().to_owned();
    lock_path.push(".owner.lock");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(std::path::PathBuf::from(lock_path));
}

impl TaskHandler<Counter> for ParallelismHandler {
    fn run<'a>(&'a self, _value: Counter, _context: TaskContext) -> TaskFuture<'a, qubit_task::handler::TaskRunResult> {
        let active = self.active.fetch_add(1, Ordering::AcqRel) + 1;
        self.maximum.fetch_max(active, Ordering::AcqRel);
        self.started.add_permits(1);
        let release = Arc::clone(&self.release);
        let active = &self.active;
        Box::pin(async move {
            let permit = release.acquire().await.unwrap();
            permit.forget();
            active.fetch_sub(1, Ordering::AcqRel);
            Ok(TaskRunOutcome::Succeeded(qubit_task::model::TaskOutput {
                summary: Vec::new(),
            }))
        })
    }
}

struct ContextProgressHandler;

impl TaskHandler<Counter> for ContextProgressHandler {
    fn run<'a>(&'a self, value: Counter, context: TaskContext) -> TaskFuture<'a, qubit_task::handler::TaskRunResult> {
        Box::pin(async move {
            assert_eq!(value, Counter(42));
            assert_eq!(context.task_id().to_padded_decimal(), "00000000000000000301");
            assert_eq!(context.attempt(), 1);
            assert!(!context.is_cancelled());
            assert!(!context.cancellation_signal().load(Ordering::Acquire));

            let _progress = context
                .progress_builder()
                .stage(qubit_progress::Stage::new("index", "Index records").position(2, 3))
                .metric(qubit_progress::Metric::new("records", "Records").total(100))
                .start_async()
                .await
                .expect("handler progress starts and persists");

            Ok(TaskRunOutcome::Succeeded(qubit_task::model::TaskOutput {
                summary: b"context-progress".to_vec(),
            }))
        })
    }
}

fn descriptor(mode: CancellationMode) -> TaskHandlerDescriptor {
    TaskHandlerDescriptor {
        kind_id: "test.typed-service".into(),
        payload_type_id: ModelIdBuf::parse("test.TypedServicePayload").unwrap(),
        accepted_schema_versions: vec![1, 2],
        cancellation_mode: mode,
    }
}

fn registry() -> Arc<ValueBytesCodecRegistry> {
    Arc::new(ValueBytesCodecRegistry::from_registrations([&CODEC_REGISTRATION]).unwrap())
}

fn request() -> TaskRequest<Counter> {
    let mut request = TaskRequest::new(
        "test.typed-service",
        2,
        ValueCodecId::new("qubit_task.typed_service.u32"),
        Counter(42),
    );
    request.resource_limit.cpu_slots = 1;
    request
}

async fn wait_for_terminal(service: &qubit_task::TaskExecutionService, id: qubit_task::model::TaskId) -> TaskState {
    for _ in 0..1000 {
        if let Some(summary) = service.get(id).await.unwrap()
            && summary.state.is_terminal()
        {
            return summary.state;
        }
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    panic!("typed task did not reach a terminal state")
}

#[tokio::test]
async fn typed_submit_decodes_runs_and_persists_terminal_state() {
    let store = Arc::new(MemoryTaskStore::new(16));
    let mut builder = TaskExecutionServiceBuilder::new(store, registry(), Arc::new(Ids(AtomicU64::new(101)))).capacity(
        ResourceCapacity {
            cpu_slots: 1,
            ..ResourceCapacity::default()
        },
    );
    builder
        .handlers_mut()
        .register::<Counter, _>(
            descriptor(CancellationMode::Cooperative),
            Arc::new(Handler {
                cooperative_cancel: false,
            }),
        )
        .unwrap();
    let service = builder.build().await.unwrap();
    let accepted = service.submit(request()).await.unwrap();
    assert_eq!(wait_for_terminal(&service, accepted.id).await, TaskState::Succeeded);
    let completed = service.get(accepted.id).await.unwrap().unwrap();
    assert_eq!(completed.output.unwrap().summary, b"typed-result");
}

#[tokio::test]
async fn typed_submit_rejection_unfinished_limit_keeps_service_running() {
    let store = Arc::new(MemoryTaskStore::with_limits(
        16,
        NonZeroUsize::new(1024).expect("payload budget is nonzero"),
        NonZeroUsize::new(1).expect("unfinished limit is nonzero"),
    ));
    let service = TaskExecutionServiceBuilder::new(store, registry(), Arc::new(Ids(AtomicU64::new(2001))))
        .build()
        .await
        .expect("service starts");
    let first = service.submit(request()).await.expect("first task is accepted");
    let blocked = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let summary = service
                .get(first.id)
                .await
                .expect("task can be queried")
                .expect("task exists");
            if matches!(summary.state, TaskState::Blocked { .. }) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    blocked.expect("missing handler blocks first task");
    assert!(matches!(
        service.submit(request()).await,
        Err(qubit_task::service::TaskServiceError::UnfinishedTaskLimitExceeded { limit: 1 })
    ));
    assert_eq!(
        service.cancel(first.id).await.expect("blocked task can be cancelled"),
        CancelOutcome::CancelledBeforeStart
    );
    assert_eq!(wait_for_terminal(&service, first.id).await, TaskState::Cancelled);
    service
        .submit(request())
        .await
        .expect("service accepts after cancellation");
    service
        .shutdown()
        .await
        .expect("ordinary rejection does not fault shutdown");
}

#[tokio::test]
async fn typed_submit_rejection_payload_budget_keeps_service_running() {
    assert_eq!(
        qubit_codec::ValueEncoder::encode(&mut U32Codec, &Counter(42))
            .expect("u32 encodes")
            .len(),
        4
    );
    let store = Arc::new(MemoryTaskStore::with_payload_budget(
        16,
        NonZeroUsize::new(4).expect("payload budget is nonzero"),
    ));
    let service = TaskExecutionServiceBuilder::new(store, registry(), Arc::new(Ids(AtomicU64::new(2021))))
        .build()
        .await
        .expect("service starts");
    let first = service.submit(request()).await.expect("first task is accepted");
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let summary = service
                .get(first.id)
                .await
                .expect("task can be queried")
                .expect("task exists");
            if matches!(summary.state, TaskState::Blocked { .. }) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("missing handler blocks first task");
    assert!(matches!(
        service.submit(request()).await,
        Err(qubit_task::service::TaskServiceError::SubmissionCapacityExceeded {
            requested_bytes: 4,
            available_bytes: 0
        })
    ));
    assert_eq!(
        service.cancel(first.id).await.expect("blocked task can be cancelled"),
        CancelOutcome::CancelledBeforeStart
    );
    assert_eq!(wait_for_terminal(&service, first.id).await, TaskState::Cancelled);
    service
        .submit(request())
        .await
        .expect("service accepts after cancellation");
    service
        .shutdown()
        .await
        .expect("ordinary rejection does not fault shutdown");
}

#[tokio::test]
async fn typed_submit_rejection_idempotency_conflict_keeps_service_running() {
    let store = Arc::new(MemoryTaskStore::new(16));
    let service = TaskExecutionServiceBuilder::new(store, registry(), Arc::new(Ids(AtomicU64::new(2041))))
        .build()
        .await
        .expect("service starts");
    let mut first = request();
    first.idempotency_key = Some("shared-key".into());
    service.submit(first).await.expect("first task is accepted");
    let mut conflicting = request();
    conflicting.idempotency_key = Some("shared-key".into());
    conflicting.payload.data = Counter(43);
    assert!(matches!(
        service.submit(conflicting).await,
        Err(qubit_task::service::TaskServiceError::IdempotencyConflict)
    ));
    let mut distinct = request();
    distinct.idempotency_key = Some("distinct-key".into());
    service.submit(distinct).await.expect("distinct key is accepted");
    service
        .shutdown()
        .await
        .expect("ordinary rejection does not fault shutdown");
}

#[tokio::test]
async fn typed_submit_rejection_duplicate_id_keeps_service_running() {
    let store = Arc::new(MemoryTaskStore::new(16));
    let mut builder = TaskExecutionServiceBuilder::new(store, registry(), Arc::new(ConstantId));
    builder
        .handlers_mut()
        .register::<Counter, _>(
            descriptor(CancellationMode::Cooperative),
            Arc::new(Handler {
                cooperative_cancel: false,
            }),
        )
        .expect("handler registers");
    let service = builder.build().await.expect("service starts");
    let first = service.submit(request()).await.expect("first task is accepted");
    assert_eq!(wait_for_terminal(&service, first.id).await, TaskState::Succeeded);
    assert!(matches!(
        service.submit(request()).await,
        Err(qubit_task::service::TaskServiceError::DuplicateTaskId)
    ));
    assert_eq!(
        service
            .get(first.id)
            .await
            .expect("original can be queried")
            .expect("original exists")
            .state,
        TaskState::Succeeded
    );
    service
        .shutdown()
        .await
        .expect("ordinary rejection does not fault shutdown");
}

#[tokio::test]
async fn retryable_handler_error_is_persisted_and_retried_after_deadline() {
    let store = Arc::new(MemoryTaskStore::new(16));
    let mut builder = TaskExecutionServiceBuilder::new(store, registry(), Arc::new(Ids(AtomicU64::new(1201))))
        .retry_policy(
            qubit_task::service::RetryPolicy::new(
                std::time::Duration::from_millis(80),
                std::time::Duration::from_millis(80),
            )
            .unwrap(),
        );
    builder
        .handlers_mut()
        .register::<Counter, _>(
            descriptor(CancellationMode::Cooperative),
            Arc::new(RetryOnceHandler(AtomicU64::new(0))),
        )
        .unwrap();
    let service = builder.build().await.unwrap();
    let accepted = service.submit(request()).await.unwrap();
    let queued_retry = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if let Some(summary) = service.get(accepted.id).await.unwrap()
                && summary.state == TaskState::Queued
                && summary.retry_not_before_ms.is_some()
            {
                break summary;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("retry transition is persisted");
    assert_eq!(queued_retry.attempt, 1);
    assert_eq!(wait_for_terminal(&service, accepted.id).await, TaskState::Succeeded);
    assert_eq!(service.get(accepted.id).await.unwrap().unwrap().attempt, 2);
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn max_attempts_one_persists_retryable_failure_as_terminal() {
    let store = Arc::new(MemoryTaskStore::new(16));
    let mut builder = TaskExecutionServiceBuilder::new(store, registry(), Arc::new(Ids(AtomicU64::new(1211))))
        .max_attempts(std::num::NonZeroU32::new(1).expect("attempt limit is nonzero"));
    builder
        .handlers_mut()
        .register::<Counter, _>(
            descriptor(CancellationMode::Cooperative),
            Arc::new(RetryOnceHandler(AtomicU64::new(0))),
        )
        .unwrap();
    let service = builder.build().await.expect("service starts");
    let accepted = service.submit(request()).await.expect("task is accepted");

    assert!(matches!(
        wait_for_terminal(&service, accepted.id).await,
        TaskState::Failed { .. }
    ));
    let failed = service.get(accepted.id).await.unwrap().expect("task remains persisted");
    assert_eq!(failed.attempt, 1);
    service.shutdown().await.expect("service shuts down cleanly");
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_retry_deadline_survives_service_restart() {
    let path = std::env::temp_dir().join(format!(
        "qubit-task-retry-restart-{}.sqlite",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    ));
    let retry_delay = std::time::Duration::from_secs(3);
    let store: Arc<dyn TaskStore> = Arc::new(qubit_task::store::SqliteTaskStore::open(&path).unwrap());
    let mut first_builder =
        TaskExecutionServiceBuilder::new(Arc::clone(&store), registry(), Arc::new(Ids(AtomicU64::new(1251))))
            .retry_policy(qubit_task::service::RetryPolicy::new(retry_delay, retry_delay).unwrap());
    first_builder
        .handlers_mut()
        .register::<Counter, _>(
            descriptor(CancellationMode::Cooperative),
            Arc::new(RetryOnceHandler(AtomicU64::new(0))),
        )
        .unwrap();
    let first = first_builder.build().await.unwrap();
    let accepted = first.submit(request()).await.unwrap();
    let retry = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let summary = first.get(accepted.id).await.unwrap().unwrap();
            if summary.attempt == 1 && summary.retry_not_before_ms.is_some() {
                break summary;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("first retry deadline is persisted");
    first.shutdown().await.unwrap();
    drop(first);
    drop(store);

    let reopened: Arc<dyn TaskStore> = Arc::new(qubit_task::store::SqliteTaskStore::open(&path).unwrap());
    let mut second_builder =
        TaskExecutionServiceBuilder::new(Arc::clone(&reopened), registry(), Arc::new(Ids(AtomicU64::new(1252))));
    second_builder
        .handlers_mut()
        .register::<Counter, _>(
            descriptor(CancellationMode::Cooperative),
            Arc::new(Handler {
                cooperative_cancel: false,
            }),
        )
        .unwrap();
    let second = second_builder.build().await.unwrap();
    let remaining = retry.retry_not_before_ms.unwrap().saturating_sub(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64,
    );
    assert!(remaining > 100, "test must restart before the stored deadline");
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let still_waiting = second.get(accepted.id).await.unwrap().unwrap();
    assert_eq!(still_waiting.attempt, 1);
    assert!(matches!(still_waiting.state, TaskState::Queued));
    assert_eq!(still_waiting.retry_not_before_ms, retry.retry_not_before_ms);
    assert_eq!(wait_for_terminal(&second, accepted.id).await, TaskState::Succeeded);
    second.shutdown().await.unwrap();
    drop(second);
    drop(reopened);
    let mut lock_path = path.as_os_str().to_owned();
    lock_path.push(".owner.lock");
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(std::path::PathBuf::from(lock_path));
}

#[tokio::test]
async fn scheduler_never_exceeds_configured_running_limit() {
    let store = Arc::new(MemoryTaskStore::new(32));
    let handler = Arc::new(ParallelismHandler {
        active: AtomicU64::new(0),
        maximum: AtomicU64::new(0),
        started: Arc::new(tokio::sync::Semaphore::new(0)),
        release: Arc::new(tokio::sync::Semaphore::new(0)),
    });
    let mut builder = TaskExecutionServiceBuilder::new(store, registry(), Arc::new(Ids(AtomicU64::new(1401))))
        .max_running_tasks(std::num::NonZeroUsize::new(2).unwrap());
    builder
        .handlers_mut()
        .register::<Counter, _>(descriptor(CancellationMode::Cooperative), handler.clone())
        .unwrap();
    let service = builder.build().await.unwrap();
    let mut accepted = Vec::new();
    for _ in 0..8 {
        accepted.push(service.submit(request()).await.unwrap().id);
    }
    let started = Arc::clone(&handler.started);
    tokio::time::timeout(std::time::Duration::from_secs(2), async move {
        started.acquire_many(2).await.unwrap().forget();
    })
    .await
    .expect("two handlers start");
    tokio::task::yield_now().await;
    assert_eq!(handler.maximum.load(Ordering::Acquire), 2);
    handler.release.add_permits(8);
    for id in accepted {
        assert_eq!(wait_for_terminal(&service, id).await, TaskState::Succeeded);
    }
    assert_eq!(handler.maximum.load(Ordering::Acquire), 2);
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn scheduler_store_failure_is_latched_and_reported_by_shutdown() {
    let store = failing_store();
    let mut builder = TaskExecutionServiceBuilder::new(store.clone(), registry(), Arc::new(Ids(AtomicU64::new(1501))));
    builder
        .handlers_mut()
        .register::<Counter, _>(
            descriptor(CancellationMode::Cooperative),
            Arc::new(Handler {
                cooperative_cancel: false,
            }),
        )
        .unwrap();
    let service = builder.build().await.unwrap();
    store.fail_next_list.store(true, Ordering::Release);
    wait_for_latched_store_fault(&service).await;
    assert!(
        matches!(service.shutdown().await, Err(qubit_task::service::TaskServiceError::StoreUnavailable(message)) if message.contains("injected list failure"))
    );
}

#[tokio::test]
async fn typed_service_latches_get_and_start_failures() {
    for (fail_get, expected) in [(true, "injected get failure"), (false, "injected start failure")] {
        let store = failing_store();
        let mut builder =
            TaskExecutionServiceBuilder::new(store.clone(), registry(), Arc::new(Ids(AtomicU64::new(1551))));
        builder
            .handlers_mut()
            .register::<Counter, _>(
                descriptor(CancellationMode::Cooperative),
                Arc::new(Handler {
                    cooperative_cancel: false,
                }),
            )
            .unwrap();
        let service = builder.build().await.unwrap();
        if fail_get {
            store.fail_next_get.store(true, Ordering::Release);
        } else {
            store.fail_next_start.store(true, Ordering::Release);
        }
        service.submit(request()).await.unwrap();
        wait_for_latched_store_fault(&service).await;
        assert!(matches!(
            service.shutdown().await,
            Err(qubit_task::service::TaskServiceError::StoreUnavailable(message)) if message.contains(expected)
        ));
    }
}

#[tokio::test]
async fn typed_service_latches_finalizer_transition_failure() {
    let store = failing_store();
    let mut builder = TaskExecutionServiceBuilder::new(store.clone(), registry(), Arc::new(Ids(AtomicU64::new(1581))));
    builder
        .handlers_mut()
        .register::<Counter, _>(
            descriptor(CancellationMode::Cooperative),
            Arc::new(Handler {
                cooperative_cancel: false,
            }),
        )
        .unwrap();
    let service = builder.build().await.unwrap();
    store.fail_next_transition.store(true, Ordering::Release);
    service.submit(request()).await.unwrap();
    wait_for_latched_store_fault(&service).await;
    assert!(matches!(
        service.shutdown().await,
        Err(qubit_task::service::TaskServiceError::StoreUnavailable(message)) if message.contains("injected transition failure")
    ));
}

#[tokio::test]
async fn typed_context_exposes_attempt_cancellation_and_persisted_progress() {
    let store = Arc::new(MemoryTaskStore::new(16));
    let mut builder = TaskExecutionServiceBuilder::new(store, registry(), Arc::new(Ids(AtomicU64::new(301)))).capacity(
        ResourceCapacity {
            cpu_slots: 1,
            ..ResourceCapacity::default()
        },
    );
    builder
        .handlers_mut()
        .register::<Counter, _>(
            descriptor(CancellationMode::Cooperative),
            Arc::new(ContextProgressHandler),
        )
        .unwrap();
    let service = builder.build().await.unwrap();
    let accepted = service.submit(request()).await.unwrap();

    assert_eq!(wait_for_terminal(&service, accepted.id).await, TaskState::Succeeded);
    let summary = service.get(accepted.id).await.unwrap().unwrap();
    let progress = summary.progress.expect("handler progress remains queryable");
    assert_eq!(progress.attempt, 1);
    assert_eq!(progress.stage.as_ref().map(|stage| stage.id.as_str()), Some("index"));
    assert_eq!(progress.metrics[0].id, "records");
}

#[tokio::test]
async fn typed_running_cancel_is_persisted_then_acknowledged_by_handler() {
    let store = Arc::new(MemoryTaskStore::new(16));
    let mut builder = TaskExecutionServiceBuilder::new(store, registry(), Arc::new(Ids(AtomicU64::new(201)))).capacity(
        ResourceCapacity {
            cpu_slots: 1,
            ..ResourceCapacity::default()
        },
    );
    builder
        .handlers_mut()
        .register::<Counter, _>(
            descriptor(CancellationMode::Cooperative),
            Arc::new(Handler {
                cooperative_cancel: true,
            }),
        )
        .unwrap();
    let service = builder.build().await.unwrap();
    let accepted = service.submit(request()).await.unwrap();
    for _ in 0..1000 {
        if service
            .get(accepted.id)
            .await
            .unwrap()
            .is_some_and(|summary| matches!(summary.state, TaskState::Running))
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    assert_eq!(
        service.cancel(accepted.id).await.unwrap(),
        CancelOutcome::CancellationRequested
    );
    assert_eq!(wait_for_terminal(&service, accepted.id).await, TaskState::Cancelled);
}

#[tokio::test]
async fn missing_handler_is_retained_as_blocked_and_can_be_cancelled() {
    let store = Arc::new(MemoryTaskStore::new(16));
    let service = TaskExecutionServiceBuilder::new(store, registry(), Arc::new(Ids(AtomicU64::new(301))))
        .build()
        .await
        .unwrap();
    let accepted = service.submit(request()).await.unwrap();
    for _ in 0..1000 {
        if service
            .get(accepted.id)
            .await
            .unwrap()
            .is_some_and(|summary| matches!(summary.state, TaskState::Blocked { .. }))
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    assert_eq!(
        service.cancel(accepted.id).await.unwrap(),
        CancelOutcome::CancelledBeforeStart
    );
    assert_eq!(wait_for_terminal(&service, accepted.id).await, TaskState::Cancelled);
}

#[tokio::test]
async fn blocked_task_can_be_resumed_after_restarting_with_its_handler() {
    let store = Arc::new(MemoryTaskStore::new(16));
    let first = TaskExecutionServiceBuilder::new(store.clone(), registry(), Arc::new(Ids(AtomicU64::new(1301))))
        .build()
        .await
        .unwrap();
    let accepted = first.submit(request()).await.unwrap();
    let blocked = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let summary = first.get(accepted.id).await.unwrap().unwrap();
            if matches!(summary.state, TaskState::Blocked { .. }) {
                break summary;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    first.shutdown().await.unwrap();

    let mut builder = TaskExecutionServiceBuilder::new(store, registry(), Arc::new(Ids(AtomicU64::new(1302))));
    builder
        .handlers_mut()
        .register::<Counter, _>(
            descriptor(CancellationMode::Cooperative),
            Arc::new(Handler {
                cooperative_cancel: false,
            }),
        )
        .unwrap();
    let second = builder.build().await.unwrap();
    let queued = second.resume_blocked(accepted.id, blocked.state_version).await.unwrap();
    assert_eq!(queued.state, TaskState::Queued);
    assert_eq!(wait_for_terminal(&second, accepted.id).await, TaskState::Succeeded);
    second.shutdown().await.unwrap();
}

#[tokio::test]
async fn typed_service_query_filters_and_get_handles_missing_task() {
    let store = Arc::new(MemoryTaskStore::new(16));
    let service = TaskExecutionServiceBuilder::new(store, registry(), Arc::new(Ids(AtomicU64::new(1311))))
        .build()
        .await
        .expect("service starts");

    let mut billing = request();
    billing.category = Some("billing".into());
    billing.correlation_key = Some("invoice-17".into());
    let billing = service.submit(billing).await.expect("billing task is accepted");
    let mut operations = request();
    operations.category = Some("operations".into());
    operations.correlation_key = Some("deploy-9".into());
    service.submit(operations).await.expect("operations task is accepted");

    let billing_page = service
        .query(TaskQuery {
            category: Some("billing".into()),
            ..TaskQuery::default()
        })
        .await
        .expect("category query succeeds");
    assert_eq!(billing_page.records.len(), 1);
    assert_eq!(billing_page.records[0].id, billing.id);

    let correlation_page = service
        .query(TaskQuery {
            correlation_key: Some("deploy-9".into()),
            ..TaskQuery::default()
        })
        .await
        .expect("correlation query succeeds");
    assert_eq!(correlation_page.records.len(), 1);
    assert_eq!(correlation_page.records[0].category.as_deref(), Some("operations"));
    assert_eq!(
        service.get(TaskId::from_id(qubit_id::Id::new(9999))).await.unwrap(),
        None
    );
    service.shutdown().await.expect("service shuts down cleanly");
}

#[tokio::test]
async fn typed_service_get_query_and_cancel_map_store_failures() {
    let store = failing_store();
    let service = TaskExecutionServiceBuilder::new(store.clone(), registry(), Arc::new(Ids(AtomicU64::new(1331))))
        .build()
        .await
        .expect("service starts");

    store.fail_next_get.store(true, Ordering::Release);
    assert!(matches!(
        service.get(TaskId::from_id(qubit_id::Id::new(9998))).await,
        Err(qubit_task::service::TaskServiceError::Store(
            qubit_task::store::StoreError::Failure(message)
        )) if message == "injected get failure"
    ));
    assert_eq!(
        service.get(TaskId::from_id(qubit_id::Id::new(9998))).await.unwrap(),
        None
    );

    store.fail_next_get.store(true, Ordering::Release);
    assert!(matches!(
        service.cancel(TaskId::from_id(qubit_id::Id::new(9997))).await,
        Err(qubit_task::service::TaskServiceError::Store(
            qubit_task::store::StoreError::Failure(message)
        )) if message == "injected get failure"
    ));

    store.fail_next_query.store(true, Ordering::Release);
    assert!(matches!(
        service.query(TaskQuery::default()).await,
        Err(qubit_task::service::TaskServiceError::Store(
            qubit_task::store::StoreError::Failure(message)
        )) if message == "injected list failure"
    ));
    assert!(service.query(TaskQuery::default()).await.is_ok());
    assert!(matches!(
        service.shutdown().await,
        Err(qubit_task::service::TaskServiceError::StoreUnavailable(message))
            if message.contains("injected get failure")
    ));
}

#[tokio::test]
async fn resume_blocked_rejects_stale_revision_and_terminal_task() {
    let store = Arc::new(MemoryTaskStore::new(16));
    let service = TaskExecutionServiceBuilder::new(store, registry(), Arc::new(Ids(AtomicU64::new(1321))))
        .build()
        .await
        .expect("service starts");
    let accepted = service.submit(request()).await.expect("task is accepted");
    let blocked = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let summary = service.get(accepted.id).await.unwrap().unwrap();
            if matches!(summary.state, TaskState::Blocked { .. }) {
                break summary;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("unhandled task reaches blocked state");

    assert!(matches!(
        service.resume_blocked(accepted.id, blocked.state_version + 1).await,
        Err(qubit_task::service::TaskServiceError::Store(
            qubit_task::store::StoreError::Conflict
        ))
    ));
    assert_eq!(
        service.cancel(accepted.id).await.unwrap(),
        CancelOutcome::CancelledBeforeStart
    );
    let cancelled = service.get(accepted.id).await.unwrap().unwrap();
    assert!(matches!(
        service.resume_blocked(accepted.id, cancelled.state_version).await,
        Err(qubit_task::service::TaskServiceError::NotBlocked {
            actual: qubit_task::model::TaskStateKind::Cancelled
        })
    ));
    service.shutdown().await.expect("service shuts down cleanly");
}

#[tokio::test]
async fn id_generation_failure_prevents_acceptance() {
    let store = Arc::new(MemoryTaskStore::new(16));
    let service = TaskExecutionServiceBuilder::new(store.clone(), registry(), Arc::new(FailingIds))
        .build()
        .await
        .unwrap();
    assert!(service.submit(request()).await.is_err());
    let page = store
        .list_encoded(TaskQuery {
            limit: 10,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(page.records.is_empty());
}

#[tokio::test]
async fn unsupported_schema_is_retained_as_blocked() {
    let store = Arc::new(MemoryTaskStore::new(16));
    let mut builder = TaskExecutionServiceBuilder::new(store, registry(), Arc::new(Ids(AtomicU64::new(401))));
    builder
        .handlers_mut()
        .register::<Counter, _>(
            descriptor(CancellationMode::Cooperative),
            Arc::new(Handler {
                cooperative_cancel: false,
            }),
        )
        .unwrap();
    let service = builder.build().await.unwrap();
    let mut unsupported = request();
    unsupported.payload.schema_version = 3;
    let accepted = service.submit(unsupported).await.unwrap();
    let state = wait_for_terminal_or_blocked(&service, accepted.id).await;
    assert!(matches!(state, TaskState::Blocked { .. }));
}

async fn wait_for_terminal_or_blocked(service: &qubit_task::TaskExecutionService, id: TaskId) -> TaskState {
    for _ in 0..1000 {
        if let Some(summary) = service.get(id).await.unwrap()
            && (summary.state.is_terminal() || matches!(summary.state, TaskState::Blocked { .. }))
        {
            return summary.state;
        }
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    panic!("typed task did not reach a blocked or terminal state")
}

#[tokio::test]
async fn queued_typed_task_can_be_cancelled_before_resource_admission() {
    let store = Arc::new(MemoryTaskStore::new(16));
    let mut builder = TaskExecutionServiceBuilder::new(store, registry(), Arc::new(Ids(AtomicU64::new(501)))).capacity(
        ResourceCapacity {
            cpu_slots: 1,
            ..ResourceCapacity::default()
        },
    );
    builder
        .handlers_mut()
        .register::<Counter, _>(descriptor(CancellationMode::Cooperative), Arc::new(PendingHandler))
        .unwrap();
    let service = builder.build().await.unwrap();
    let running = service.submit(request()).await.unwrap();
    for _ in 0..1000 {
        if service
            .get(running.id)
            .await
            .unwrap()
            .is_some_and(|summary| matches!(summary.state, TaskState::Running))
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    let queued = service.submit(request()).await.unwrap();
    assert_eq!(
        service.cancel(queued.id).await.unwrap(),
        CancelOutcome::CancelledBeforeStart
    );
    assert_eq!(wait_for_terminal(&service, queued.id).await, TaskState::Cancelled);
}

#[tokio::test]
async fn typed_service_cancel_missing_task_returns_not_found() {
    let service = TaskExecutionServiceBuilder::new(
        Arc::new(MemoryTaskStore::new(16)),
        registry(),
        Arc::new(Ids(AtomicU64::new(1331))),
    )
    .build()
    .await
    .expect("service starts");

    assert!(matches!(
        service.cancel(TaskId::from_id(qubit_id::Id::new(9991))).await,
        Err(qubit_task::service::TaskServiceError::Store(
            qubit_task::store::StoreError::NotFound
        ))
    ));
    service.shutdown().await.expect("service shuts down cleanly");
}

#[tokio::test]
async fn typed_service_resume_missing_task_returns_not_found() {
    let service = TaskExecutionServiceBuilder::new(
        Arc::new(MemoryTaskStore::new(16)),
        registry(),
        Arc::new(Ids(AtomicU64::new(1341))),
    )
    .build()
    .await
    .expect("service starts");

    assert!(matches!(
        service
            .resume_blocked(TaskId::from_id(qubit_id::Id::new(9992)), 0)
            .await,
        Err(qubit_task::service::TaskServiceError::Store(
            qubit_task::store::StoreError::NotFound
        ))
    ));
    service.shutdown().await.expect("service shuts down cleanly");
}

#[tokio::test]
async fn typed_submit_missing_codec_does_not_write_task_or_consume_id() {
    const FIRST_ID: u64 = 1351;
    let store: Arc<dyn TaskStore> = Arc::new(MemoryTaskStore::new(16));
    let service =
        TaskExecutionServiceBuilder::new(Arc::clone(&store), registry(), Arc::new(Ids(AtomicU64::new(FIRST_ID))))
            .build()
            .await
            .expect("service starts");

    let mut unsupported = request();
    unsupported.payload.codec_id = ValueCodecId::new("qubit_task.typed_service.missing");
    assert!(matches!(
        service.submit(unsupported).await,
        Err(qubit_task::service::TaskServiceError::TypedRequest(message))
            if message.contains("qubit_task.typed_service.missing")
    ));
    assert!(
        store
            .list_encoded(TaskQuery::default())
            .await
            .expect("store query succeeds")
            .records
            .is_empty()
    );

    let accepted = service.submit(request()).await.expect("valid request is accepted");
    assert_eq!(accepted.id, TaskId::from_id(qubit_id::Id::new(FIRST_ID)));
    service.shutdown().await.expect("service shuts down cleanly");
}

#[tokio::test]
async fn running_handler_without_cancel_support_reports_unsupported() {
    let store = Arc::new(MemoryTaskStore::new(16));
    let mut builder = TaskExecutionServiceBuilder::new(store, registry(), Arc::new(Ids(AtomicU64::new(601))));
    builder
        .handlers_mut()
        .register::<Counter, _>(descriptor(CancellationMode::Unsupported), Arc::new(PendingHandler))
        .unwrap();
    let service = builder.build().await.unwrap();
    let accepted = service.submit(request()).await.unwrap();
    for _ in 0..1000 {
        if service
            .get(accepted.id)
            .await
            .unwrap()
            .is_some_and(|summary| matches!(summary.state, TaskState::Running))
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    assert_eq!(
        service.cancel(accepted.id).await.unwrap(),
        CancelOutcome::CancellationUnsupported
    );
}

#[tokio::test]
async fn repeated_idempotency_key_returns_the_existing_typed_task() {
    let store = Arc::new(MemoryTaskStore::new(16));
    let mut builder = TaskExecutionServiceBuilder::new(store, registry(), Arc::new(Ids(AtomicU64::new(801))));
    builder
        .handlers_mut()
        .register::<Counter, _>(
            descriptor(CancellationMode::Cooperative),
            Arc::new(Handler {
                cooperative_cancel: false,
            }),
        )
        .unwrap();
    let service = builder.build().await.unwrap();
    let mut first = request();
    first.idempotency_key = Some("client-key-1".into());
    let mut repeated = request();
    repeated.idempotency_key = Some("client-key-1".into());
    let accepted = service.submit(first).await.unwrap();
    let existing = service.submit(repeated).await.unwrap();
    assert_eq!(accepted.id, existing.id);
}

#[tokio::test]
async fn recovery_blocks_unsupported_schema_and_missing_codec() {
    let store = Arc::new(MemoryTaskStore::new(16));
    for (value, version, codec) in [(901, 99, "qubit_task.typed_service.u32"), (902, 1, "missing.codec")] {
        store
            .accept_encoded(
                TaskId::from_id(qubit_id::Id::new(value)),
                StoredTaskRequest {
                    kind_id: "test.typed-service".into(),
                    category: Some("recovery".into()),
                    payload: StoredPayload {
                        type_id: ModelIdBuf::parse("test.TypedServicePayload").unwrap(),
                        schema_version: version,
                        codec_id: codec.into(),
                        bytes: 42_u32.to_le_bytes().to_vec(),
                    },
                    metadata: qubit_metadata::Metadata::new(),
                    resource_limit: ResourceRequest::default(),
                    correlation_key: None,
                    idempotency_key: None,
                },
            )
            .await
            .unwrap();
    }
    let mut builder = TaskExecutionServiceBuilder::new(store, registry(), Arc::new(Ids(AtomicU64::new(903))));
    builder
        .handlers_mut()
        .register::<Counter, _>(
            descriptor(CancellationMode::Cooperative),
            Arc::new(Handler {
                cooperative_cancel: false,
            }),
        )
        .unwrap();
    let service = builder.build().await.unwrap();
    for value in [901, 902] {
        let state = wait_for_terminal_or_blocked(&service, TaskId::from_id(qubit_id::Id::new(value))).await;
        assert!(matches!(state, TaskState::Blocked { .. }));
    }
}

#[tokio::test]
async fn external_cancel_retries_failed_hook() {
    struct FinishOnExternalCancel(Arc<tokio::sync::Notify>);
    impl TaskHandler<Counter> for FinishOnExternalCancel {
        fn run<'a>(
            &'a self,
            _value: Counter,
            _context: TaskContext,
        ) -> TaskFuture<'a, qubit_task::handler::TaskRunResult> {
            let finished = Arc::clone(&self.0);
            Box::pin(async move {
                finished.notified().await;
                Ok(TaskRunOutcome::Cancelled)
            })
        }
    }
    let store = Arc::new(MemoryTaskStore::new(16));
    let mut builder = TaskExecutionServiceBuilder::new(store, registry(), Arc::new(Ids(AtomicU64::new(701))));
    let hook_calls = Arc::new(AtomicU64::new(0));
    let hook_call_counter = Arc::clone(&hook_calls);
    let hook_finished = Arc::new(tokio::sync::Notify::new());
    let hook_signal = Arc::clone(&hook_finished);
    let hook: qubit_task::ExternalCancellationHook = Arc::new(move |_, _| {
        let call = hook_call_counter.fetch_add(1, Ordering::Relaxed);
        let hook_signal = Arc::clone(&hook_signal);
        Box::pin(async move {
            if call == 0 {
                Err(TaskRunError {
                    category: "remote".into(),
                    message: "temporary".into(),
                    retryable: false,
                })
            } else {
                hook_signal.notify_one();
                Ok(())
            }
        })
    });
    builder
        .handlers_mut()
        .register_with_cancellation_hook::<Counter, _>(
            descriptor(CancellationMode::ExternalHook),
            Arc::new(FinishOnExternalCancel(hook_finished)),
            "test",
            hook,
        )
        .unwrap();
    let service = builder.build().await.unwrap();
    let accepted = service.submit(request()).await.unwrap();
    for _ in 0..1000 {
        if service
            .get(accepted.id)
            .await
            .unwrap()
            .is_some_and(|summary| matches!(summary.state, TaskState::Running))
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    assert!(
        matches!(service.cancel(accepted.id).await, Err(qubit_task::service::TaskServiceError::ExternalCancellationFailed { message, .. }) if message == "temporary")
    );
    let summary = service.get(accepted.id).await.unwrap().unwrap();
    assert!(summary.cancel_requested);
    assert_eq!(summary.cancel_error.as_deref(), Some("temporary"));
    assert_eq!(
        service.cancel(accepted.id).await.unwrap(),
        CancelOutcome::CancellationRequested
    );
    assert_eq!(hook_calls.load(Ordering::Relaxed), 2);
    assert_eq!(wait_for_terminal(&service, accepted.id).await, TaskState::Cancelled);
    assert_eq!(
        service.cancel(accepted.id).await.unwrap(),
        CancelOutcome::AlreadyTerminal
    );
    assert_eq!(hook_calls.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn external_cancel_terminal_race_preserves_terminal_state() {
    struct FinishWhenReleased(Arc<tokio::sync::Notify>);
    impl TaskHandler<Counter> for FinishWhenReleased {
        fn run<'a>(
            &'a self,
            _value: Counter,
            _context: TaskContext,
        ) -> TaskFuture<'a, qubit_task::handler::TaskRunResult> {
            let release = Arc::clone(&self.0);
            Box::pin(async move {
                release.notified().await;
                Ok(TaskRunOutcome::Succeeded(qubit_task::model::TaskOutput::default()))
            })
        }
    }
    let store = Arc::new(MemoryTaskStore::new(16));
    let hook_started = Arc::new(tokio::sync::Notify::new());
    let hook_release = Arc::new(tokio::sync::Notify::new());
    let handler_release = Arc::new(tokio::sync::Notify::new());
    let calls = Arc::new(AtomicU64::new(0));
    let started = Arc::clone(&hook_started);
    let release = Arc::clone(&hook_release);
    let count = Arc::clone(&calls);
    let hook: qubit_task::ExternalCancellationHook = Arc::new(move |_, _| {
        count.fetch_add(1, Ordering::Relaxed);
        let started = Arc::clone(&started);
        let release = Arc::clone(&release);
        Box::pin(async move {
            started.notify_one();
            release.notified().await;
            Err(TaskRunError {
                category: "remote".into(),
                message: "late failure".into(),
                retryable: false,
            })
        })
    });
    let mut builder = TaskExecutionServiceBuilder::new(store, registry(), Arc::new(Ids(AtomicU64::new(704))));
    builder
        .handlers_mut()
        .register_with_cancellation_hook::<Counter, _>(
            descriptor(CancellationMode::ExternalHook),
            Arc::new(FinishWhenReleased(Arc::clone(&handler_release))),
            "test",
            hook,
        )
        .unwrap();
    let service = builder.build().await.unwrap();
    let accepted = service.submit(request()).await.unwrap();
    for _ in 0..1000 {
        if service
            .get(accepted.id)
            .await
            .unwrap()
            .is_some_and(|s| matches!(s.state, TaskState::Running))
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    let cancel_service = service.clone();
    let cancel = tokio::spawn(async move { cancel_service.cancel(accepted.id).await });
    hook_started.notified().await;
    handler_release.notify_one();
    assert_eq!(wait_for_terminal(&service, accepted.id).await, TaskState::Succeeded);
    hook_release.notify_one();
    assert_eq!(cancel.await.unwrap().unwrap(), CancelOutcome::AlreadyTerminal);
    let terminal = service.get(accepted.id).await.unwrap().unwrap();
    assert_eq!(terminal.state, TaskState::Succeeded);
    assert_eq!(terminal.cancel_error, None);
    assert_eq!(
        service.cancel(accepted.id).await.unwrap(),
        CancelOutcome::AlreadyTerminal
    );
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn external_cancel_concurrent_callers_share_hook() {
    struct FinishOnHook(Arc<tokio::sync::Notify>);
    impl TaskHandler<Counter> for FinishOnHook {
        fn run<'a>(
            &'a self,
            _value: Counter,
            _context: TaskContext,
        ) -> TaskFuture<'a, qubit_task::handler::TaskRunResult> {
            let finished = Arc::clone(&self.0);
            Box::pin(async move {
                finished.notified().await;
                Ok(TaskRunOutcome::Cancelled)
            })
        }
    }
    let store = Arc::new(MemoryTaskStore::new(16));
    let hook_started = Arc::new(tokio::sync::Notify::new());
    let hook_release = Arc::new(tokio::sync::Notify::new());
    let calls = Arc::new(AtomicU64::new(0));
    let hook_finished = Arc::new(tokio::sync::Notify::new());
    let started = Arc::clone(&hook_started);
    let release = Arc::clone(&hook_release);
    let call_count = Arc::clone(&calls);
    let finished = Arc::clone(&hook_finished);
    let hook: qubit_task::ExternalCancellationHook = Arc::new(move |_, _| {
        call_count.fetch_add(1, Ordering::Relaxed);
        let started = Arc::clone(&started);
        let release = Arc::clone(&release);
        let finished = Arc::clone(&finished);
        Box::pin(async move {
            started.notify_one();
            release.notified().await;
            finished.notify_one();
            Ok(())
        })
    });
    let mut builder = TaskExecutionServiceBuilder::new(store, registry(), Arc::new(Ids(AtomicU64::new(702))));
    builder
        .handlers_mut()
        .register_with_cancellation_hook::<Counter, _>(
            descriptor(CancellationMode::ExternalHook),
            Arc::new(FinishOnHook(hook_finished)),
            "test",
            hook,
        )
        .unwrap();
    let service = builder.build().await.unwrap();
    let accepted = service.submit(request()).await.unwrap();
    for _ in 0..1000 {
        if service
            .get(accepted.id)
            .await
            .unwrap()
            .is_some_and(|s| matches!(s.state, TaskState::Running))
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    let first_service = service.clone();
    let first = tokio::spawn(async move { first_service.cancel(accepted.id).await });
    hook_started.notified().await;
    let second_service = service.clone();
    let second = tokio::spawn(async move { second_service.cancel(accepted.id).await });
    tokio::task::yield_now().await;
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    hook_release.notify_one();
    assert_eq!(first.await.unwrap().unwrap(), CancelOutcome::CancellationRequested);
    assert_eq!(second.await.unwrap().unwrap(), CancelOutcome::CancellationRequested);
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn external_cancel_aborted_waiter_does_not_abort_hook() {
    struct FinishOnHook(Arc<tokio::sync::Notify>);
    impl TaskHandler<Counter> for FinishOnHook {
        fn run<'a>(
            &'a self,
            _value: Counter,
            _context: TaskContext,
        ) -> TaskFuture<'a, qubit_task::handler::TaskRunResult> {
            let finished = Arc::clone(&self.0);
            Box::pin(async move {
                finished.notified().await;
                Ok(TaskRunOutcome::Cancelled)
            })
        }
    }
    let store = Arc::new(MemoryTaskStore::new(16));
    let hook_started = Arc::new(tokio::sync::Notify::new());
    let hook_release = Arc::new(tokio::sync::Notify::new());
    let hook_finished = Arc::new(tokio::sync::Notify::new());
    let started = Arc::clone(&hook_started);
    let release = Arc::clone(&hook_release);
    let finished = Arc::clone(&hook_finished);
    let hook: qubit_task::ExternalCancellationHook = Arc::new(move |_, _| {
        let started = Arc::clone(&started);
        let release = Arc::clone(&release);
        let finished = Arc::clone(&finished);
        Box::pin(async move {
            started.notify_one();
            release.notified().await;
            finished.notify_one();
            Ok(())
        })
    });
    let mut builder = TaskExecutionServiceBuilder::new(store, registry(), Arc::new(Ids(AtomicU64::new(703))));
    builder
        .handlers_mut()
        .register_with_cancellation_hook::<Counter, _>(
            descriptor(CancellationMode::ExternalHook),
            Arc::new(FinishOnHook(Arc::clone(&hook_finished))),
            "test",
            hook,
        )
        .unwrap();
    let service = builder.build().await.unwrap();
    let accepted = service.submit(request()).await.unwrap();
    for _ in 0..1000 {
        if service
            .get(accepted.id)
            .await
            .unwrap()
            .is_some_and(|s| matches!(s.state, TaskState::Running))
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    let cancel_service = service.clone();
    let cancel = tokio::spawn(async move { cancel_service.cancel(accepted.id).await });
    hook_started.notified().await;
    cancel.abort();
    let shutdown_service = service.clone();
    let shutdown = tokio::spawn(async move { shutdown_service.shutdown().await });
    tokio::task::yield_now().await;
    assert!(!shutdown.is_finished());
    hook_release.notify_one();
    shutdown.await.unwrap().unwrap();
    assert!(service.get(accepted.id).await.unwrap().is_some());
    let _ = hook_finished;
}

#[tokio::test]
async fn external_cancel_hook_panics_are_reported_and_retryable() {
    let store = Arc::new(MemoryTaskStore::new(16));
    let calls = Arc::new(AtomicU64::new(0));
    let hook_calls = Arc::clone(&calls);
    let hook: qubit_task::ExternalCancellationHook =
        Arc::new(move |_, _| match hook_calls.fetch_add(1, Ordering::Relaxed) {
            0 => panic!("hook factory failed"),
            1 => Box::pin(async {
                panic!("hook future failed");
                #[allow(unreachable_code)]
                Ok(())
            }),
            _ => Box::pin(async { Ok(()) }),
        });
    let mut builder = TaskExecutionServiceBuilder::new(store, registry(), Arc::new(Ids(AtomicU64::new(705))));
    builder
        .handlers_mut()
        .register_with_cancellation_hook::<Counter, _>(
            descriptor(CancellationMode::ExternalHook),
            Arc::new(PendingHandler),
            "test",
            hook,
        )
        .unwrap();
    let service = builder.build().await.unwrap();
    let accepted = service.submit(request()).await.unwrap();
    for _ in 0..1000 {
        if service
            .get(accepted.id)
            .await
            .unwrap()
            .is_some_and(|summary| matches!(summary.state, TaskState::Running))
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }

    assert!(matches!(
        service.cancel(accepted.id).await,
        Err(qubit_task::service::TaskServiceError::ExternalCancellationFailed {
            message,
            ..
        }) if message == "hook factory failed"
    ));
    assert_eq!(
        service.get(accepted.id).await.unwrap().unwrap().cancel_error.as_deref(),
        Some("hook factory failed")
    );
    assert!(matches!(
        service.cancel(accepted.id).await,
        Err(qubit_task::service::TaskServiceError::ExternalCancellationFailed {
            message,
            ..
        }) if message == "hook future failed"
    ));
    assert_eq!(
        service.get(accepted.id).await.unwrap().unwrap().cancel_error.as_deref(),
        Some("hook future failed")
    );
    assert_eq!(
        service.cancel(accepted.id).await.unwrap(),
        CancelOutcome::CancellationRequested
    );
    assert_eq!(calls.load(Ordering::Relaxed), 3);
}

#[tokio::test]
async fn shutdown_rejects_new_work_and_waits_for_running_attempts() {
    let store = Arc::new(MemoryTaskStore::new(16));
    let mut builder = TaskExecutionServiceBuilder::new(store, registry(), Arc::new(Ids(AtomicU64::new(751))));
    builder
        .handlers_mut()
        .register::<Counter, _>(
            descriptor(CancellationMode::Cooperative),
            Arc::new(Handler {
                cooperative_cancel: true,
            }),
        )
        .unwrap();
    let service = builder.build().await.unwrap();
    let accepted = service.submit(request()).await.unwrap();
    for _ in 0..1000 {
        if service
            .get(accepted.id)
            .await
            .unwrap()
            .is_some_and(|summary| matches!(summary.state, TaskState::Running))
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    let shutdown_service = service.clone();
    let shutdown = tokio::spawn(async move { shutdown_service.shutdown().await });
    assert_eq!(
        service.cancel(accepted.id).await.unwrap(),
        CancelOutcome::CancellationRequested
    );
    shutdown.await.unwrap().unwrap();
    assert!(matches!(
        service.submit(request()).await,
        Err(qubit_task::service::TaskServiceError::ShuttingDown)
    ));
    assert_eq!(wait_for_terminal(&service, accepted.id).await, TaskState::Cancelled);
}

async fn assert_dropping_last_service_handle_drains_and_releases_owner(store: Arc<dyn TaskStore>) {
    let started = Arc::new(tokio::sync::Semaphore::new(0));
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let mut builder =
        TaskExecutionServiceBuilder::new(Arc::clone(&store), registry(), Arc::new(Ids(AtomicU64::new(801))));
    builder
        .handlers_mut()
        .register::<Counter, _>(
            descriptor(CancellationMode::Cooperative),
            Arc::new(GatedHandler {
                started: Arc::clone(&started),
                release: Arc::clone(&release),
            }),
        )
        .expect("gated handler registers");
    let service = builder.build().await.expect("first service acquires owner");
    service.submit(request()).await.expect("task is accepted");
    tokio::time::timeout(std::time::Duration::from_secs(2), started.acquire())
        .await
        .expect("handler starts")
        .expect("start gate remains open")
        .forget();
    drop(service);

    let build_next =
        || TaskExecutionServiceBuilder::new(Arc::clone(&store), registry(), Arc::new(Ids(AtomicU64::new(802))));
    assert!(matches!(
        build_next().build().await,
        Err(qubit_task::service::TaskServiceError::Store(
            qubit_task::store::StoreError::OwnerConflict
        ))
    ));
    release.add_permits(1);
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            match build_next().build().await {
                Ok(next) => {
                    next.shutdown().await.expect("second service shuts down");
                    break;
                }
                Err(qubit_task::service::TaskServiceError::Store(qubit_task::store::StoreError::OwnerConflict)) => {
                    tokio::task::yield_now().await;
                }
                Err(error) => panic!("unexpected second build error: {error}"),
            }
        }
    })
    .await
    .expect("dropping the last service handle eventually releases owner");
}

#[tokio::test]
async fn dropping_last_service_handle_drains_and_releases_owner_memory() {
    let store: Arc<dyn TaskStore> = Arc::new(MemoryTaskStore::new(16));
    assert_dropping_last_service_handle_drains_and_releases_owner(store).await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn dropping_last_service_handle_drains_and_releases_owner_sqlite() {
    let path = std::env::temp_dir().join(format!(
        "qubit-task-drop-owner-{}.sqlite",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time after epoch")
            .as_nanos(),
    ));
    let store: Arc<dyn TaskStore> =
        Arc::new(qubit_task::store::SqliteTaskStore::open(&path).expect("SQLite store opens"));
    assert_dropping_last_service_handle_drains_and_releases_owner(Arc::clone(&store)).await;
    drop(store);
    let mut lock_path = path.as_os_str().to_owned();
    lock_path.push(".owner.lock");
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(std::path::PathBuf::from(lock_path));
}

#[tokio::test]
async fn dropping_one_of_multiple_service_handles_keeps_owner() {
    let store: Arc<dyn TaskStore> = Arc::new(MemoryTaskStore::new(16));
    let build = || TaskExecutionServiceBuilder::new(Arc::clone(&store), registry(), Arc::new(Ids(AtomicU64::new(811))));
    let first = build().build().await.expect("first service acquires owner");
    let remaining = first.clone();
    drop(first);
    remaining
        .submit(request())
        .await
        .expect("remaining handle still accepts work");
    assert!(matches!(
        build().build().await,
        Err(qubit_task::service::TaskServiceError::Store(
            qubit_task::store::StoreError::OwnerConflict
        ))
    ));
    remaining.shutdown().await.expect("remaining handle shuts down");
    let next = build().build().await.expect("next owner acquires store");
    next.shutdown().await.expect("next owner shuts down");
}

#[tokio::test]
async fn concurrent_shutdown_calls_share_result() {
    let store: Arc<dyn TaskStore> = Arc::new(MemoryTaskStore::new(16));
    let service = TaskExecutionServiceBuilder::new(store, registry(), Arc::new(Ids(AtomicU64::new(821))))
        .build()
        .await
        .expect("service builds");
    let (first, second) = tokio::join!(service.shutdown(), service.shutdown());
    assert!(first.is_ok(), "first shutdown result: {first:?}");
    assert!(second.is_ok(), "second shutdown result: {second:?}");
}

#[tokio::test]
async fn aborted_shutdown_waiter_does_not_cancel_owner_release() {
    let store: Arc<dyn TaskStore> = Arc::new(MemoryTaskStore::new(16));
    let started = Arc::new(tokio::sync::Semaphore::new(0));
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let mut builder =
        TaskExecutionServiceBuilder::new(Arc::clone(&store), registry(), Arc::new(Ids(AtomicU64::new(831))));
    builder
        .handlers_mut()
        .register::<Counter, _>(
            descriptor(CancellationMode::Cooperative),
            Arc::new(GatedHandler {
                started: Arc::clone(&started),
                release: Arc::clone(&release),
            }),
        )
        .expect("gated handler registers");
    let service = builder.build().await.expect("first service acquires owner");
    service.submit(request()).await.expect("task is accepted");
    tokio::time::timeout(std::time::Duration::from_secs(2), started.acquire())
        .await
        .expect("handler starts")
        .expect("start gate remains open")
        .forget();

    let waiter_service = service.clone();
    let waiter = tokio::spawn(async move { waiter_service.shutdown().await });
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if matches!(
                service.submit(request()).await,
                Err(qubit_task::service::TaskServiceError::ShuttingDown)
            ) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("shutdown request closes admission");
    waiter.abort();
    let _ = waiter.await;
    release.add_permits(1);

    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            match TaskExecutionServiceBuilder::new(Arc::clone(&store), registry(), Arc::new(Ids(AtomicU64::new(832))))
                .build()
                .await
            {
                Ok(next) => {
                    next.shutdown().await.expect("next service shuts down");
                    break;
                }
                Err(qubit_task::service::TaskServiceError::Store(qubit_task::store::StoreError::OwnerConflict)) => {
                    tokio::task::yield_now().await;
                }
                Err(error) => panic!("unexpected next build error: {error}"),
            }
        }
    })
    .await
    .expect("supervisor releases owner after waiter is aborted");
    service
        .shutdown()
        .await
        .expect("aborted waiter does not change terminal result");
}

#[tokio::test]
async fn concurrent_shutdown_calls_share_store_fault() {
    let store = failing_store();
    let service = TaskExecutionServiceBuilder::new(store.clone(), registry(), Arc::new(Ids(AtomicU64::new(841))))
        .build()
        .await
        .expect("service builds");
    store.fail_next_list.store(true, Ordering::Release);
    service.submit(request()).await.expect("task is accepted");
    wait_for_latched_store_fault(&service).await;
    let (first, second) = tokio::join!(service.shutdown(), service.shutdown());
    let (
        Err(qubit_task::service::TaskServiceError::StoreUnavailable(first)),
        Err(qubit_task::service::TaskServiceError::StoreUnavailable(second)),
    ) = (first, second)
    else {
        panic!("both shutdown callers must observe the same store fault class");
    };
    assert_eq!(first, second);
    assert!(first.contains("injected list failure"));
}

#[tokio::test]
async fn panicking_owner_release_completes_shutdown_and_retries_on_cleanup_worker() {
    let store = failing_store();
    let service = TaskExecutionServiceBuilder::new(store.clone(), registry(), Arc::new(Ids(AtomicU64::new(851))))
        .build()
        .await
        .expect("first service builds");
    store.panic_next_release.store(true, Ordering::Release);

    let shutdown = tokio::time::timeout(std::time::Duration::from_secs(2), service.shutdown())
        .await
        .expect("shutdown waiter is woken after release panic");
    assert!(matches!(
        shutdown,
        Err(qubit_task::service::TaskServiceError::StoreUnavailable(message)) if message.contains("shutdown supervisor panicked")
    ));
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            match TaskExecutionServiceBuilder::new(store.clone(), registry(), Arc::new(Ids(AtomicU64::new(852))))
                .build()
                .await
            {
                Ok(next) => {
                    next.shutdown().await.expect("next service shuts down");
                    break;
                }
                Err(qubit_task::service::TaskServiceError::Store(qubit_task::store::StoreError::OwnerConflict)) => {
                    tokio::task::yield_now().await;
                }
                Err(error) => panic!("unexpected next build error: {error}"),
            }
        }
    })
    .await
    .expect("cleanup worker retries armed owner after supervisor panic");
}

#[tokio::test]
async fn subsequent_shutdown_retries_transient_owner_release_failure() {
    let store = failing_store();
    let service = TaskExecutionServiceBuilder::new(store.clone(), registry(), Arc::new(Ids(AtomicU64::new(861))))
        .build()
        .await
        .expect("first service builds");
    store.fail_next_release.store(true, Ordering::Release);

    let (first, concurrent) = tokio::join!(service.shutdown(), service.shutdown());
    let (
        Err(qubit_task::service::TaskServiceError::StoreUnavailable(first)),
        Err(qubit_task::service::TaskServiceError::StoreUnavailable(concurrent)),
    ) = (first, concurrent)
    else {
        panic!("both waiters in the first attempt must see its release failure");
    };
    assert_eq!(first, concurrent);
    assert!(first.contains("injected release failure"));
    service
        .shutdown()
        .await
        .expect("later explicit shutdown retries owner release");
    let next = TaskExecutionServiceBuilder::new(store, registry(), Arc::new(Ids(AtomicU64::new(862))))
        .build()
        .await
        .expect("next service acquires the released owner");
    next.shutdown().await.expect("next service shuts down");
}

#[tokio::test]
async fn shutdown_fences_cancel_calls_from_previous_owner() {
    let store: Arc<dyn TaskStore> = Arc::new(MemoryTaskStore::new(16));
    let first_builder =
        TaskExecutionServiceBuilder::new(Arc::clone(&store), registry(), Arc::new(Ids(AtomicU64::new(761))));
    let first = first_builder.build().await.unwrap();
    first.shutdown().await.unwrap();

    let mut second_builder = TaskExecutionServiceBuilder::new(store, registry(), Arc::new(Ids(AtomicU64::new(762))));
    second_builder
        .handlers_mut()
        .register::<Counter, _>(
            descriptor(CancellationMode::Cooperative),
            Arc::new(Handler {
                cooperative_cancel: true,
            }),
        )
        .unwrap();
    let second = second_builder.build().await.unwrap();
    let accepted = second.submit(request()).await.unwrap();
    for _ in 0..1000 {
        if second
            .get(accepted.id)
            .await
            .unwrap()
            .is_some_and(|summary| matches!(summary.state, TaskState::Running))
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }

    let before = second.get(accepted.id).await.unwrap().unwrap();
    assert!(matches!(
        first.cancel(accepted.id).await,
        Err(qubit_task::service::TaskServiceError::ShuttingDown)
    ));
    let after = second.get(accepted.id).await.unwrap().unwrap();
    assert_eq!(after, before);

    assert_eq!(
        second.cancel(accepted.id).await.unwrap(),
        CancelOutcome::CancellationRequested
    );
    assert_eq!(wait_for_terminal(&second, accepted.id).await, TaskState::Cancelled);
    second.shutdown().await.unwrap();
}

#[tokio::test]
async fn shutdown_waits_for_external_cancel_hook() {
    struct HookWaitHandler(Arc<tokio::sync::Notify>);

    impl TaskHandler<Counter> for HookWaitHandler {
        fn run<'a>(
            &'a self,
            _value: Counter,
            _context: TaskContext,
        ) -> TaskFuture<'a, qubit_task::handler::TaskRunResult> {
            let hook_finished = Arc::clone(&self.0);
            Box::pin(async move {
                hook_finished.notified().await;
                Ok(TaskRunOutcome::Succeeded(qubit_task::model::TaskOutput::default()))
            })
        }
    }

    let store = Arc::new(MemoryTaskStore::new(16));
    let hook_started = Arc::new(tokio::sync::Notify::new());
    let hook_release = Arc::new(tokio::sync::Notify::new());
    let hook_finished = Arc::new(tokio::sync::Notify::new());
    let hook_signal = Arc::clone(&hook_started);
    let hook_wait = Arc::clone(&hook_release);
    let handler_signal = Arc::clone(&hook_finished);
    let hook: qubit_task::ExternalCancellationHook = Arc::new(move |_, _| {
        let hook_signal = Arc::clone(&hook_signal);
        let hook_wait = Arc::clone(&hook_wait);
        let handler_signal = Arc::clone(&handler_signal);
        Box::pin(async move {
            hook_signal.notify_one();
            hook_wait.notified().await;
            handler_signal.notify_one();
            Ok(())
        })
    });
    let mut builder = TaskExecutionServiceBuilder::new(store, registry(), Arc::new(Ids(AtomicU64::new(771))));
    builder
        .handlers_mut()
        .register_with_cancellation_hook::<Counter, _>(
            descriptor(CancellationMode::ExternalHook),
            Arc::new(HookWaitHandler(Arc::clone(&hook_finished))),
            "test",
            hook,
        )
        .unwrap();
    let service = builder.build().await.unwrap();
    let accepted = service.submit(request()).await.unwrap();
    for _ in 0..1000 {
        if service
            .get(accepted.id)
            .await
            .unwrap()
            .is_some_and(|summary| matches!(summary.state, TaskState::Running))
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }

    let cancel_service = service.clone();
    let cancel = tokio::spawn(async move { cancel_service.cancel(accepted.id).await });
    hook_started.notified().await;
    let shutdown_service = service.clone();
    let shutdown = tokio::spawn(async move { shutdown_service.shutdown().await });
    tokio::task::yield_now().await;
    assert!(!shutdown.is_finished());
    hook_release.notify_one();
    assert_eq!(cancel.await.unwrap().unwrap(), CancelOutcome::CancellationRequested);
    shutdown.await.unwrap().unwrap();
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn typed_service_persists_sqlite_terminal_transition() {
    let path = std::env::temp_dir().join(format!(
        "qubit-task-typed-service-{}.sqlite",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    ));
    let store: Arc<dyn TaskStore> = Arc::new(qubit_task::store::SqliteTaskStore::open(&path).unwrap());
    let mut builder =
        TaskExecutionServiceBuilder::new(Arc::clone(&store), registry(), Arc::new(Ids(AtomicU64::new(1001))));
    builder
        .handlers_mut()
        .register::<Counter, _>(
            descriptor(CancellationMode::Cooperative),
            Arc::new(Handler {
                cooperative_cancel: false,
            }),
        )
        .unwrap();
    let service = builder.build().await.unwrap();
    let accepted = service.submit(request()).await.unwrap();
    assert_eq!(wait_for_terminal(&service, accepted.id).await, TaskState::Succeeded);
    service.shutdown().await.unwrap();
    drop(service);
    drop(store);
    let mut lock_path = path.as_os_str().to_owned();
    lock_path.push(".owner.lock");
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(std::path::PathBuf::from(lock_path));
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn typed_service_fences_build_until_shutdown_releases_owner() {
    let path = std::env::temp_dir().join(format!(
        "qubit-task-typed-owner-{}.sqlite",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    ));
    let store: Arc<dyn TaskStore> = Arc::new(qubit_task::store::SqliteTaskStore::open(&path).unwrap());
    let build =
        || TaskExecutionServiceBuilder::new(Arc::clone(&store), registry(), Arc::new(Ids(AtomicU64::new(1101))));
    let first = build().build().await.unwrap();
    let second = build().build().await;
    assert!(matches!(
        second,
        Err(qubit_task::service::TaskServiceError::Store(
            qubit_task::store::StoreError::OwnerConflict
        ))
    ));
    first.shutdown().await.unwrap();
    let third = build().build().await.unwrap();
    third.shutdown().await.unwrap();
    drop(third);
    drop(first);
    drop(store);
    let mut lock_path = path.as_os_str().to_owned();
    lock_path.push(".owner.lock");
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(std::path::PathBuf::from(lock_path));
}
