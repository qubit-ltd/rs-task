// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::array::TryFromSliceError;
use std::convert::Infallible;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

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
use qubit_task::TaskExecutionService;
use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::TaskHandler;
use qubit_task::TaskHandlerDescriptor;
use qubit_task::handler::TaskRunOutcome;
use qubit_task::handler::TaskRunResult;
use qubit_task::model::AcceptOutcome;
use qubit_task::model::OwnerEpoch;
use qubit_task::model::ProgressCommand;
use qubit_task::model::ResourceCapacity;
use qubit_task::model::ResourceRequest;
use qubit_task::model::StartCommand;
use qubit_task::model::StoreCapabilities;
use qubit_task::model::StoredPayload;
use qubit_task::model::StoredTask;
use qubit_task::model::StoredTaskRequest;
use qubit_task::model::TaskCursor;
use qubit_task::model::TaskId;
use qubit_task::model::TaskOutput;
use qubit_task::model::TaskPage;
use qubit_task::model::TaskQuery;
use qubit_task::model::TaskRequest;
use qubit_task::model::TaskState;
use qubit_task::model::TaskSummary;
use qubit_task::model::TransitionCommand;
use qubit_task::service::CancelOutcome;
use qubit_task::store::MemoryTaskStore;
use qubit_task::store::StoreError;
use qubit_task::store::TaskFuture;
use qubit_task::store::TaskStore;
use tokio::sync::Notify;
use tokio::sync::Semaphore;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Counter(u32);

impl HasModelId for Counter {
    const MODEL_ID: ModelId = ModelId::new("test.ResourceReservationPayload");
}

#[derive(Default)]
struct U32Codec;

impl ValueEncoder<Counter> for U32Codec {
    type Output = Vec<u8>;
    type Error = Infallible;

    fn encode(&mut self, value: &Counter) -> Result<Vec<u8>, Self::Error> {
        Ok(value.0.to_le_bytes().to_vec())
    }
}

impl ValueDecoder<[u8]> for U32Codec {
    type Output = Counter;
    type Error = TryFromSliceError;

    fn decode(&mut self, bytes: &[u8]) -> Result<Counter, Self::Error> {
        let raw: [u8; 4] = bytes.try_into()?;
        Ok(Counter(u32::from_le_bytes(raw)))
    }
}

static CODEC_DESCRIPTOR: ValueBytesCodecDescriptor = ValueBytesCodecDescriptor::of::<U32Codec, Counter>();
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

impl IdGenerator for Ids {
    fn generate(&self) -> Result<Id, IdGenerationError> {
        Ok(Id::new(self.0.fetch_add(1, Ordering::Relaxed)))
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

fn request(cpu_slots: u32) -> TaskRequest<Counter> {
    let mut request = TaskRequest::new(
        "test.resource-reservation",
        1,
        ValueCodecId::new("qubit_task.typed_resource_tests.u32"),
        Counter(7),
    );
    request.resource_limit = ResourceRequest {
        cpu_slots,
        ..ResourceRequest::default()
    };
    request
}

fn request_resources(cpu_slots: u32, gpu_count: u32) -> TaskRequest<Counter> {
    let mut task = request(cpu_slots);
    task.resource_limit.gpu_count = gpu_count;
    task
}

struct ReadyScanGate {
    armed: AtomicBool,
    reached: Notify,
    release: Semaphore,
    completed: Notify,
}

struct ReadyScanRelease {
    gate: Arc<ReadyScanGate>,
    released: bool,
}

impl ReadyScanRelease {
    fn new(gate: Arc<ReadyScanGate>) -> Self {
        Self { gate, released: false }
    }

    fn release(&mut self) {
        self.gate.release.add_permits(1);
        self.released = true;
    }
}

impl Drop for ReadyScanRelease {
    fn drop(&mut self) {
        if !self.released {
            self.gate.release.add_permits(1);
        }
    }
}

#[derive(Default)]
struct StartTrace {
    ids: Mutex<Vec<TaskId>>,
    changed: Notify,
    conflicts: Mutex<Vec<TaskId>>,
    ready_scans: Mutex<Vec<Option<TaskCursor>>>,
    pending_scan_time: Mutex<Option<u64>>,
    scan_deadline_pairs: Mutex<Vec<ScanDeadlinePair>>,
    deadline_changed: Notify,
    ready_scan_changed: Notify,
    blocked_transitions: AtomicUsize,
    yield_probe: Mutex<Option<Arc<YieldProbe>>>,
}

struct YieldProbe {
    first_page_returned: Notify,
    observed_page_count: AtomicUsize,
}

#[derive(Clone, Copy, Debug)]
struct ScanDeadlinePair {
    scan_now_ms: Option<u64>,
    deadline_now_ms: u64,
    next_retry_deadline_ms: Option<u64>,
    blocked_transition_count: usize,
}

struct StartCasGate {
    armed: AtomicBool,
    reached: Notify,
    release: Semaphore,
}

struct GatedReadyStore {
    inner: Arc<dyn TaskStore>,
    gate: Arc<ReadyScanGate>,
    start_gate: Option<Arc<StartCasGate>>,
    trace: Option<Arc<StartTrace>>,
}

fn test_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock is after the Unix epoch")
        .as_millis() as u64
}

async fn retry_deadline_crossing_boundary(store: Arc<dyn TaskStore>, id: TaskId) -> Result<(), String> {
    let gate = Arc::new(ReadyScanGate {
        armed: AtomicBool::new(false),
        reached: Notify::new(),
        release: Semaphore::new(0),
        completed: Notify::new(),
    });
    let trace = Arc::new(StartTrace::default());
    let gated_store = Arc::new(GatedReadyStore {
        inner: Arc::clone(&store),
        gate: Arc::clone(&gate),
        start_gate: None,
        trace: Some(Arc::clone(&trace)),
    });
    let started_handler = Arc::new(Notify::new());
    let handler_release = Arc::new(Semaphore::new(1));
    let codecs = Arc::new(ValueBytesCodecRegistry::from_registrations([&CODEC_REGISTRATION]).unwrap());
    let mut builder = TaskExecutionServiceBuilder::new(gated_store, codecs, Arc::new(Ids(AtomicU64::new(9000))))
        .capacity(ResourceCapacity {
            cpu_slots: 1,
            ..ResourceCapacity::default()
        });
    builder
        .handlers_mut()
        .register::<Counter, _>(
            descriptor(),
            Arc::new(GatedHandler {
                started: Arc::clone(&started_handler),
                release: handler_release,
                calls: Arc::new(AtomicUsize::new(0)),
            }),
        )
        .expect("handler registration");
    let service = builder.build().await.expect("service build");
    if wait_for_deadline_queries(&trace, 2).await.is_err() {
        let _ = service.shutdown().await;
        return Err("startup scheduler notifications were not drained".into());
    }

    let reached = gate.reached.notified();
    tokio::pin!(reached);
    reached.as_mut().enable();
    gate.armed.store(true, Ordering::Release);
    let mut wake_request = TaskRequest::new(
        "test.unregistered-wakeup",
        1,
        ValueCodecId::new("qubit_task.typed_resource_tests.u32"),
        Counter(7),
    );
    wake_request.resource_limit = ResourceRequest::default();
    if let Err(error) = service.submit(wake_request).await {
        let _ = service.shutdown().await;
        return Err(format!("wake task submit failed: {error}"));
    }
    if tokio::time::timeout(Duration::from_secs(2), reached).await.is_err() {
        gate.release.add_permits(1);
        let _ = service.shutdown().await;
        return Err("scheduler did not reach the gated ready scan".into());
    }
    let mut gate_release = ReadyScanRelease::new(Arc::clone(&gate));
    let scan_time = trace
        .pending_scan_time
        .lock()
        .expect("pending scan-time lock")
        .expect("gated scan time is recorded");
    let retry_deadline = scan_time.saturating_add(250);
    let now = test_now_ms();
    let accepted = store
        .accept_encoded(
            id,
            StoredTaskRequest {
                kind_id: "test.resource-reservation".into(),
                category: None,
                payload: StoredPayload {
                    type_id: ModelIdBuf::parse("test.ResourceReservationPayload").expect("model ID"),
                    schema_version: 1,
                    codec_id: "qubit_task.typed_resource_tests.u32".into(),
                    bytes: 7_u32.to_le_bytes().to_vec(),
                },
                metadata: qubit_metadata::Metadata::new(),
                resource_limit: ResourceRequest::default(),
                correlation_key: None,
                idempotency_key: None,
            },
        )
        .await
        .expect("retry task is accepted")
        .summary;
    let started = store
        .start_encoded(StartCommand {
            id,
            expected_state_version: accepted.state_version,
            started_at_ms: now,
        })
        .await
        .expect("first attempt is started");
    store
        .transition_encoded(TransitionCommand {
            id,
            expected_state_version: started.state_version,
            expected_attempt: started.attempt,
            retry_not_before_ms: Some(retry_deadline),
            state: TaskState::Queued,
            cancel_requested: false,
            cancel_error: None,
            finished_at_ms: None,
            output: None,
        })
        .await
        .expect("failed attempt is queued for a timed retry");
    assert!(scan_time < retry_deadline, "the gated scan precedes the retry deadline");
    tokio::time::sleep(Duration::from_millis(retry_deadline.saturating_sub(test_now_ms()) + 10)).await;
    gate_release.release();

    let terminal = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(summary) = service.get(id).await.expect("retry task query succeeds")
                && summary.state.is_terminal()
            {
                break summary.state;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await;
    service
        .shutdown()
        .await
        .map_err(|error| format!("service shutdown failed: {error}"))?;
    let terminal = terminal.map_err(|_| "retry did not become ready without an external notification".to_owned())?;
    if terminal != TaskState::Succeeded {
        return Err(format!("retry reached {terminal:?}, expected Succeeded"));
    }
    let pair = wait_for_scan_deadline_pair(&trace, |pair| {
        pair.scan_now_ms == Some(scan_time) && pair.next_retry_deadline_ms == Some(retry_deadline)
    })
    .await;
    assert_eq!(
        pair.scan_now_ms,
        Some(pair.deadline_now_ms),
        "scan and deadline lookup share one time sample"
    );
    Ok(())
}

async fn wait_for_deadline_queries(trace: &StartTrace, count: usize) -> Result<(), ()> {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let changed = trace.deadline_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if trace
                .scan_deadline_pairs
                .lock()
                .expect("scan/deadline trace lock")
                .len()
                >= count
            {
                return;
            }
            changed.await;
        }
    })
    .await
    .map_err(|_| ())
}

async fn wait_for_scan_deadline_pair(
    trace: &StartTrace,
    predicate: impl Fn(&ScanDeadlinePair) -> bool,
) -> ScanDeadlinePair {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let changed = trace.deadline_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let Some(pair) = trace
                .scan_deadline_pairs
                .lock()
                .expect("scan/deadline trace lock")
                .iter()
                .copied()
                .find(|pair| predicate(pair))
            {
                return pair;
            }
            changed.await;
        }
    })
    .await
    .expect("scheduler does not record the expected scan/deadline pair")
}

fn ready_scan_count(trace: &StartTrace) -> usize {
    trace.ready_scans.lock().expect("ready scan trace lock").len()
}

async fn assert_no_additional_ready_scan(trace: &StartTrace, baseline: usize) {
    let additional = tokio::time::timeout(Duration::from_millis(100), async {
        loop {
            let changed = trace.ready_scan_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if ready_scan_count(trace) > baseline {
                return;
            }
            changed.await;
        }
    })
    .await;
    assert!(additional.is_err(), "scheduler performed another ready scan while idle");
}

#[tokio::test]
async fn test_retry_deadline_crossing_scan_boundary_is_not_lost_memory() {
    retry_deadline_crossing_boundary(Arc::new(MemoryTaskStore::new(8)), TaskId::from_id(Id::new(9901)))
        .await
        .expect("retry deadline crossing remains scheduled");
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_retry_deadline_crossing_scan_boundary_is_not_lost_sqlite() {
    let path = std::env::temp_dir().join(format!("qubit-task-retry-boundary-{}.sqlite", test_now_ms()));
    let store: Arc<dyn TaskStore> =
        Arc::new(qubit_task::store::SqliteTaskStore::open(&path).expect("SQLite store opens"));
    let result = retry_deadline_crossing_boundary(store.clone(), TaskId::from_id(Id::new(test_now_ms()))).await;
    drop(store);
    for suffix in ["", ".owner.lock", "-wal", "-shm"] {
        let mut file = path.as_os_str().to_os_string();
        file.push(suffix);
        let _ = std::fs::remove_file(std::path::PathBuf::from(file));
    }
    result.expect("retry deadline crossing remains scheduled");
}

impl TaskStore for GatedReadyStore {
    fn capabilities(&self) -> StoreCapabilities {
        self.inner.capabilities()
    }
    fn accept_encoded<'a>(
        &'a self,
        id: TaskId,
        request: StoredTaskRequest,
    ) -> TaskFuture<'a, Result<AcceptOutcome, StoreError>> {
        self.inner.accept_encoded(id, request)
    }
    fn get_encoded_task<'a>(&'a self, id: TaskId) -> TaskFuture<'a, Result<Option<StoredTask>, StoreError>> {
        self.inner.get_encoded_task(id)
    }
    fn get_state_checkpoint<'a>(
        &'a self,
        id: TaskId,
    ) -> TaskFuture<'a, Result<Option<qubit_task::model::TaskStateCheckpoint>, StoreError>> {
        self.inner.get_state_checkpoint(id)
    }
    fn start_encoded<'a>(&'a self, command: StartCommand) -> TaskFuture<'a, Result<TaskSummary, StoreError>> {
        Box::pin(async move {
            if self.trace.as_ref().is_some_and(|trace| {
                trace
                    .conflicts
                    .lock()
                    .expect("conflict trace lock is not poisoned")
                    .contains(&command.id)
            }) {
                return Err(StoreError::Conflict);
            }
            if let Some(gate) = &self.start_gate
                && gate.armed.swap(false, Ordering::AcqRel)
            {
                gate.reached.notify_one();
                gate.release.acquire().await.expect("start gate stays open").forget();
            }
            let result = self.inner.start_encoded(command).await;
            if let (Ok(summary), Some(trace)) = (&result, &self.trace) {
                trace
                    .ids
                    .lock()
                    .expect("start trace lock is not poisoned")
                    .push(summary.id);
                trace.changed.notify_waiters();
            }
            result
        })
    }
    fn transition_encoded<'a>(&'a self, command: TransitionCommand) -> TaskFuture<'a, Result<TaskSummary, StoreError>> {
        Box::pin(async move {
            let is_blocked = matches!(&command.state, TaskState::Blocked { .. });
            let result = self.inner.transition_encoded(command).await;
            if is_blocked
                && result.is_ok()
                && let Some(trace) = &self.trace
            {
                trace.blocked_transitions.fetch_add(1, Ordering::AcqRel);
            }
            result
        })
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
        limit: NonZeroUsize,
        now_ms: u64,
    ) -> TaskFuture<'a, Result<TaskPage, StoreError>> {
        Box::pin(async move {
            if let Some(trace) = &self.trace {
                *trace
                    .pending_scan_time
                    .lock()
                    .expect("pending scan-time lock is not poisoned") = Some(now_ms);
            }
            if after.is_none() && self.gate.armed.swap(false, Ordering::AcqRel) {
                self.gate.reached.notify_one();
                self.gate
                    .release
                    .acquire()
                    .await
                    .expect("scan gate stays open")
                    .forget();
            }
            if let Some(trace) = &self.trace {
                trace
                    .ready_scans
                    .lock()
                    .expect("ready scan trace lock is not poisoned")
                    .push(after);
                trace.ready_scan_changed.notify_waiters();
            }
            let page = self.inner.list_ready_queued(after, limit, now_ms).await?;
            if after.is_none()
                && page.next.is_some()
                && let Some(trace) = &self.trace
                && let Some(probe) = trace
                    .yield_probe
                    .lock()
                    .expect("yield-probe lock is not poisoned")
                    .as_ref()
            {
                probe.first_page_returned.notify_one();
            }
            Ok(page)
        })
    }
    fn next_retry_deadline<'a>(&'a self, now_ms: u64) -> TaskFuture<'a, Result<Option<u64>, StoreError>> {
        Box::pin(async move {
            let scan_now_ms = self.trace.as_ref().map(|trace| {
                trace
                    .pending_scan_time
                    .lock()
                    .expect("pending scan-time lock is not poisoned")
                    .take()
            });
            let result = self.inner.next_retry_deadline(now_ms).await;
            if let Some(trace) = &self.trace {
                let next_retry_deadline_ms = match &result {
                    Ok(deadline) => *deadline,
                    Err(_) => None,
                };
                trace
                    .scan_deadline_pairs
                    .lock()
                    .expect("scan/deadline trace lock is not poisoned")
                    .push(ScanDeadlinePair {
                        scan_now_ms: scan_now_ms.flatten(),
                        deadline_now_ms: now_ms,
                        next_retry_deadline_ms,
                        blocked_transition_count: trace.blocked_transitions.load(Ordering::Acquire),
                    });
                trace.deadline_changed.notify_waiters();
            }
            self.gate.completed.notify_waiters();
            result
        })
    }
    fn prune_terminal_before<'a>(
        &'a self,
        cutoff: u64,
        max_rows: NonZeroUsize,
    ) -> TaskFuture<'a, Result<usize, StoreError>> {
        self.inner.prune_terminal_before(cutoff, max_rows)
    }
    fn acquire_owner<'a>(&'a self) -> TaskFuture<'a, Result<OwnerEpoch, StoreError>> {
        self.inner.acquire_owner()
    }
    fn release_owner<'a>(&'a self, epoch: OwnerEpoch) -> TaskFuture<'a, Result<(), StoreError>> {
        self.inner.release_owner(epoch)
    }
}

async fn wait_for(service: &TaskExecutionService, id: TaskId, predicate: impl Fn(&TaskState) -> bool) -> TaskState {
    for _ in 0..1000 {
        if let Some(summary) = service.get(id).await.unwrap()
            && predicate(&summary.state)
        {
            return summary.state;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    panic!("typed task did not reach the expected state")
}

/// Waits for the scheduler to start at least `count` tasks and returns their
/// order.
async fn wait_for_starts(trace: &StartTrace, count: usize) -> Vec<TaskId> {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let changed = trace.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let ids = trace.ids.lock().expect("start trace lock is not poisoned").clone();
            if ids.len() >= count {
                return ids;
            }
            changed.await;
        }
    })
    .await
    .expect("scheduler did not start the expected tasks")
}

/// Builds a traced scheduler with one GPU, two CPU slots, and a gated first
/// handler.
async fn bypass_fixture(
    page_size: usize,
) -> (
    TaskExecutionService,
    Arc<ReadyScanGate>,
    Arc<StartTrace>,
    Arc<Notify>,
    Arc<Semaphore>,
) {
    let gate = Arc::new(ReadyScanGate {
        armed: AtomicBool::new(false),
        reached: Notify::new(),
        release: Semaphore::new(0),
        completed: Notify::new(),
    });
    let trace = Arc::new(StartTrace::default());
    let store = Arc::new(GatedReadyStore {
        inner: Arc::new(MemoryTaskStore::new(8)),
        gate: Arc::clone(&gate),
        start_gate: None,
        trace: Some(Arc::clone(&trace)),
    });
    let started = Arc::new(Notify::new());
    let release = Arc::new(Semaphore::new(0));
    let codecs =
        Arc::new(ValueBytesCodecRegistry::from_registrations([&CODEC_REGISTRATION]).expect("codec registration"));
    let mut builder = TaskExecutionServiceBuilder::new(store, codecs, Arc::new(Ids(AtomicU64::new(1000))))
        .capacity(ResourceCapacity {
            cpu_slots: 2,
            gpus: [("gpu-0".to_owned(), vec!["test".to_owned()])].into(),
            ..ResourceCapacity::default()
        })
        .max_running_tasks(NonZeroUsize::new(2).expect("positive run limit"))
        .scan_page_size(NonZeroUsize::new(page_size).expect("positive page size"))
        .max_resource_bypasses(NonZeroUsize::new(1).expect("positive bypass limit"));
    builder
        .handlers_mut()
        .register::<Counter, _>(
            descriptor(),
            Arc::new(GatedHandler {
                started: Arc::clone(&started),
                release: Arc::clone(&release),
                calls: Arc::new(AtomicUsize::new(0)),
            }),
        )
        .expect("handler registration");
    (
        builder.build().await.expect("service build"),
        gate,
        trace,
        started,
        release,
    )
}

/// Runs one complete scheduler scan after a new submission, with a stable
/// observation point.
async fn complete_gated_scan(gate: &ReadyScanGate) {
    let reached = gate.reached.notified();
    tokio::pin!(reached);
    reached.as_mut().enable();
    tokio::time::timeout(Duration::from_secs(2), reached)
        .await
        .expect("scheduler did not reach ready scan");
    let completed = gate.completed.notified();
    tokio::pin!(completed);
    completed.as_mut().enable();
    gate.release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(2), completed)
        .await
        .expect("scheduler did not complete ready scan");
}

struct GatedHandler {
    started: Arc<Notify>,
    release: Arc<Semaphore>,
    calls: Arc<AtomicUsize>,
}

impl TaskHandler<Counter> for GatedHandler {
    fn run<'a>(&'a self, _value: Counter, _context: TaskContext) -> TaskFuture<'a, TaskRunResult> {
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

#[tokio::test(flavor = "current_thread")]
async fn test_ready_scan_yields_between_pages_and_honors_shutdown() {
    const TIMEOUT: Duration = Duration::from_secs(2);
    let gate = Arc::new(ReadyScanGate {
        armed: AtomicBool::new(false),
        reached: Notify::new(),
        release: Semaphore::new(0),
        completed: Notify::new(),
    });
    let trace = Arc::new(StartTrace::default());
    let store = Arc::new(GatedReadyStore {
        inner: Arc::new(MemoryTaskStore::new(16)),
        gate: Arc::clone(&gate),
        start_gate: None,
        trace: Some(Arc::clone(&trace)),
    });
    let started = Arc::new(Notify::new());
    let handler_release = Arc::new(Semaphore::new(0));
    let mut builder = TaskExecutionServiceBuilder::new(
        store,
        Arc::new(ValueBytesCodecRegistry::from_registrations([&CODEC_REGISTRATION]).unwrap()),
        Arc::new(Ids(AtomicU64::new(40_000))),
    )
    .capacity(ResourceCapacity {
        cpu_slots: 1,
        ..ResourceCapacity::default()
    })
    .max_running_tasks(NonZeroUsize::new(2).expect("positive run limit"))
    .scan_page_size(NonZeroUsize::new(2).expect("positive page size"))
    .max_resource_bypasses(NonZeroUsize::new(1).expect("positive bypass limit"));
    builder
        .handlers_mut()
        .register::<Counter, _>(
            descriptor(),
            Arc::new(GatedHandler {
                started: Arc::clone(&started),
                release: Arc::clone(&handler_release),
                calls: Arc::new(AtomicUsize::new(0)),
            }),
        )
        .expect("handler registration");
    let service = builder.build().await.expect("service builds");
    wait_for_deadline_queries(&trace, 2)
        .await
        .expect("startup scheduler scans drain");
    let running = service
        .submit(request_resources(1, 0))
        .await
        .expect("running task accepted");
    if tokio::time::timeout(TIMEOUT, started.notified()).await.is_err() {
        handler_release.add_permits(1);
        let _ = tokio::time::timeout(TIMEOUT, service.shutdown()).await;
        panic!("first handler starts");
    }

    let scans_before_queue = ready_scan_count(&trace);
    gate.armed.store(true, Ordering::Release);
    let waiting_tasks = vec![
        service
            .submit(request_resources(1, 0))
            .await
            .expect("first waiting task accepted"),
        service
            .submit(request_resources(1, 0))
            .await
            .expect("second waiting task accepted"),
        service
            .submit(request_resources(1, 0))
            .await
            .expect("third waiting task accepted"),
        service
            .submit(request_resources(1, 0))
            .await
            .expect("fourth waiting task accepted"),
        service
            .submit(request_resources(1, 0))
            .await
            .expect("fifth waiting task accepted"),
    ];
    if tokio::time::timeout(TIMEOUT, gate.reached.notified()).await.is_err() {
        gate.release.add_permits(1);
        handler_release.add_permits(1);
        let _ = tokio::time::timeout(TIMEOUT, service.shutdown()).await;
        panic!("queued scan reaches its gate before paging");
    }

    let probe = Arc::new(YieldProbe {
        first_page_returned: Notify::new(),
        observed_page_count: AtomicUsize::new(0),
    });
    *trace.yield_probe.lock().expect("yield-probe lock is not poisoned") = Some(Arc::clone(&probe));
    let probe_trace = Arc::clone(&trace);
    let probe_task = Arc::clone(&probe);
    let probe_service = service.clone();
    let (probe_tx, probe_rx) = tokio::sync::oneshot::channel();
    let mut probe_worker = tokio::spawn(async move {
        probe_task.first_page_returned.notified().await;
        probe_task
            .observed_page_count
            .store(ready_scan_count(&probe_trace), Ordering::Release);
        let shutdown = probe_service.shutdown();
        tokio::pin!(shutdown);
        let shutdown_was_requested = futures::poll!(shutdown.as_mut()).is_pending();
        let _ = probe_tx.send(shutdown_was_requested);
        shutdown.await
    });

    let mut scan_gate_release = ReadyScanRelease::new(Arc::clone(&gate));
    scan_gate_release.release();
    let shutdown_request_observed = tokio::time::timeout(TIMEOUT, probe_rx).await;
    let queued_while_resource_held = {
        let mut all_queued = true;
        for task in &waiting_tasks {
            all_queued &= service
                .get(task.id)
                .await
                .expect("task lookup")
                .is_some_and(|summary| matches!(summary.state, TaskState::Queued));
        }
        all_queued
    };
    let shutdown = service.shutdown();
    tokio::pin!(shutdown);
    let _ = futures::poll!(shutdown.as_mut());
    handler_release.add_permits(1);
    let shutdown_result = tokio::time::timeout(TIMEOUT, shutdown).await;
    let probe_result = tokio::time::timeout(TIMEOUT, &mut probe_worker).await;
    if probe_result.is_err() {
        probe_worker.abort();
        let _ = probe_worker.await;
    }
    let only_running_task_started =
        trace.ids.lock().expect("start trace lock is not poisoned").as_slice() == [running.id];
    let observed_pages = probe.observed_page_count.load(Ordering::Acquire);
    let scans_after_shutdown = ready_scan_count(&trace);

    assert!(
        matches!(shutdown_request_observed, Ok(Ok(true))),
        "probe requested shutdown while the scan was between pages"
    );
    assert!(
        matches!(shutdown_result, Ok(Ok(()))),
        "shutdown drains the running handler"
    );
    assert!(
        matches!(probe_result, Ok(Ok(Ok(())))),
        "probe task completes after shutdown"
    );
    assert_eq!(
        observed_pages,
        scans_before_queue + 1,
        "probe runs after page one and before the next page is read"
    );
    assert_eq!(
        scans_after_shutdown,
        scans_before_queue + 1,
        "shutdown observed at the page boundary stops the remaining scan"
    );
    assert!(
        queued_while_resource_held,
        "resource pressure leaves queued tasks queued"
    );
    assert!(
        only_running_task_started,
        "blocked tasks do not exceed the configured bypass count"
    );
}

#[tokio::test]
async fn test_typed_resource_request_above_capacity_is_blocked() {
    let gate = Arc::new(ReadyScanGate {
        armed: AtomicBool::new(false),
        reached: Notify::new(),
        release: Semaphore::new(0),
        completed: Notify::new(),
    });
    let trace = Arc::new(StartTrace::default());
    let store = Arc::new(GatedReadyStore {
        inner: Arc::new(MemoryTaskStore::new(8)),
        gate,
        start_gate: None,
        trace: Some(Arc::clone(&trace)),
    });
    let codecs = Arc::new(ValueBytesCodecRegistry::from_registrations([&CODEC_REGISTRATION]).unwrap());
    let mut builder =
        TaskExecutionServiceBuilder::new(store, codecs, Arc::new(Ids(AtomicU64::new(1)))).capacity(ResourceCapacity {
            cpu_slots: 1,
            ..ResourceCapacity::default()
        });
    builder
        .handlers_mut()
        .register::<Counter, _>(
            descriptor(),
            Arc::new(GatedHandler {
                started: Arc::new(Notify::new()),
                release: Arc::new(Semaphore::new(0)),
                calls: Arc::new(AtomicUsize::new(0)),
            }),
        )
        .unwrap();
    let service = builder.build().await.unwrap();
    wait_for_deadline_queries(&trace, 2)
        .await
        .expect("startup scheduler scans drain");

    let accepted = service.submit(request(2)).await.unwrap();
    let state = wait_for(&service, accepted.id, |state| {
        matches!(state, TaskState::Blocked { .. })
    })
    .await;
    assert!(matches!(state, TaskState::Blocked { .. }));
    let blocked_scan = wait_for_scan_deadline_pair(&trace, |pair| pair.blocked_transition_count > 0).await;
    assert_eq!(
        blocked_scan.next_retry_deadline_ms, None,
        "resource-insufficient task has no retry deadline"
    );
    let scans_before_shutdown = ready_scan_count(&trace);
    assert_no_additional_ready_scan(&trace, scans_before_shutdown).await;
    let shutdown = tokio::spawn(async move { service.shutdown().await });
    tokio::time::timeout(Duration::from_secs(2), shutdown)
        .await
        .expect("shutdown notification wakes idle scheduler")
        .expect("shutdown task joins")
        .expect("service shuts down");
    assert_eq!(
        ready_scan_count(&trace),
        scans_before_shutdown,
        "shutdown wake does not trigger a ready scan"
    );
}

#[tokio::test]
async fn test_scheduler_waits_without_scanning_when_max_running_is_full() {
    let gate = Arc::new(ReadyScanGate {
        armed: AtomicBool::new(false),
        reached: Notify::new(),
        release: Semaphore::new(0),
        completed: Notify::new(),
    });
    let trace = Arc::new(StartTrace::default());
    let store = Arc::new(GatedReadyStore {
        inner: Arc::new(MemoryTaskStore::new(8)),
        gate,
        start_gate: None,
        trace: Some(Arc::clone(&trace)),
    });
    let started = Arc::new(Notify::new());
    let release = Arc::new(Semaphore::new(0));
    let codecs = Arc::new(ValueBytesCodecRegistry::from_registrations([&CODEC_REGISTRATION]).unwrap());
    let mut builder = TaskExecutionServiceBuilder::new(store, codecs, Arc::new(Ids(AtomicU64::new(50))))
        .max_running_tasks(NonZeroUsize::new(1).expect("nonzero limit"))
        .capacity(ResourceCapacity {
            cpu_slots: 1,
            ..ResourceCapacity::default()
        });
    builder
        .handlers_mut()
        .register::<Counter, _>(
            descriptor(),
            Arc::new(GatedHandler {
                started: Arc::clone(&started),
                release: Arc::clone(&release),
                calls: Arc::new(AtomicUsize::new(0)),
            }),
        )
        .expect("handler registration");
    let service = builder.build().await.expect("service builds");
    wait_for_deadline_queries(&trace, 2)
        .await
        .expect("startup scheduler scans drain");
    let first = service.submit(request(1)).await.expect("first task accepted");
    tokio::time::timeout(Duration::from_secs(2), started.notified())
        .await
        .expect("first handler starts");
    let second = service.submit(request(1)).await.expect("second task accepted");
    let full_scan = wait_for_scan_deadline_pair(&trace, |pair| pair.scan_now_ms.is_none()).await;
    assert_eq!(
        full_scan.next_retry_deadline_ms, None,
        "max-running-full scheduler has no retry deadline"
    );
    let scans_before_shutdown = ready_scan_count(&trace);
    assert_no_additional_ready_scan(&trace, scans_before_shutdown).await;

    let shutdown = service.shutdown();
    tokio::pin!(shutdown);
    assert!(
        futures::poll!(shutdown.as_mut()).is_pending(),
        "shutdown waits for the running handler"
    );
    release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(2), shutdown)
        .await
        .expect("shutdown notification wakes scheduler")
        .expect("service shuts down");
    assert_eq!(
        ready_scan_count(&trace),
        scans_before_shutdown,
        "shutdown wake does not trigger a ready scan"
    );
    assert!(matches!(
        service.get(first.id).await.unwrap().unwrap().state,
        TaskState::Succeeded
    ));
    assert!(matches!(
        service.get(second.id).await.unwrap().unwrap().state,
        TaskState::Queued
    ));
}

#[tokio::test]
async fn test_typed_resource_reservation_waits_until_capacity_is_released() {
    let codecs = Arc::new(ValueBytesCodecRegistry::from_registrations([&CODEC_REGISTRATION]).unwrap());
    let started = Arc::new(Notify::new());
    let release = Arc::new(Semaphore::new(0));
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
        .register::<Counter, _>(
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
    tokio::time::sleep(Duration::from_millis(100)).await;
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
async fn test_scheduler_skips_resource_blocked_task_without_consuming_run_slot() {
    let codecs = Arc::new(ValueBytesCodecRegistry::from_registrations([&CODEC_REGISTRATION]).unwrap());
    let started = Arc::new(Notify::new());
    let release = Arc::new(Semaphore::new(0));
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
    .max_running_tasks(NonZeroUsize::new(2).unwrap());
    builder
        .handlers_mut()
        .register::<Counter, _>(
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
async fn test_resource_bypass_limit_starts_old_gpu_task_before_second_new_cpu_task() {
    let (service, gate, trace, started, release) = bypass_fixture(128).await;
    let running = service
        .submit(request_resources(1, 1))
        .await
        .expect("running task accepted");
    tokio::time::timeout(Duration::from_secs(2), started.notified())
        .await
        .expect("first handler did not start");
    let anchor = service.submit(request_resources(0, 1)).await.expect("anchor accepted");
    let first_bypass = service
        .submit(request_resources(1, 0))
        .await
        .expect("first bypass accepted");
    assert_eq!(wait_for_starts(&trace, 2).await, vec![running.id, first_bypass.id]);
    assert_eq!(
        wait_for(&service, first_bypass.id, TaskState::is_terminal).await,
        TaskState::Succeeded
    );

    gate.armed.store(true, Ordering::Release);
    let second_bypass = service
        .submit(request_resources(1, 0))
        .await
        .expect("second bypass accepted");
    complete_gated_scan(&gate).await;
    assert_eq!(trace.ids.lock().expect("start trace lock is not poisoned").len(), 2);
    assert!(matches!(
        service
            .get(second_bypass.id)
            .await
            .expect("task lookup")
            .expect("task exists")
            .state,
        TaskState::Queued
    ));

    release.add_permits(1);
    assert_eq!(
        wait_for_starts(&trace, 4).await,
        vec![running.id, first_bypass.id, anchor.id, second_bypass.id]
    );
    service.shutdown().await.expect("service shutdown");
}

#[tokio::test]
async fn test_resource_bypass_limit_clears_when_anchor_is_cancelled() {
    let (service, gate, trace, started, release) = bypass_fixture(128).await;
    let running = service
        .submit(request_resources(1, 1))
        .await
        .expect("running task accepted");
    tokio::time::timeout(Duration::from_secs(2), started.notified())
        .await
        .expect("first handler did not start");
    let anchor = service.submit(request_resources(0, 1)).await.expect("anchor accepted");
    let first_bypass = service
        .submit(request_resources(1, 0))
        .await
        .expect("first bypass accepted");
    assert_eq!(wait_for_starts(&trace, 2).await, vec![running.id, first_bypass.id]);
    assert_eq!(
        wait_for(&service, first_bypass.id, TaskState::is_terminal).await,
        TaskState::Succeeded
    );

    gate.armed.store(true, Ordering::Release);
    let waiting = service
        .submit(request_resources(1, 0))
        .await
        .expect("waiting task accepted");
    complete_gated_scan(&gate).await;
    assert!(matches!(
        service
            .get(waiting.id)
            .await
            .expect("task lookup")
            .expect("task exists")
            .state,
        TaskState::Queued
    ));

    assert_eq!(
        service.cancel(anchor.id).await.expect("anchor cancellation"),
        CancelOutcome::CancelledBeforeStart
    );
    assert_eq!(
        wait_for_starts(&trace, 3).await,
        vec![running.id, first_bypass.id, waiting.id]
    );
    release.add_permits(1);
    service.shutdown().await.expect("service shutdown");
}

#[tokio::test]
async fn test_resource_bypass_limit_preserves_anchor_across_ready_pages() {
    let (service, gate, trace, started, release) = bypass_fixture(2).await;
    let running = service
        .submit(request_resources(1, 1))
        .await
        .expect("running task accepted");
    tokio::time::timeout(Duration::from_secs(2), started.notified())
        .await
        .expect("first handler did not start");

    gate.armed.store(true, Ordering::Release);
    let impossible_a = service
        .submit(request_resources(0, 2))
        .await
        .expect("first impossible task accepted");
    let impossible_b = service
        .submit(request_resources(0, 2))
        .await
        .expect("second impossible task accepted");
    let anchor = service.submit(request_resources(0, 1)).await.expect("anchor accepted");
    let first_bypass = service
        .submit(request_resources(1, 0))
        .await
        .expect("first bypass accepted");
    complete_gated_scan(&gate).await;
    assert_eq!(wait_for_starts(&trace, 2).await, vec![running.id, first_bypass.id]);
    assert_eq!(
        wait_for(&service, first_bypass.id, TaskState::is_terminal).await,
        TaskState::Succeeded
    );
    assert!(matches!(
        wait_for(&service, impossible_a.id, |state| matches!(
            state,
            TaskState::Blocked { .. }
        ))
        .await,
        TaskState::Blocked { .. }
    ));
    assert!(matches!(
        wait_for(&service, impossible_b.id, |state| matches!(
            state,
            TaskState::Blocked { .. }
        ))
        .await,
        TaskState::Blocked { .. }
    ));

    gate.armed.store(true, Ordering::Release);
    let waiting_a = service
        .submit(request_resources(1, 0))
        .await
        .expect("first waiting task accepted");
    let waiting_b = service
        .submit(request_resources(1, 0))
        .await
        .expect("second waiting task accepted");
    complete_gated_scan(&gate).await;
    assert!(matches!(
        service
            .get(waiting_a.id)
            .await
            .expect("task lookup")
            .expect("task exists")
            .state,
        TaskState::Queued
    ));
    assert!(matches!(
        service
            .get(waiting_b.id)
            .await
            .expect("task lookup")
            .expect("task exists")
            .state,
        TaskState::Queued
    ));

    release.add_permits(1);
    let starts = wait_for_starts(&trace, 4).await;
    assert_eq!(starts[2], anchor.id);
    service.shutdown().await.expect("service shutdown");
}

#[tokio::test]
async fn test_resource_bypass_limit_rescans_anchor_beyond_persistent_first_page() {
    let (service, gate, trace, started, release) = bypass_fixture(2).await;
    let running = service
        .submit(request_resources(1, 1))
        .await
        .expect("running task accepted");
    tokio::time::timeout(Duration::from_secs(2), started.notified())
        .await
        .expect("first handler did not start");

    gate.armed.store(true, Ordering::Release);
    let earlier_a = service
        .submit(request_resources(0, 0))
        .await
        .expect("first earlier task accepted");
    let earlier_b = service
        .submit(request_resources(0, 0))
        .await
        .expect("second earlier task accepted");
    let anchor = service.submit(request_resources(0, 1)).await.expect("anchor accepted");
    let first_bypass = service
        .submit(request_resources(1, 0))
        .await
        .expect("first bypass accepted");
    trace
        .conflicts
        .lock()
        .expect("conflict trace lock is not poisoned")
        .extend([earlier_a.id, earlier_b.id]);
    complete_gated_scan(&gate).await;
    assert_eq!(wait_for_starts(&trace, 2).await, vec![running.id, first_bypass.id]);
    assert_eq!(
        wait_for(&service, first_bypass.id, TaskState::is_terminal).await,
        TaskState::Succeeded
    );
    assert!(matches!(
        service
            .get(earlier_a.id)
            .await
            .expect("task lookup")
            .expect("task exists")
            .state,
        TaskState::Queued
    ));
    let earlier_b_summary = service
        .get(earlier_b.id)
        .await
        .expect("task lookup")
        .expect("task exists");
    assert!(matches!(earlier_b_summary.state, TaskState::Queued));
    let first_page_cursor = TaskCursor::new(earlier_b_summary.accepted_at_ms, earlier_b.id);

    gate.armed.store(true, Ordering::Release);
    let waiting = service
        .submit(request_resources(1, 0))
        .await
        .expect("second bypass accepted");
    let reached = gate.reached.notified();
    tokio::pin!(reached);
    reached.as_mut().enable();
    tokio::time::timeout(Duration::from_secs(2), reached)
        .await
        .expect("second scan did not reach gate");
    trace
        .ready_scans
        .lock()
        .expect("ready scan trace lock is not poisoned")
        .clear();
    let completed = gate.completed.notified();
    tokio::pin!(completed);
    completed.as_mut().enable();
    gate.release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(2), completed)
        .await
        .expect("second scan did not complete");

    let scans = trace
        .ready_scans
        .lock()
        .expect("ready scan trace lock is not poisoned")
        .clone();
    assert_eq!(scans.first(), Some(&None), "every scan must restart at the first page");
    assert_eq!(
        scans.get(1),
        Some(&Some(first_page_cursor)),
        "anchor must remain on a later page"
    );
    assert_eq!(trace.ids.lock().expect("start trace lock is not poisoned").len(), 2);
    assert!(matches!(
        service
            .get(earlier_a.id)
            .await
            .expect("task lookup")
            .expect("task exists")
            .state,
        TaskState::Queued
    ));
    assert!(matches!(
        service
            .get(earlier_b.id)
            .await
            .expect("task lookup")
            .expect("task exists")
            .state,
        TaskState::Queued
    ));
    assert!(matches!(
        service
            .get(anchor.id)
            .await
            .expect("task lookup")
            .expect("task exists")
            .state,
        TaskState::Queued
    ));
    assert!(matches!(
        service
            .get(waiting.id)
            .await
            .expect("task lookup")
            .expect("task exists")
            .state,
        TaskState::Queued
    ));

    release.add_permits(1);
    assert_eq!(
        wait_for_starts(&trace, 4).await,
        vec![running.id, first_bypass.id, anchor.id, waiting.id]
    );
    service.shutdown().await.expect("service shutdown");
}

#[tokio::test]
async fn test_resource_release_during_ready_scan_is_not_lost() {
    let codecs = Arc::new(ValueBytesCodecRegistry::from_registrations([&CODEC_REGISTRATION]).unwrap());
    let started = Arc::new(Notify::new());
    let release_handler = Arc::new(Semaphore::new(0));
    let gate = Arc::new(ReadyScanGate {
        armed: AtomicBool::new(false),
        reached: Notify::new(),
        release: Semaphore::new(0),
        completed: Notify::new(),
    });
    let store = Arc::new(GatedReadyStore {
        inner: Arc::new(MemoryTaskStore::new(8)),
        gate: Arc::clone(&gate),
        start_gate: None,
        trace: None,
    });
    let mut builder = TaskExecutionServiceBuilder::new(store, codecs, Arc::new(Ids(AtomicU64::new(200))))
        .capacity(ResourceCapacity {
            cpu_slots: 1,
            ..ResourceCapacity::default()
        })
        .max_running_tasks(NonZeroUsize::new(2).unwrap());
    builder
        .handlers_mut()
        .register::<Counter, _>(
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
    tokio::time::timeout(Duration::from_secs(2), scan_reached)
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
async fn test_cancel_during_start_cas_releases_reservation_without_running_handler() {
    let codecs = Arc::new(ValueBytesCodecRegistry::from_registrations([&CODEC_REGISTRATION]).unwrap());
    let ready_gate = Arc::new(ReadyScanGate {
        armed: AtomicBool::new(false),
        reached: Notify::new(),
        release: Semaphore::new(0),
        completed: Notify::new(),
    });
    let start_gate = Arc::new(StartCasGate {
        armed: AtomicBool::new(true),
        reached: Notify::new(),
        release: Semaphore::new(0),
    });
    let store = Arc::new(GatedReadyStore {
        inner: Arc::new(MemoryTaskStore::new(8)),
        gate: ready_gate,
        start_gate: Some(Arc::clone(&start_gate)),
        trace: None,
    });
    let started = Arc::new(Notify::new());
    let release_handler = Arc::new(Semaphore::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let mut builder = TaskExecutionServiceBuilder::new(store, codecs, Arc::new(Ids(AtomicU64::new(900)))).capacity(
        ResourceCapacity {
            cpu_slots: 1,
            ..ResourceCapacity::default()
        },
    );
    builder
        .handlers_mut()
        .register::<Counter, _>(
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
    tokio::time::timeout(Duration::from_secs(2), start_reached)
        .await
        .unwrap();

    assert_eq!(
        service.cancel(cancelled.id).await.unwrap(),
        CancelOutcome::CancelledBeforeStart
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
