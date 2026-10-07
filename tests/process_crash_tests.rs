// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Verifies typed SQLite task recovery after killing a child process.
//! Handler side effects may repeat after a crash; this is not exactly-once
//! execution.
#![cfg(feature = "sqlite")]

use std::fs::OpenOptions;
use std::fs::Permissions;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::process::Child;
use std::process::Command;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::OnceLock;
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
use qubit_task::model::TaskCursor;
use qubit_task::model::TaskId;
use qubit_task::model::TaskOutput;
use qubit_task::model::TaskQuery;
use qubit_task::model::TaskRequest;
use qubit_task::model::TaskState;
use qubit_task::store::SqliteTaskStore;
use qubit_task::store::TaskFuture;
use qubit_task::store::TaskStore;
use serde::Deserialize;
use serde_json::Value;
use serde_json::from_str;
use serde_json::from_value;
use tokio::sync::Semaphore;
use tokio::sync::mpsc;
use tokio::task::spawn_blocking;
use tokio::test as tokio_test;
use tokio::time::timeout;

const DEADLINE: Duration = Duration::from_secs(30);
const BULK_FIXTURE_DEADLINE: Duration = Duration::from_secs(120);
const PAYLOAD_TYPE_ID: &str = "fixture.CrashWorkerPayload";
const CODEC_ID: &str = "fixture.crash_worker.json";

struct CrashPayload(Value);

impl HasModelId for CrashPayload {
    const MODEL_ID: ModelId = ModelId::new(PAYLOAD_TYPE_ID);
}
static WORKER: OnceLock<WorkerFixture> = OnceLock::new();
static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(10_000);

#[derive(Default)]
struct JsonValueCodec;

impl qubit_codec::ValueEncoder<CrashPayload> for JsonValueCodec {
    type Output = Vec<u8>;
    type Error = serde_json::Error;

    fn encode(&mut self, value: &CrashPayload) -> Result<Self::Output, Self::Error> {
        serde_json::to_vec(&value.0)
    }
}

impl qubit_codec::ValueDecoder<[u8]> for JsonValueCodec {
    type Output = CrashPayload;
    type Error = serde_json::Error;

    fn decode(&mut self, bytes: &[u8]) -> Result<Self::Output, Self::Error> {
        serde_json::from_slice(bytes).map(CrashPayload)
    }
}

static JSON_DESCRIPTOR: ValueBytesCodecDescriptor =
    ValueBytesCodecDescriptor::of::<JsonValueCodec, CrashPayload>();
static JSON_CODEC: ValueBytesCodecRegistration = ValueCodecRegistration::new(
    ValueCodecId::new(CODEC_ID),
    &JSON_DESCRIPTOR,
    ValueCodecRegistrationSource::new("qubit-task", "crash-worker", "fixture", 1),
);

fn new_task_id() -> TaskId {
    TaskId::from_id(Id::new(NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed)))
}

fn codec_registry() -> Arc<ValueBytesCodecRegistry> {
    Arc::new(
        ValueBytesCodecRegistry::from_registrations([&JSON_CODEC])
            .expect("fixture codec registers"),
    )
}

/// Caches compiled bytes, rather than a static directory whose destructor
/// would never run. Every on-disk workspace has a local cleanup owner.
struct WorkerFixture {
    bytes: Vec<u8>,
    permissions: Permissions,
}

/// Holds one executable's local workspace until its child is killed/reaped.
struct WorkerInstance {
    path: PathBuf,
    _workspace: Database,
}

/// Selects output inside the owned temporary workspace, ignoring Cargo target
/// paths.
fn fixture_target_path(workspace: &Path) -> PathBuf {
    workspace.join("fixture-target")
}

/// Builds the standalone typed worker once and keeps a private executable per
/// case.
fn build_worker() -> WorkerInstance {
    let fixture = WORKER.get_or_init(|| {
        let repository = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let workspace = Database::new();
        let target = fixture_target_path(&workspace.0);
        let output = Command::new(env!("CARGO"))
            .current_dir(&repository)
            .args(["build", "--locked", "--manifest-path"])
            .arg(repository.join("tests/fixtures/crash-worker/Cargo.toml"))
            .arg("--target-dir")
            .arg(&target)
            .output()
            .expect("fixture compiler starts");
        assert!(
            output.status.success(),
            "fixture build failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let executable = target.join("debug").join(format!(
            "rs-task-crash-worker-fixture{}",
            std::env::consts::EXE_SUFFIX
        ));
        let fixture = WorkerFixture {
            bytes: std::fs::read(&executable).expect("compiled worker bytes are retained"),
            permissions: std::fs::metadata(&executable)
                .expect("worker permissions read")
                .permissions(),
        };
        drop(workspace);
        assert!(
            !target.exists(),
            "temporary compiler output is removed after retaining the executable"
        );
        fixture
    });

    let workspace = Database::new();
    let path = workspace
        .0
        .join(format!("worker{}", std::env::consts::EXE_SUFFIX));
    let temporary_path = workspace
        .0
        .join(format!("worker.tmp{}", std::env::consts::EXE_SUFFIX));
    let mut temporary_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary_path)
        .expect("private worker temporary file is created");
    temporary_file
        .write_all(&fixture.bytes)
        .expect("private worker executable bytes are written");
    temporary_file
        .set_permissions(fixture.permissions.clone())
        .expect("worker executable permissions are restored");
    temporary_file
        .sync_all()
        .expect("worker executable is flushed");
    drop(temporary_file);
    std::fs::rename(&temporary_path, &path)
        .expect("complete worker executable is published atomically");
    WorkerInstance {
        path,
        _workspace: workspace,
    }
}

/// Owns only this test's child, killing and reaping it even during a panic.
struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if matches!(self.0.try_wait(), Ok(None)) {
            let _ = self.0.kill();
        }
        let _ = self.0.wait();
    }
}

/// Owns a disposable namespace and never accepts a caller's database path.
struct Database(PathBuf);

impl Database {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("qubit-task-process-crash-{}", new_task_id()));
        std::fs::create_dir(&path).expect("disposable database directory is created");
        Self(path)
    }

    fn path(&self) -> PathBuf {
        self.0.join("tasks.sqlite")
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Ready {
    event: String,
    mode: String,
    task_id: TaskId,
    attempt: u32,
}

/// Waits for a committed-state handshake, then kills and reaps the child.
async fn crash_worker(database: &Database, mode: &str, id: TaskId, count: usize) {
    let worker = spawn_blocking(build_worker)
        .await
        .expect("fixture build task finishes");
    let mut command = Command::new(&worker.path);
    command
        .arg(database.path())
        .arg(mode)
        .arg(id.to_string())
        .arg(count.to_string())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    let mut etxtbsy_attempts = 0_u32;
    let child = loop {
        match command.spawn() {
            Ok(child) => break child,
            Err(error) if error.kind() == std::io::ErrorKind::ExecutableFileBusy => {
                if etxtbsy_attempts >= 5 {
                    panic!(
                        "crash worker remains busy at {}: {error}",
                        worker.path.display()
                    );
                }
                tokio::time::sleep(Duration::from_millis(1 << etxtbsy_attempts)).await;
                etxtbsy_attempts += 1;
            }
            Err(error) => panic!("crash worker starts at {}: {error}", worker.path.display()),
        }
    };
    let mut child = ChildGuard(child);
    let output = child.0.stdout.take().expect("protocol stdout is piped");
    let reader = spawn_blocking(move || {
        for line in BufReader::new(output).lines() {
            let line = line.expect("protocol line reads");
            let value: Value = from_str(&line).expect("stdout contains JSON protocol only");
            if value.get("event").and_then(Value::as_str) == Some("ready") {
                return from_value::<Ready>(value).expect("READY schema is valid");
            }
        }
        panic!("worker closed stdout before committed READY");
    });
    let deadline = if count > 1 {
        BULK_FIXTURE_DEADLINE
    } else {
        DEADLINE
    };
    let ready = timeout(deadline, reader)
        .await
        .expect("committed READY arrives before deadline")
        .expect("protocol reader finishes");
    assert_eq!(ready.event, "ready");
    assert_eq!(ready.mode, mode);
    assert_eq!(ready.task_id, id);
    assert_eq!(ready.attempt, u32::from(mode != "queued"));
    assert!(
        child.0.try_wait().expect("worker liveness reads").is_none(),
        "worker must remain alive after READY"
    );
    child.0.kill().expect("only this child is killed");
    let status = child.0.wait().expect("killed child is reaped");
    assert!(!status.success(), "child did not shut down gracefully");
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(
            status.signal(),
            Some(9),
            "Child::kill terminates the worker with SIGKILL"
        );
    }
    drop(child);
    let executable_workspace = worker._workspace.0.clone();
    drop(worker);
    assert!(
        !executable_workspace.exists(),
        "private executable is removed after its child exits"
    );
}

fn request(id: TaskId) -> TaskRequest<CrashPayload> {
    let mut request = TaskRequest::new(
        "crash-worker",
        1,
        ValueCodecId::new(CODEC_ID),
        CrashPayload(serde_json::json!({"payload": "durable"})),
    );
    request.idempotency_key = Some(id.to_string());
    request
}

struct RestartHandler {
    starts: mpsc::UnboundedSender<TaskId>,
    count: Arc<AtomicUsize>,
    gate: Arc<Semaphore>,
}

impl TaskHandler<CrashPayload> for RestartHandler {
    fn run<'a>(
        &'a self,
        _payload: CrashPayload,
        context: TaskContext,
    ) -> TaskFuture<'a, TaskRunResult> {
        Box::pin(async move {
            self.count.fetch_add(1, Ordering::SeqCst);
            self.starts
                .send(context.task_id())
                .expect("restart observer remains open");
            self.gate
                .acquire()
                .await
                .expect("restart gate remains open")
                .forget();
            Ok(TaskRunOutcome::Succeeded(TaskOutput {
                summary: b"completed".to_vec(),
            }))
        })
    }
}

struct TestServiceIds(AtomicU64);

impl IdGenerator<Id, IdGenerationError> for TestServiceIds {
    fn generate(&self) -> Result<Id, IdGenerationError> {
        Ok(Id::new(self.0.fetch_add(1, Ordering::Relaxed)))
    }
}

fn service_builder(
    store: Arc<SqliteTaskStore>,
    starts: Option<mpsc::UnboundedSender<TaskId>>,
    count: Arc<AtomicUsize>,
    gate: Arc<Semaphore>,
) -> TaskExecutionServiceBuilder {
    let mut builder = TaskExecutionServiceBuilder::new(
        store as Arc<dyn TaskStore>,
        codec_registry(),
        Arc::new(TestServiceIds(AtomicU64::new(9_000_000))),
    );
    if let Some(starts) = starts {
        builder
            .handlers_mut()
            .register::<CrashPayload, _>(
                TaskHandlerDescriptor {
                    kind_id: "crash-worker".into(),
                    payload_type_id: ModelIdBuf::try_from(PAYLOAD_TYPE_ID)
                        .expect("payload model ID is valid"),
                    accepted_schema_versions: vec![1],
                    cancellation_mode: CancellationMode::Unsupported,
                },
                Arc::new(RestartHandler {
                    starts,
                    count,
                    gate,
                }),
            )
            .expect("restart handler registers");
    }
    builder
}

async fn wait_for_state(service: &TaskExecutionService, id: TaskId, state: TaskState) {
    timeout(DEADLINE, async {
        loop {
            if service
                .get(id)
                .await
                .expect("task summary reads")
                .is_some_and(|summary| summary.state == state)
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("task reaches expected state before deadline");
}

async fn wait_for_terminal(
    service: &TaskExecutionService,
    id: TaskId,
) -> qubit_task::model::TaskSummary {
    timeout(DEADLINE, async {
        loop {
            let summary = service
                .get(id)
                .await
                .expect("task summary reads")
                .expect("task remains retained");
            if summary.state.is_terminal() {
                return summary;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("task reaches a terminal state before deadline")
}

async fn assert_recovery(mode: &str) {
    let database = Database::new();
    let id = new_task_id();
    crash_worker(&database, mode, id, 1).await;

    let store = Arc::new(
        SqliteTaskStore::open_next(database.path()).expect("killed owner's database reopens"),
    );
    let before = store
        .get_encoded_task(id)
        .await
        .expect("committed task reads")
        .expect("committed task survives kill");
    assert_eq!(
        before.request,
        request(id)
            .encode(&codec_registry())
            .expect("fixture request encodes")
    );
    assert_eq!(before.summary.attempt, u32::from(mode != "queued"));
    let expected_state = match mode {
        "queued" => TaskState::Queued,
        "running" => TaskState::Running,
        "terminal" => TaskState::Succeeded,
        _ => unreachable!("test passes a known crash mode"),
    };
    assert_eq!(before.summary.state, expected_state);
    if mode == "terminal" {
        assert_eq!(
            before
                .summary
                .output
                .as_ref()
                .map(|output| output.summary.as_slice()),
            Some(&b"completed"[..])
        );
    }
    drop(store);

    let (starts, mut observed) = mpsc::unbounded_channel();
    let count = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(Semaphore::new(0));
    let store =
        Arc::new(SqliteTaskStore::open_next(database.path()).expect("recovery store opens"));
    let service = service_builder(
        Arc::clone(&store),
        Some(starts),
        Arc::clone(&count),
        Arc::clone(&gate),
    )
    .build()
    .await
    .expect("replacement service recovers");

    if mode == "terminal" {
        let recovered = service
            .get(id)
            .await
            .expect("terminal summary reads")
            .expect("terminal task remains retained");
        assert_eq!(
            recovered, before.summary,
            "terminal lifecycle remains unchanged"
        );
        assert_eq!(
            count.load(Ordering::SeqCst),
            0,
            "terminal work never restarts"
        );
        service
            .shutdown()
            .await
            .expect("replacement service closes");
        return;
    }

    assert_eq!(observed.recv().await.expect("recovered handler starts"), id);
    let running = service
        .get(id)
        .await
        .expect("running summary reads")
        .expect("running task exists");
    assert_eq!(running.state, TaskState::Running);
    assert_eq!(running.attempt, if mode == "running" { 2 } else { 1 });
    let duplicate = service
        .submit(request(id))
        .await
        .expect("idempotent resubmission succeeds");
    assert_eq!(
        duplicate.id, id,
        "restart does not accept the same work twice"
    );
    let history = service
        .query(TaskQuery {
            limit: 16,
            ..TaskQuery::default()
        })
        .await
        .expect("history reads");
    assert_eq!(
        history.records.len(),
        1,
        "idempotent resubmission adds no history row"
    );
    gate.add_permits(1);
    let finished = wait_for_terminal(&service, id).await;
    assert_eq!(finished.state, TaskState::Succeeded);
    assert_eq!(
        finished
            .output
            .as_ref()
            .map(|output| output.summary.as_slice()),
        Some(&b"completed"[..])
    );
    service
        .shutdown()
        .await
        .expect("replacement service closes");
    assert_eq!(count.load(Ordering::SeqCst), 1);
}

/// Committed acceptance is retained before any handler starts and later
/// recovers.
#[tokio_test]
async fn test_killed_queued_worker_recovers_acceptance() {
    assert_recovery("queued").await;
}

/// An interrupted running attempt is counted when the next attempt starts.
#[tokio_test]
async fn test_killed_running_worker_recovers_next_attempt() {
    assert_recovery("running").await;
}

/// A committed terminal result never starts its handler again.
#[tokio_test]
async fn test_killed_terminal_worker_preserves_completion() {
    assert_recovery("terminal").await;
}

/// A crashed running task is blocked when the replacement has no matching
/// handler.
#[tokio_test]
async fn test_killed_running_worker_blocks_without_matching_handler() {
    let database = Database::new();
    let id = new_task_id();
    crash_worker(&database, "running", id, 1).await;
    let store =
        Arc::new(SqliteTaskStore::open_next(database.path()).expect("recovery store opens"));
    let service = service_builder(
        Arc::clone(&store),
        None,
        Arc::new(AtomicUsize::new(0)),
        Arc::new(Semaphore::new(0)),
    )
    .build()
    .await
    .expect("replacement service scans unfinished work");
    wait_for_state(
        &service,
        id,
        TaskState::Blocked {
            reason: "handler is not registered".into(),
        },
    )
    .await;
    let blocked = service
        .get(id)
        .await
        .expect("blocked summary reads")
        .expect("task remains retained");
    assert_eq!(
        blocked.attempt, 1,
        "blocked recovery does not begin another attempt"
    );
    service
        .shutdown()
        .await
        .expect("replacement service closes");
}

/// Crash recovery retains every committed row across the 256-row page boundary.
#[tokio_test]
async fn test_killed_worker_preserves_513_committed_tasks_across_pages() {
    let database = Database::new();
    let id = new_task_id();
    crash_worker(&database, "queued", id, 513).await;
    let store = SqliteTaskStore::open_next(database.path()).expect("multi-page database reopens");
    let mut cursor = None;
    let mut ids = std::collections::BTreeSet::new();
    for expected in [256, 256, 1] {
        let page = store
            .list_encoded(TaskQuery {
                after: cursor,
                limit: 256,
                ..TaskQuery::default()
            })
            .await
            .expect("committed recovery page reads");
        assert_eq!(page.records.len(), expected);
        for summary in &page.records {
            assert!(cursor.is_none_or(|previous| TaskCursor::from(summary) > previous));
            assert!(ids.insert(summary.id), "no task repeats across pages");
            let stored = store
                .get_encoded_task(summary.id)
                .await
                .expect("committed payload reads")
                .expect("committed row exists");
            assert_eq!(
                stored.request,
                request(summary.id)
                    .encode(&codec_registry())
                    .expect("request encodes")
            );
            assert_eq!(summary.state, TaskState::Queued);
            assert_eq!(summary.attempt, 0);
            assert_eq!(summary.kind_id, "crash-worker");
            assert_eq!(summary.payload_type_id, PAYLOAD_TYPE_ID);
            assert_eq!(summary.payload_schema_version, 1);
            assert_eq!(summary.payload_codec_id, CODEC_ID);
            assert_eq!(
                summary.idempotency_key.as_deref(),
                Some(summary.id.to_string().as_str()),
                "idempotency key survives process death"
            );
        }
        assert_eq!(
            page.next,
            if expected == 1 {
                None
            } else {
                page.records.last().map(TaskCursor::from)
            }
        );
        cursor = page.next;
    }
    assert_eq!(ids.len(), 513);
    assert!(ids.contains(&id));
}
