// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Committed SQLite state survives an actual child-process kill.
//! Handler side effects may repeat after a crash; this is not exactly-once
//! execution.
#![cfg(feature = "sqlite")]

use std::io::BufRead;
use std::io::BufReader;
use std::path::Path;
use std::path::PathBuf;
use std::process::Child;
use std::process::Command;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::handler::TaskContext;
use qubit_task::handler::TaskHandler;
use qubit_task::handler::TaskHandlerDescriptor;
use qubit_task::handler::TaskRunOutcome;
use qubit_task::handler::TaskRunResult;
use qubit_task::model::AcceptOutcome;
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
static WORKER: OnceLock<PathBuf> = OnceLock::new();

/// Builds the independent fixture once, outside the repository target
/// directory. Panics with compiler diagnostics if the required fixture cannot
/// be built.
fn build_worker() -> PathBuf {
    WORKER
        .get_or_init(|| {
            let repository = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            let workspace = std::env::var_os("CARGO_TARGET_DIR")
                .map(PathBuf::from)
                .and_then(|path| path.parent().map(Path::to_path_buf))
                .unwrap_or_else(|| std::env::temp_dir().join(format!("qubit-task-crash-build-{}", TaskId::generate())));
            let target = workspace.join("fixture-target");
            let mut command = Command::new(env!("CARGO"));
            command
                .current_dir(&repository)
                .args(["build", "--locked", "--manifest-path"])
                .arg(repository.join("tests/fixtures/crash-worker/Cargo.toml"))
                .arg("--target-dir")
                .arg(&target);
            // This independent workspace enables only sqlite, so event-bus
            // siblings and the parent crate's dev-dependencies are unnecessary.
            let output = command.output().expect("fixture compiler starts");
            assert!(
                output.status.success(),
                "fixture build failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            target
                .join("debug")
                .join(format!("rs-task-crash-worker-fixture{}", std::env::consts::EXE_SUFFIX))
        })
        .clone()
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

/// Owns a new disposable namespace; never accepts a caller's database path.
struct Database(PathBuf);

impl Database {
    /// Creates a fresh directory for one case, panicking on filesystem errors.
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("qubit-task-process-crash-{}", TaskId::generate()));
        std::fs::create_dir(&path).expect("disposable database directory is created");
        Self(path)
    }

    /// Returns the child's absolute database path inside the owned namespace.
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

/// Waits for a committed-state JSON handshake, then kills and reaps the child.
/// Blocking stdout reads run on the blocking pool; timeout/panic triggers the
/// guard.
async fn crash_worker(database: &Database, mode: &str, id: TaskId, count: usize) {
    let worker = spawn_blocking(build_worker).await.expect("fixture build task finishes");
    let child = Command::new(worker)
        .arg(database.path())
        .arg(mode)
        .arg(id.to_string())
        .arg(count.to_string())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("crash worker starts");
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
    let ready = timeout(DEADLINE, reader)
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
}

/// Uses stable payload and idempotency data shared with the worker fixture.
fn request(id: TaskId) -> TaskRequest {
    TaskRequest::new("crash-worker", "1", b"durable-payload".to_vec()).with_idempotency_key(id.to_string())
}

/// Reports handler starts and waits for explicit completion permits.
struct RestartHandler {
    starts: mpsc::UnboundedSender<TaskId>,
    count: Arc<AtomicUsize>,
    gate: Arc<Semaphore>,
}

impl TaskHandler for RestartHandler {
    fn descriptor(&self) -> TaskHandlerDescriptor {
        TaskHandlerDescriptor {
            task_type: "crash-worker".into(),
            version: "1".into(),
        }
    }

    fn run<'a>(&'a self, _payload: &'a [u8], context: TaskContext) -> TaskFuture<'a, TaskRunResult> {
        Box::pin(async move {
            self.count.fetch_add(1, Ordering::SeqCst);
            self.starts
                .send(context.task_id())
                .expect("restart observer remains open");
            self.gate.acquire().await.expect("restart gate remains open").forget();
            Ok(TaskRunOutcome::Succeeded(TaskOutput {
                summary: b"completed".to_vec(),
            }))
        })
    }
}

/// Reopens after kill, checks identity/payload, and observes recovery via a
/// gated handler. The outer deadline bounds recovery and graceful cleanup of
/// the replacement service.
async fn assert_recovery(mode: &str, max_attempts: u32) {
    let database = Database::new();
    let id = TaskId::generate();
    crash_worker(&database, mode, id, 1).await;
    timeout(DEADLINE, async {
        let store = Arc::new(SqliteTaskStore::open(database.path()).expect("killed owner's database reopens"));
        let owner = store.acquire_owner().await.expect("process death releases owner lock");
        let before = store.get(id).await.expect("committed task reads").expect("committed task survives kill");
        assert_eq!(before.request, request(id));
        assert_eq!(before.attempt, u32::from(mode != "queued"));
        assert_eq!(before.state, match mode { "queued" => TaskState::Queued, "running" => TaskState::Running, _ => TaskState::Succeeded });
        assert_eq!(store.get_by_idempotency_key(&id.to_string()).await.expect("idempotency lookup works").expect("key survives").id, id);
        assert!(matches!(store.accept(id, request(id)).await.expect("duplicate request checks"), AcceptOutcome::Existing(record) if record.id == id));
        store.release_owner(owner).await.expect("inspection owner releases");
        drop(store);

        let (starts, mut observed) = mpsc::unbounded_channel();
        let count = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new(Semaphore::new(0));
        let service = TaskExecutionServiceBuilder::recoverable_sqlite(database.path()).expect("restart builder opens")
            .max_attempts(max_attempts)
            .register_handler(Arc::new(RestartHandler { starts, count: Arc::clone(&count), gate: Arc::clone(&gate) }))
            .expect("restart handler registers").build().await.expect("replacement service recovers");
        if mode == "terminal" || (mode == "running" && max_attempts == 1) {
            let record = service.get_summary(id).await.expect("recovered summary reads").expect("task remains retained");
            if mode == "terminal" {
                assert_eq!(record, before.summary(), "terminal lifecycle remains unchanged");
                assert_eq!(service.wait(id).await.expect("terminal wait returns").output, before.output);
            } else {
                assert!(matches!(record.state, TaskState::Blocked { ref reason } if reason.contains("attempt")));
                assert_eq!(record.attempt, 1);
            }
            service.shutdown().await.expect("replacement service closes");
            assert_eq!(count.load(Ordering::SeqCst), 0, "terminal or exhausted work never restarts");
        } else {
            assert_eq!(observed.recv().await.expect("recovered handler starts"), id);
            let running = service.get_summary(id).await.expect("running summary reads").expect("running task exists");
            assert_eq!(running.state, TaskState::Running);
            assert_eq!(running.attempt, if mode == "running" { 2 } else { 1 });
            let duplicate = service.submit(request(id)).await.expect("idempotent resubmission succeeds");
            assert_eq!(duplicate.id, id, "restart does not accept the same work twice");
            let history = service.list(TaskQuery::default()).await.expect("deduplicated history reads");
            assert_eq!(history.records.len(), 1, "idempotent resubmission adds no history row");
            gate.add_permits(1);
            let finished = service.wait(id).await.expect("recovered task finishes");
            assert_eq!(finished.state, TaskState::Succeeded);
            assert_eq!(finished.request, before.summary().request);
            service.shutdown().await.expect("replacement service closes");
            assert_eq!(count.load(Ordering::SeqCst), 1);
        }
    }).await.expect("recovery and shutdown finish before deadline");
}

/// Committed acceptance is retained before any handler starts.
#[tokio_test]
async fn test_killed_queued_worker_recovers_acceptance() {
    assert_recovery("queued", 3).await;
}

/// An interrupted running attempt is counted when the next attempt starts.
#[tokio_test]
async fn test_killed_running_worker_recovers_next_attempt() {
    assert_recovery("running", 3).await;
}

/// A committed terminal result never starts its handler again.
#[tokio_test]
async fn test_killed_terminal_worker_preserves_completion() {
    assert_recovery("terminal", 3).await;
}

/// A killed attempt that spent its budget is blocked during recovery.
#[tokio_test]
async fn test_killed_running_worker_with_exhausted_attempts_is_blocked() {
    assert_recovery("running", 1).await;
}

/// Crash recovery retains every committed row across the 256-row page boundary.
#[tokio_test]
async fn test_killed_worker_preserves_513_committed_tasks_across_pages() {
    let database = Database::new();
    let id = TaskId::generate();
    crash_worker(&database, "queued", id, 513).await;
    timeout(DEADLINE, async {
        let store = SqliteTaskStore::open(database.path()).expect("multi-page database reopens");
        let owner = store.acquire_owner().await.expect("multi-page owner is acquired");
        let mut cursor = None;
        let mut ids = std::collections::BTreeSet::new();
        for expected in [256, 256, 1] {
            let page = store
                .scan_unfinished(cursor)
                .await
                .expect("committed recovery page reads");
            assert_eq!(page.tasks.len(), expected);
            for task in &page.tasks {
                let key = TaskCursor::from(task);
                assert!(cursor.is_none_or(|previous| key > previous));
                assert!(ids.insert(task.id), "no task is repeated across pages");
                let record = store
                    .get(task.id)
                    .await
                    .expect("committed payload reads")
                    .expect("committed row exists");
                assert_eq!(record.request, request(task.id));
                assert_eq!(record.state, TaskState::Queued);
                assert_eq!(record.attempt, 0);
                assert_eq!(
                    store
                        .get_by_idempotency_key(&task.id.to_string())
                        .await
                        .expect("retained key reads")
                        .expect("key exists")
                        .id,
                    task.id
                );
            }
            assert_eq!(
                page.next,
                if expected == 1 {
                    None
                } else {
                    page.tasks.last().map(TaskCursor::from)
                }
            );
            cursor = page.next;
        }
        assert_eq!(ids.len(), 513);
        assert!(ids.contains(&id));
        store.release_owner(owner).await.expect("multi-page owner releases");
    })
    .await
    .expect("multi-page inspection finishes before deadline");
}
