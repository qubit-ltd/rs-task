// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! A process-owned SQLite fixture that stays alive after committed-state READY.

use std::io;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::handler::TaskContext;
use qubit_task::handler::TaskHandler;
use qubit_task::handler::TaskHandlerDescriptor;
use qubit_task::handler::TaskRunOutcome;
use qubit_task::handler::TaskRunResult;
use qubit_task::model::AcceptOutcome;
use qubit_task::model::TaskId;
use qubit_task::model::TaskOutput;
use qubit_task::model::TaskRequest;
use qubit_task::model::TaskState;
use qubit_task::store::SqliteTaskStore;
use qubit_task::store::TaskFuture;
use qubit_task::store::TaskStore;
use serde_json::Value;
use serde_json::from_value;
use serde_json::json;
use tokio::main as tokio_main;
use tokio::sync::Semaphore;
use tokio::sync::mpsc;

/// Waits on an explicit gate so the parent can kill a genuinely running task.
struct WorkerHandler {
    gate: Arc<Semaphore>,
    started: mpsc::UnboundedSender<TaskId>,
}

impl TaskHandler for WorkerHandler {
    fn descriptor(&self) -> TaskHandlerDescriptor {
        TaskHandlerDescriptor {
            task_type: "crash-worker".into(),
            version: "1".into(),
        }
    }

    fn run<'a>(&'a self, _payload: &'a [u8], context: TaskContext) -> TaskFuture<'a, TaskRunResult> {
        Box::pin(async move {
            self.started
                .send(context.task_id())
                .expect("worker observer remains open");
            self.gate.acquire().await.expect("worker gate remains open").forget();
            Ok(TaskRunOutcome::Succeeded(TaskOutput {
                summary: b"completed".to_vec(),
            }))
        })
    }
}

/// Writes and flushes one machine-readable state acknowledgement.
/// I/O errors propagate; this is called only after the corresponding store
/// write returned.
fn signal_ready(mode: &str, id: TaskId, attempt: u32) -> io::Result<()> {
    let message = json!({ "event": "ready", "mode": mode, "task_id": id.to_string(), "attempt": attempt });
    let stdout = io::stdout();
    let mut output = stdout.lock();
    writeln!(output, "{message}")?;
    output.flush()
}

/// Parses the absolute database path, mode, task ID, and optional queued row
/// count. The worker refuses an existing database, leaving all database
/// creation to this child.
fn arguments() -> Result<(PathBuf, String, TaskId, usize), Box<dyn std::error::Error>> {
    let mut arguments = std::env::args().skip(1);
    let path = PathBuf::from(arguments.next().ok_or("missing absolute database path")?);
    let mode = arguments.next().ok_or("missing queued|running|terminal mode")?;
    let id_text = arguments.next().ok_or("missing task ID")?;
    let id = from_value(Value::String(id_text))?;
    let count = arguments.next().map_or(Ok(1), |value| value.parse::<usize>())?;
    if !path.is_absolute()
        || path.exists()
        || !matches!(mode.as_str(), "queued" | "running" | "terminal")
        || count == 0
        || count > 513
        || (mode != "queued" && count != 1)
        || arguments.next().is_some()
    {
        return Err("expected a new absolute database path, valid mode, UUID, and 1..=513 queued rows".into());
    }
    Ok((path, mode, id, count))
}

/// Creates committed work and emits READY without invoking service shutdown.
/// All diagnostics are errors on stderr; stdout is reserved for protocol JSON.
#[tokio_main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (path, mode, id, count) = arguments()?;
    let store = Arc::new(SqliteTaskStore::open(&path)?);
    let owner = store.acquire_owner().await?;
    for index in 0..count {
        let task_id = if index == 0 { id } else { TaskId::generate() };
        let request = TaskRequest::new("crash-worker", "1", b"durable-payload".to_vec())
            .with_idempotency_key(task_id.to_string());
        if !matches!(store.accept(task_id, request).await?, AcceptOutcome::Accepted(_)) {
            return Err("fixture task unexpectedly already existed".into());
        }
    }
    if mode == "queued" {
        let record = store.get_summary(id).await?.ok_or("committed acceptance disappeared")?;
        if record.state != TaskState::Queued || record.attempt != 0 {
            return Err("queued READY does not match persisted state".into());
        }
        signal_ready(&mode, id, record.attempt)?;
        std::future::pending::<()>().await;
    }

    // The initial store owner is released before the service takes ownership.
    // The service and its handler remain alive after READY until the parent kills
    // us.
    store.release_owner(owner).await?;
    drop(store);
    let store = Arc::new(SqliteTaskStore::open(&path)?);
    let gate = Arc::new(Semaphore::new(usize::from(mode == "terminal")));
    let (started, mut starts) = mpsc::unbounded_channel();
    let service = TaskExecutionServiceBuilder::in_memory()
        .store(Arc::clone(&store) as Arc<dyn TaskStore>)
        .require_recovery(true)
        .register_handler(Arc::new(WorkerHandler { gate, started }))?
        .build()
        .await?;
    if starts.recv().await != Some(id) {
        return Err("worker handler did not start the accepted task".into());
    }
    if mode == "terminal" {
        service.wait(id).await?;
    }
    let record = store.get_summary(id).await?.ok_or("committed execution disappeared")?;
    let expected = if mode == "running" {
        TaskState::Running
    } else {
        TaskState::Succeeded
    };
    if record.state != expected || record.attempt != 1 {
        return Err("execution READY does not match persisted state".into());
    }
    signal_ready(&mode, id, record.attempt)?;
    std::future::pending::<()>().await;
    Ok(())
}
