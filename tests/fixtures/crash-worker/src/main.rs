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
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

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
use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::handler::TaskRunOutcome;
use qubit_task::handler::TaskRunResult;
use qubit_task::model::TaskId;
use qubit_task::model::TaskOutput;
use qubit_task::model::TaskRequest;
use qubit_task::model::TaskState;
use qubit_task::store::SqliteTaskStore;
use qubit_task::store::TaskFuture;
use qubit_task::store::TaskStore;
use qubit_task::TaskHandler;
use qubit_task::TaskContext;
use qubit_task::TaskHandlerDescriptor;
use qubit_task::CancellationMode;
use serde_json::Value;
use serde_json::from_value;
use serde_json::json;
use tokio::main as tokio_main;
use tokio::sync::Semaphore;
use tokio::sync::mpsc;

struct WorkerPayload(serde_json::Value);

impl HasModelId for WorkerPayload {
    const MODEL_ID: ModelId = ModelId::new("fixture.CrashWorkerPayload");
}

/// Waits on an explicit gate so the parent can kill a genuinely running task.
struct WorkerHandler {
    gate: Arc<Semaphore>,
    started: mpsc::UnboundedSender<TaskId>,
}

impl TaskHandler<WorkerPayload> for WorkerHandler {
    fn run<'a>(&'a self, _payload: WorkerPayload, context: TaskContext) -> TaskFuture<'a, TaskRunResult> {
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

#[derive(Default)]
struct JsonValueCodec;

impl qubit_codec::ValueEncoder<WorkerPayload> for JsonValueCodec {
    type Output = Vec<u8>;
    type Error = serde_json::Error;

    fn encode(&mut self, value: &WorkerPayload) -> Result<Vec<u8>, Self::Error> {
        serde_json::to_vec(&value.0)
    }
}

impl qubit_codec::ValueDecoder<[u8]> for JsonValueCodec {
    type Output = WorkerPayload;
    type Error = serde_json::Error;

    fn decode(&mut self, bytes: &[u8]) -> Result<Self::Output, Self::Error> {
        serde_json::from_slice(bytes).map(WorkerPayload)
    }
}

static JSON_DESCRIPTOR: ValueBytesCodecDescriptor = ValueBytesCodecDescriptor::of::<JsonValueCodec, WorkerPayload>();
static JSON_CODEC: ValueBytesCodecRegistration = ValueCodecRegistration::new(
    ValueCodecId::new("fixture.crash_worker.json"),
    &JSON_DESCRIPTOR,
    ValueCodecRegistrationSource::new("qubit-task", "crash-worker", "fixture", 1),
);

struct WorkerIds(AtomicU64);

impl IdGenerator<Id, IdGenerationError> for WorkerIds {
    fn generate(&self) -> Result<Id, IdGenerationError> {
        Ok(Id::new(self.0.fetch_add(1, Ordering::Relaxed)))
    }
}

fn request(idempotency_key: String) -> TaskRequest<WorkerPayload> {
    let mut request = TaskRequest::new(
        "crash-worker",
        1,
        ValueCodecId::new("fixture.crash_worker.json"),
        WorkerPayload(serde_json::json!({"payload": "durable"})),
    );
    request.idempotency_key = Some(idempotency_key);
    request
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
    let codecs = Arc::new(ValueBytesCodecRegistry::from_registrations([&JSON_CODEC])?);
    let store = Arc::new(SqliteTaskStore::open_next(&path)?);
    let owner = store.acquire_owner().await?;
    for index in 0..count {
        let task_id = if index == 0 {
            id
        } else {
            TaskId::from_id(Id::new(1_000_000 + u64::try_from(index)?))
        };
        let encoded = request(task_id.to_string()).encode(&codecs)?;
        if !store.accept_encoded(task_id, encoded).await?.created {
            return Err("fixture task unexpectedly already existed".into());
        }
    }
    if mode == "queued" {
        let record = store
            .get_encoded_task(id)
            .await?
            .ok_or("committed acceptance disappeared")?
            .summary;
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
    let store = Arc::new(SqliteTaskStore::open_next(&path)?);
    let gate = Arc::new(Semaphore::new(usize::from(mode == "terminal")));
    let (started, mut starts) = mpsc::unbounded_channel();
    let mut builder = TaskExecutionServiceBuilder::new(
        Arc::clone(&store) as Arc<dyn TaskStore>,
        codecs,
        Arc::new(WorkerIds(AtomicU64::new(1_000_000))),
    );
    builder.handlers_mut().register::<WorkerPayload, _>(
        TaskHandlerDescriptor {
            kind_id: "crash-worker".into(),
            payload_type_id: ModelIdBuf::try_from("fixture.CrashWorkerPayload")?,
            accepted_schema_versions: vec![1],
            cancellation_mode: CancellationMode::Unsupported,
        },
        Arc::new(WorkerHandler { gate, started }),
    )?;
    let service = builder.build().await?;
    if starts.recv().await != Some(id) {
        return Err("worker handler did not start the accepted task".into());
    }
    if mode == "terminal" {
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                if service.get(id).await?.is_some_and(|summary| summary.state.is_terminal()) {
                    return Ok::<(), qubit_task::service::TaskServiceError>(());
                }
                tokio::task::yield_now().await;
            }
        })
        .await??;
    }
    let record = store
        .get_encoded_task(id)
        .await?
        .ok_or("committed execution disappeared")?
        .summary;
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
