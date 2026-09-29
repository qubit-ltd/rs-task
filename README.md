# Qubit Task (`rs-task`)

[![Rust CI](https://github.com/qubit-ltd/rs-task/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-task/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-task/coverage-badge.json)](https://qubit-ltd.github.io/rs-task/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-task.svg?color=blue)](https://crates.io/crates/qubit-task)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![中文文档](https://img.shields.io/badge/文档-中文版-blue.svg)](README.zh_CN.md)

`qubit-task` solves a common problem in Rust services: a request arrives whose work takes far longer than the request should stay open, such as importing a large CSV file into the database. Running the import inside the request handler ties up the connection, offers no progress view, and loses the work if the process restarts. This crate lets the handler describe the work as a versioned `TaskRequest`, hand it to one `TaskExecutionService`, and return a task ID immediately. The service runs the matching `TaskHandler` under bounded queue, concurrency, and resource limits, keeps a queryable record of every task, and, with a recoverable store, resumes accepted work after a restart. It schedules within one process and does not turn business side effects into exactly-once operations.

By design, a `TaskExecutionService` is assembled from four replaceable parts: a **task store** (`TaskStore`), a **scheduling policy** (`SchedulingPolicy`), an **execution engine** (`TaskExecutionEngine`), and **business handlers** (`TaskHandler`). Applications inject their own trait implementations or register and discover providers through [`qubit-spi`](doc/user-guide.md#assemble-components-with-qubit-spi) (optional `inventory` feature). What ships in this crate are defaults for single-process development: an in-memory store, a fair FIFO scheduler, and a local in-process engine, plus SQLite persistence and restart recovery when the `sqlite` feature is enabled. Import, export, and other job logic always come from the application as versioned handlers. `TaskExecutionServiceBuilder::in_memory()` and `recoverable_sqlite()` are preset combinations of those built-ins; they do not prevent swapping in a custom store or other backends.

## A data import service example

A tenant administrator uploads a CSV file to object storage and calls `POST /imports`. The API encodes the tenant ID and object key into a `csv-import` task, submits it with a client-generated request key, and returns the task ID with HTTP 202. The `CsvImportV1` handler decodes the payload, imports rows in batches through the application's repository, and stops between batches when cancellation is requested. `GET /imports/{id}` maps the stored task state to an API status. If the process restarts with the SQLite store, queued imports resume and an interrupted running import may run again, so the repository must tolerate repeated batches. The imported rows live in the application database; the task record keeps only a short summary.

## Installation

```toml
[dependencies]
qubit-task = { version = "0.7", features = ["sqlite"] }
tokio = { version = "1.53", features = ["macros", "rt-multi-thread"] }
```

The `sqlite` feature enables restart recovery through `TaskExecutionServiceBuilder::recoverable_sqlite`. Omit it when volatile in-memory execution is enough. The quick start also uses `serde` and `serde_json` to encode task payloads.

| Feature | Effect |
| --- | --- |
| Default (empty) | In-memory service and explicit SPI registration |
| `sqlite` | Persistent history and restart recovery |
| `inventory` | Discover providers linked into the application |
| `event-bus` | Publish best-effort lifecycle notifications |

## Quick start

Complete, runnable programs live in [`examples/task_service.rs`](examples/task_service.rs) (local closures, cooperative cancellation, versioned requests) and [`examples/blocked_maintenance.rs`](examples/blocked_maintenance.rs) (operator review of blocked tasks). The excerpts below show how the import feature connects to the service; `ImportRepository` is an application interface that you connect to your own storage.

The handler module owns the payload format and the versioned handler:

```rust
// src/imports/handler.rs
use std::sync::Arc;

use qubit_task::handler::{TaskContext, TaskHandler, TaskHandlerDescriptor, TaskRunOutcome, TaskRunResult};
use qubit_task::model::{TaskOutput, TaskRunError};
use qubit_task::store::TaskFuture;
use serde::{Deserialize, Serialize};

// The payload stores import parameters; the CSV file itself stays in object storage.
#[derive(Serialize, Deserialize)]
pub struct CsvImportJob {
    pub tenant_id: String,
    pub object_key: String,
}

// Application error classified by the repository.
pub struct ImportError {
    pub category: String,
    pub message: String,
    pub retryable: bool,
}

// The application implements this against its object store and database.
pub trait ImportRepository: Send + Sync {
    // Imports the next batch after `imported_rows` rows; `None` means the file is exhausted.
    fn import_next_batch(&self, job: &CsvImportJob, imported_rows: usize)
        -> Result<Option<usize>, ImportError>;
}

pub struct CsvImportV1 {
    repository: Arc<dyn ImportRepository>,
}

impl CsvImportV1 {
    pub fn new(repository: Arc<dyn ImportRepository>) -> Self {
        Self { repository }
    }
}

impl From<ImportError> for TaskRunError {
    fn from(error: ImportError) -> Self {
        TaskRunError { category: error.category, message: error.message, retryable: error.retryable }
    }
}

impl TaskHandler for CsvImportV1 {
    fn descriptor(&self) -> TaskHandlerDescriptor {
        TaskHandlerDescriptor { task_type: "csv-import".into(), version: "1".into() }
    }

    fn run<'a>(&'a self, payload: &'a [u8], context: TaskContext) -> TaskFuture<'a, TaskRunResult> {
        Box::pin(async move {
            let job: Arc<CsvImportJob> = serde_json::from_slice(payload)
                .map(Arc::new)
                .map_err(|error| TaskRunError {
                    category: "invalid_payload".into(),
                    message: error.to_string(),
                    retryable: false,
                })?;
            let mut imported_rows = 0_usize;
            loop {
                // Cancellation is cooperative: stop between batches when requested.
                if context.is_cancelled() {
                    return Ok(TaskRunOutcome::Cancelled);
                }
                let repository = Arc::clone(&self.repository);
                let job = Arc::clone(&job);
                // Parsing and database writes block; keep them off the async workers.
                let batch = tokio::task::spawn_blocking(move || repository.import_next_batch(&job, imported_rows))
                    .await
                    .map_err(|error| TaskRunError {
                        category: "import_worker".into(),
                        message: error.to_string(),
                        retryable: false,
                    })??;
                match batch {
                    Some(rows) => imported_rows += rows,
                    None => break,
                }
            }
            // Only this small summary is persisted; imported rows live in the application database.
            Ok(TaskRunOutcome::Succeeded(TaskOutput {
                summary: format!("imported {imported_rows} rows").into_bytes(),
            }))
        })
    }
}
```

The API module submits tasks and translates task state for clients. It depends on the payload type, not on the handler:

```rust
// src/imports/api.rs
use qubit_task::TaskExecutionService;
use qubit_task::model::{TaskId, TaskRequest, TaskState};
use qubit_task::service::TaskServiceError;

use super::handler::CsvImportJob;

pub enum StartImport {
    // Return the task ID to the client so it can poll the status endpoint.
    Accepted { task_id: TaskId },
    // The waiting queue is full; answer with HTTP 429 and let the client retry.
    Busy,
}

pub enum ImportStatus {
    Pending,
    Running,
    Done { summary: String },
    Failed { category: String, message: String },
    NeedsOperator { reason: String },
    Cancelled,
    Unknown,
}

// `request_key` is generated once per import by the client and reused on retries.
pub async fn start_import(
    tasks: &TaskExecutionService,
    request_key: &str,
    job: &CsvImportJob,
) -> Result<StartImport, Box<dyn std::error::Error>> {
    let payload = serde_json::to_vec(job)?;
    let mut request = TaskRequest::new("csv-import", "1", payload)
        .with_idempotency_key(request_key);
    request.correlation_key = Some(job.tenant_id.clone());
    match tasks.submit(request).await {
        // An identical retry with the same key returns the original record.
        Ok(record) => Ok(StartImport::Accepted { task_id: record.id }),
        Err(TaskServiceError::QueueFull) => Ok(StartImport::Busy),
        Err(error) => Err(error.into()),
    }
}

pub async fn import_status(
    tasks: &TaskExecutionService,
    task_id: TaskId,
) -> Result<ImportStatus, TaskServiceError> {
    // `get_summary` never loads the payload.
    let Some(summary) = tasks.get_summary(task_id).await? else {
        return Ok(ImportStatus::Unknown);
    };
    Ok(match summary.state {
        TaskState::Queued => ImportStatus::Pending,
        TaskState::Running => ImportStatus::Running,
        TaskState::Succeeded => ImportStatus::Done {
            summary: summary
                .output
                .map(|output| String::from_utf8_lossy(&output.summary).into_owned())
                .unwrap_or_default(),
        },
        TaskState::Failed { category, message } => ImportStatus::Failed { category, message },
        TaskState::Panicked { message } => ImportStatus::Failed { category: "panic".into(), message },
        TaskState::Blocked { reason } => ImportStatus::NeedsOperator { reason },
        TaskState::Cancelled => ImportStatus::Cancelled,
    })
}
```

At startup, the application builds one `TaskExecutionService` and registers every handler the process may run in its `TaskHandlerRegistry` (one entry per business task type and payload version), then shares clones of the service with its request handlers. The scheduler does not use one global handler for all work: only a task whose `task_type` and `handler_version` match a registered `TaskHandlerDescriptor` can run. Submission may still accept and persist a request without a matching handler; it becomes `Blocked`. For recovery, register the missing handler before rebuilding the service against the same database, then call `retry_blocked`. Different task types can be added with repeated `register_handler` calls, or the whole table can be supplied with `handlers(...)`. Each `(task_type, handler_version)` may be registered only once; every task with that key shares the same `Arc<dyn TaskHandler>` instance. When `max_running_tasks` allows it, several tasks of the same type may call `run` on that shared handler at the same time on different Tokio workers, so `TaskHandler` requires `Send + Sync`, the implementation must be thread-safe, and per-run state belongs in the payload, `TaskContext`, or your own synchronized structures—not in unsynchronized mutable fields on the handler. Shut it down explicitly so the result of draining accepted work is observable:

```rust
// src/main.rs (startup excerpt)
use std::num::NonZeroUsize;
use std::sync::Arc;

use qubit_task::TaskExecutionServiceBuilder;

let tasks = TaskExecutionServiceBuilder::recoverable_sqlite("./state/tasks.sqlite")?
    .register_handler(Arc::new(CsvImportV1::new(repository)))?
    .max_running_tasks(NonZeroUsize::new(4).expect("positive limit"))
    .build()
    .await?;
let api_tasks = tasks.clone();
// ... serve HTTP requests with `api_tasks` ...
tasks.shutdown().await?;
```

`repository` is the application's `Arc<dyn ImportRepository>`. `recoverable_sqlite` opens the database and takes an operating-system lock; the subsequent `build().await` scans unfinished work before returning the service. A lock or recovery failure aborts startup instead of falling back to memory. Replace it with `TaskExecutionServiceBuilder::in_memory()` for volatile execution, where pending work and history are lost on exit.

What the client observes: `start_import` returns within the request; the import runs under `max_running_tasks` and the default single CPU slot per request; `import_status` reports `Pending`, `Running`, and then a terminal state. A retryable `ImportError` is retried with exponential backoff (1 second initially, capped at 60 seconds) up to three attempts by default, after which the task becomes `Blocked` for operator review. A recovered task without a registered `(task_type, handler_version)` also becomes `Blocked`; register the handler, rebuild against the same database, and call `retry_blocked`. `tasks.cancel(id)` immediately cancels queued or blocked tasks. For running work it requests cooperative cancellation; the handler must acknowledge it with `TaskRunOutcome::Cancelled`. Recovery is at-least-once, so `ImportRepository` must make repeated batches safe. See the [user guide](doc/user-guide.md) for resource capacity, local closures, notifications, and maintenance.

## What it provides

- A versioned `TaskRequest` (task type, exact handler version, opaque payload, resource demand, correlation and idempotency keys, small metadata) and a queryable `TaskRecord`/`TaskSummary` lifecycle with `Queued`, `Running`, `Blocked`, `Succeeded`, `Failed`, `Panicked`, and `Cancelled` states; one service hosts many `TaskHandler` registrations keyed by `(task_type, handler_version)`, with request fields selecting the handler; the same handler instance is shared across concurrent tasks for that key and must be `Send + Sync`.
- One `TaskExecutionService` facade: `submit`, `submit_local`, `get`, `get_summary`, `get_by_idempotency_key`, `list`, `wait`, `cancel`, `retry_blocked`, `abandon_blocked`, `prune_terminal_before`, `stats`, `shutdown`, and `shutdown_until`.
- Bounded waiting queue with `QueueFull` backpressure, a fair FIFO policy, CPU slot, GPU, and named resource budgets, and an independent `max_running_tasks` limit.
- Automatic retry of retryable handler errors with persisted backoff, an attempt budget that survives restarts, and `Blocked` records for missing handlers, exhausted attempts, or full retry queues.
- `TaskExecutionService::in_memory()` for volatile work, including `submit_local` closures with a typed `LocalTaskHandle<R, E>`, and an optional `sqlite` store with restart recovery and schema migration.
- Replaceable store, policy, engine, and handlers (see the introduction): direct injection via `from_components` / `register_handler`, or extension through `qubit-spi`.
- Optional `event-bus` feature that publishes best-effort `TaskEvent` notifications through a `qubit-event-bus` `EventBus` supplied by the application.

The crate does not provide multi-node or distributed scheduling, workflow dependencies, cron-style scheduling, forced interruption of arbitrary code, or exactly-once business side effects. `submit_local` is unavailable with a restart-recoverable store because a closure cannot be rebuilt from a database. Notifications are best effort: a full queue (256 entries by default) drops the event, and a publish failure never rolls back a task transition.

Limits that shape a deployment: the in-memory preset keeps a waiting queue of 1,024 tasks, 1,024 terminal records, 2,048 nonterminal records (including `Blocked`), and 64 MiB of request payloads; each request payload is at most 16 MiB, submissions share a 64 MiB in-flight payload budget and 64 in-flight write operations, and history pages return at most 256 records. Request text limits are measured in UTF-8 bytes: `task_type` 128, `handler_version` 64, correlation and idempotency keys 256, and metadata 32 entries with 128-byte keys, 4,096-byte values, and 16,384 combined bytes; persisted diagnostics are bounded to 128-byte categories and 4,096-byte messages. On restart, unfinished records must fit within `queue_capacity + max_running_tasks` or construction fails with the records preserved. SQLite runs one blocking database operation at a time and keeps history until the application calls `prune_terminal_before`; pruning releases idempotency keys for reuse. `shutdown_until` bounds only the caller's wait, dropping the last service handle starts an asynchronous drain, and a scheduler panic is reported as `SchedulerUnavailable` without automatic restart. See the [user guide](doc/user-guide.md#boundaries-and-a-practice-checklist) for the full list.

## Learn more

- [User guide](doc/user-guide.md)
- [Detailed design](doc/task_execution_service_design.en.md)
- [API reference](https://docs.rs/qubit-task)

## Testing

```bash
# Run tests with the default feature set
cargo test

# Run tests with all declared features
cargo test --all-features

# Project CI checks
./ci-check.sh

# Check code coverage
./coverage.sh
```

## License

Copyright (c) 2025 - 2026. Haixing Hu. All rights reserved.

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for the
full license text.

## Contributing

Contributions are welcome. Please follow the Rust API guidelines, keep public
API documentation and tests current, and run `./align-ci.sh` to format code and
`./ci-check.sh` to satisfy CI requirements before submitting a pull request.

## Author

**Haixing Hu** - *Qubit Co. Ltd.*

Repository: [https://github.com/qubit-ltd/rs-task](https://github.com/qubit-ltd/rs-task)
