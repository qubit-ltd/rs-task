# Qubit Task user guide

[Chinese user guide](user-guide.zh_CN.md) · [README](../README.md) · [API reference](https://docs.rs/qubit-task)

This guide covers `qubit-task` 0.7.x on Rust 1.94 or later. It is for Rust service developers who receive requests whose work outlives the request: imports, exports, report generation, media processing, and similar background jobs. Reading through [Check the task result](#check-the-task-result) is enough to accept such work, run it under bounded concurrency, and report its state back to clients. Later sections cover restart recovery, resource budgets, cancellation, retries, history maintenance, status notifications, component assembly, and shutdown. Developers who implement a store, scheduling policy, or execution engine should read [Assemble components with `qubit-spi`](#assemble-components-with-qubit-spi) and the [detailed design](task_execution_service_design.en.md).

## Contents

- [The problem it solves](#the-problem-it-solves)
- [Where to start](#where-to-start)
- [Integrate a CSV import service](#integrate-a-csv-import-service)
  - [Define the job payload and the handler](#define-the-job-payload-and-the-handler)
  - [Submit from the request handler](#submit-from-the-request-handler)
  - [Report status to the client](#report-status-to-the-client)
  - [Assemble the service at startup](#assemble-the-service-at-startup)
  - [Types used on this path](#types-used-on-this-path)
- [Check the task result](#check-the-task-result)
  - [What success looks like](#what-success-looks-like)
  - [Failed, panicked, and blocked](#failed-panicked-and-blocked)
- [Find a task after the caller stopped waiting](#find-a-task-after-the-caller-stopped-waiting)
- [Cancel an import](#cancel-an-import)
- [Retry policy and attempt budget](#retry-policy-and-attempt-budget)
- [Recover accepted work after a restart](#recover-accepted-work-after-a-restart)
- [Run process-local closures](#run-process-local-closures)
- [Bound concurrency and resources](#bound-concurrency-and-resources)
  - [CPU slots, GPUs, and named resources](#cpu-slots-gpus-and-named-resources)
  - [Running tasks and the waiting queue](#running-tasks-and-the-waiting-queue)
- [Browse history and keep it bounded](#browse-history-and-keep-it-bounded)
- [Publish status changes](#publish-status-changes)
  - [Subscribe inside the process](#subscribe-inside-the-process)
  - [Publish through Redis Streams](#publish-through-redis-streams)
  - [Notification counters and shutdown](#notification-counters-and-shutdown)
- [Assemble components with `qubit-spi`](#assemble-components-with-qubit-spi)
- [Lifecycle and shutdown](#lifecycle-and-shutdown)
- [Errors, diagnostics, and troubleshooting](#errors-diagnostics-and-troubleshooting)
- [Migration from 0.5 and earlier](#migration-from-05-and-earlier)
- [Boundaries and a practice checklist](#boundaries-and-a-practice-checklist)
- [Further reading](#further-reading)

## The problem it solves

Take a tenant administration API. An administrator uploads a CSV file with thousands of customer rows to object storage and asks the service to import it. The import parses the file, validates each row, and writes to the database; it can take minutes. If the HTTP handler does that work itself, the connection stays open for the whole import, the client sees no progress, a load balancer may cut the request off, and a process restart loses the import without any record that it was ever requested. Running the import on a bare `tokio::spawn` fixes only the first problem: nothing bounds how many imports run at once, nothing records their state for a later status query, and nothing brings them back after a restart.

`qubit-task` gives the service one `TaskExecutionService`. The HTTP handler describes the import as a `TaskRequest` (task type `csv-import`, handler version `1`, a small payload with the tenant ID and object key), submits it, and returns the task ID to the client right away. The service queues the request, starts the registered `CsvImportV1` handler when a running slot and the requested resources are free, records every state change, and answers `GET /imports/{id}` from that record. With the SQLite store, accepted imports survive a restart. The imported rows still live in the application database; the task record keeps only a short summary.

The crate schedules work **inside one process**. Recovery is at-least-once: an import interrupted mid-run may run again, so the repository must tolerate repeated batches. The crate does not distribute work across nodes, chain tasks into workflows, run cron schedules, forcibly interrupt running code, or make business side effects exactly-once. The relevant sections below state these boundaries where they matter.

## Where to start

1. [Integrate a CSV import service](#integrate-a-csv-import-service) covers the handler, the submit call, the status endpoint, and startup wiring.
2. [Check the task result](#check-the-task-result) shows what a finished task looks like and how `Failed`, `Panicked`, and `Blocked` differ. The basic integration ends there.
3. Read on as needed: [Find a task after the caller stopped waiting](#find-a-task-after-the-caller-stopped-waiting), [Cancel an import](#cancel-an-import), [Retry policy and attempt budget](#retry-policy-and-attempt-budget), [Recover accepted work after a restart](#recover-accepted-work-after-a-restart), [Bound concurrency and resources](#bound-concurrency-and-resources), [Publish status changes](#publish-status-changes), or [Lifecycle and shutdown](#lifecycle-and-shutdown).

Complete, runnable programs live in [`examples/task_service.rs`](../examples/task_service.rs) (local closures, cooperative cancellation, versioned requests) and [`examples/blocked_maintenance.rs`](../examples/blocked_maintenance.rs) (operator review of blocked tasks).

## Integrate a CSV import service

Add the crate, an async runtime, and a payload codec:

```toml
[dependencies]
qubit-task = { version = "0.7", features = ["sqlite"] }
tokio = { version = "1.53", features = ["macros", "rt-multi-thread"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
```

The `sqlite` feature enables restart recovery. Leave it out when volatile in-memory execution is enough; the code below changes only in the builder call. `serde_json` is the application's choice of payload encoding; the crate treats the payload as opaque bytes.

The import feature has three parts. The handler module owns the payload format and the versioned handler. The API module submits tasks and maps task state to client-facing status. Startup wiring builds one service and shares it. `ImportRepository` is an application interface backed by the object store and database; the service never sees it.

### Define the job payload and the handler

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

`TaskHandlerDescriptor` names the exact `(task_type, version)` pair this handler accepts. A stored request is only ever given to the handler with the same pair, so changing the payload format means registering `CsvImportV2` alongside `CsvImportV1` rather than editing the old handler. `run` receives the opaque payload and a `TaskContext` with `task_id()`, `attempt()` (starting at 1), `assigned_resources()`, and `is_cancelled()`. The future runs on the Tokio async workers; long parsing and database writes go through `spawn_blocking` so they do not stall other tasks. The handler decides whether an `ImportError` is retryable; the service retries only errors marked `retryable: true`. `TaskOutput.summary` is bounded persisted text, not the result itself.

### Submit from the request handler

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
```

`TaskRequest::new` sets one CPU slot and no optional fields. The idempotency key is required by `submit`: it must be non-empty, and the client (or the API layer, before it does anything else) must generate and keep it, so that a retried `POST /imports` maps to the same task. An identical request with the same key returns the existing record; a different request with the same key fails with `StoreError::IdempotencyConflict`. `correlation_key` is an application value for finding related tasks later; here it is the tenant ID. `submit` returns the accepted `TaskRecord`, whose `id` is the client-facing handle. `QueueFull` is backpressure, not a failure of the request; surface it as HTTP 429 and let the client retry with the same key.

### Report status to the client

```rust
// src/imports/api.rs (continued)
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

`get_summary` returns a `TaskSummary`: the request metadata without the payload, the lifecycle state, `state_version`, `attempt`, timestamps, assigned resources, and the output summary. Use it for every status read. Call `get` only when the application needs the full `TaskRecord` with its payload. `Unknown` covers a task ID that was never accepted or whose record has been pruned or evicted.

### Assemble the service at startup

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

`repository` is the application's `Arc<dyn ImportRepository>`. Build the service before the HTTP listener opens and register every handler version the store may still contain; the registry is fixed once `build()` returns, and a duplicate `(task_type, version)` fails with `HandlerConflict`. `recoverable_sqlite` opens the database, takes an operating-system lock so two processes never execute the same database, and scans unfinished work before returning. `TaskExecutionService` is `Clone`; hand clones to request handlers and keep one for shutdown. For volatile execution, use `TaskExecutionServiceBuilder::in_memory()` (or the shortcut `TaskExecutionService::in_memory().await?`): pending work and history are lost when the process exits.

### Types used on this path

| Type | Role |
| --- | --- |
| `TaskExecutionService` | The one facade: submit, query, wait, cancel, maintain, shut down. `clone` it into each module. |
| `TaskExecutionServiceBuilder` | Chooses the store, handlers, capacity, limits, retry policy, and optional event bus. |
| `TaskRequest` | Reconstructable description: task type, exact handler version, payload, resource demand, correlation and idempotency keys, metadata. |
| `TaskHandler` / `TaskHandlerDescriptor` | Versioned code that interprets one payload format. |
| `TaskContext` | Per-attempt task ID, attempt number, assigned resources, and the cooperative cancellation flag. |
| `TaskRunOutcome` / `TaskRunError` | Handler result: `Succeeded(TaskOutput)`, `Cancelled`, or a classified error with a `retryable` flag. |
| `TaskRecord` / `TaskSummary` | Queryable lifecycle; the summary omits the payload. |
| `TaskState` | `Queued`, `Running`, `Blocked { reason }`, `Succeeded`, `Failed { category, message }`, `Panicked { message }`, `Cancelled`. |
| `TaskServiceError` | Facade errors such as `QueueFull`, `Blocked`, `AttemptsExhausted`, `ShuttingDown`, `StoreUnavailable`, `SchedulerUnavailable`. |

## Check the task result

`submit` returning `Ok(record)` means the request is **accepted and stored** with `state == Queued`. It says nothing about when the import runs. The stages after acceptance are visible in `TaskSummary`:

| Stage | Observable fields |
| --- | --- |
| Accepted | `state == Queued`, `accepted_at_ms` set, `attempt == 0`. |
| Scheduled and started | `state == Running`, `started_at_ms` set, `attempt >= 1`, `assigned_resources` filled. |
| Waiting for a retry | `state == Queued`, `attempt >= 1`, `retry_not_before_ms` set. |
| Terminal | `state.is_terminal()`, `finished_at_ms` set; `output` present only for `Succeeded`. |
| Needs intervention | `state == Blocked { reason }`; not terminal, not scheduled. |

`state_version` increases with every transition. Consumers that see events or snapshots out of order should keep the highest version.

### What success looks like

To block until the import is done, for example in an integration test or a synchronous batch tool, use `wait`:

```rust
use qubit_task::TaskExecutionService;
use qubit_task::model::{TaskId, TaskState, TaskSummary};
use qubit_task::service::TaskServiceError;

pub async fn wait_for_import(
    tasks: &TaskExecutionService,
    task_id: TaskId,
) -> Result<TaskSummary, TaskServiceError> {
    match tasks.wait(task_id).await {
        Ok(summary) => {
            // summary.state is Succeeded, Failed, Panicked, or Cancelled here.
            if let TaskState::Succeeded = summary.state {
                let text = summary
                    .output
                    .as_ref()
                    .map(|output| String::from_utf8_lossy(&output.summary).into_owned())
                    .unwrap_or_default();
                println!("import {task_id} finished: {text}");
            }
            Ok(summary)
        }
        Err(TaskServiceError::Blocked) => {
            // The task needs an operator; read the reason with get_summary.
            Err(TaskServiceError::Blocked)
        }
        Err(error) => Err(error),
    }
}
```

For the import example, a normal run prints `import <id> finished: imported 4213 rows`, and the summary shows `state == Succeeded`, `attempt == 1`, and `finished_at_ms` set. `wait` resolves on the first terminal state and wakes only waiters in this process. It returns `Err(Blocked)` when the task enters `Blocked`, `Err(Store(NotFound))` for an unknown ID, and `StoreUnavailable` or `SchedulerUnavailable` when the service itself has failed. A long-running HTTP request should not hold `wait`; the polling endpoint above is the normal path.

### Failed, panicked, and blocked

- **`Failed { category, message }`**: the handler returned a `TaskRunError` with `retryable: false`, or the successful handler output exceeded the 64 KiB summary limit. `category` is the handler's stable classification (`invalid_payload`, `import_worker`, or whatever the repository chose); `message` is bounded to 4,096 bytes and truncated at a UTF-8 boundary. Keep the full original error in application logs.
- **`Panicked { message }`**: the handler future panicked. The engine reports it regardless of where inside the handler it happened. A business error whose category is the string `panic` stays a `Failed`.
- **`Blocked { reason }`**: the service cannot continue without intervention. Reasons include a missing handler for a recovered `(task_type, handler_version)`, an exhausted attempt budget, a full waiting queue when the failed attempt is requeued, or `EngineError::Closed` from `activate`. The record stays queryable; see [Retry policy and attempt budget](#retry-policy-and-attempt-budget) for `retry_blocked` and [Browse history and keep it bounded](#browse-history-and-keep-it-bounded) for `abandon_blocked`.
- **`Cancelled`**: the task was cancelled before it started, or the handler acknowledged a cancellation request. See [Cancel an import](#cancel-an-import).

While the attempt budget remains, a retryable error does not produce a terminal state immediately. The record goes back to `Queued` with `retry_not_before_ms` set, and the next attempt starts after the backoff.

The basic integration ends here. The following sections are optional and organised by the question a reader has next.

## Find a task after the caller stopped waiting

A client may time out on `POST /imports` after the service accepted the request. If it retries with a fresh key, the same file is imported twice. The idempotency key prevents that, and `get_by_idempotency_key` lets the API find the earlier acceptance:

```rust
use qubit_task::TaskExecutionService;
use qubit_task::model::TaskId;

pub async fn start_or_find_import(
    tasks: &TaskExecutionService,
    request_key: &str,
    job: &CsvImportJob,
) -> Result<Option<TaskId>, Box<dyn std::error::Error>> {
    // A previous attempt may have been accepted after the client gave up waiting.
    if let Some(existing) = tasks.get_by_idempotency_key(request_key).await? {
        return Ok(Some(existing.id));
    }
    match start_import(tasks, request_key, job).await? {
        StartImport::Accepted { task_id } => Ok(Some(task_id)),
        StartImport::Busy => Ok(None),
    }
}
```

`get_by_idempotency_key` returns a payload-free `TaskSummary`. `None` is a snapshot: an acceptance may still be in flight, which is why the retry goes through `submit` with the **same** key rather than a new one. `submit` then returns the existing record. The key stays reserved only while its record is retained; after pruning or in-memory eviction it can be reused for a new task. A different request under a reused key is rejected with `StoreError::IdempotencyConflict`, and an exact replay returns the original record even if configured capacity has since decreased. Keys are limited to 256 UTF-8 bytes.

To list a tenant's imports rather than one task, filter by `correlation_key`:

```rust
use qubit_task::model::{TaskQuery, TaskStateKind, TaskSummary};

pub async fn active_imports_for_tenant(
    tasks: &TaskExecutionService,
    tenant_id: &str,
) -> Result<Vec<TaskSummary>, TaskServiceError> {
    let mut cursor = None;
    let mut active = Vec::new();
    loop {
        let page = tasks
            .list(TaskQuery {
                states: vec![TaskStateKind::Queued, TaskStateKind::Running, TaskStateKind::Blocked],
                limit: 100,
                after: cursor,
                correlation_key: Some(tenant_id.to_owned()),
            })
            .await?;
        active.extend(page.records);
        cursor = page.next;
        if cursor.is_none() {
            return Ok(active);
        }
    }
}
```

`TaskQuery.states` uses `TaskStateKind`, the lifecycle category without diagnostics. Pages are ordered by `(accepted_at_ms, id)` and `limit` may not exceed 256 (`InvalidRequest` otherwise; 0 is treated as 1). The cursor is not a snapshot across concurrent writes.

## Cancel an import

Cancellation has two different outcomes depending on the task's state:

```rust
use qubit_task::TaskExecutionService;
use qubit_task::model::TaskId;
use qubit_task::service::{CancelOutcome, TaskServiceError};

pub enum CancelImport {
    Cancelled,
    Requested,
    AlreadyFinished,
}

pub async fn cancel_import(
    tasks: &TaskExecutionService,
    task_id: TaskId,
) -> Result<CancelImport, TaskServiceError> {
    Ok(match tasks.cancel(task_id).await? {
        // Queued or Blocked: the task is Cancelled now and never runs.
        CancelOutcome::CancelledBeforeStart => CancelImport::Cancelled,
        // Running: the flag is set; the handler decides when to stop.
        CancelOutcome::CancellationRequested => CancelImport::Requested,
        CancelOutcome::AlreadyTerminal => CancelImport::AlreadyFinished,
    })
}
```

A `Queued` or `Blocked` task becomes `Cancelled` immediately. For a `Running` task the service persists `cancel_requested = true` and sets the flag that `TaskContext::is_cancelled()` reads. Nothing is interrupted. `CsvImportV1` checks the flag between batches and returns `TaskRunOutcome::Cancelled`, at which point the record becomes `Cancelled`. If the handler instead finishes and returns `Succeeded` or an error, that result stands; a late cancellation request does not overwrite it. A handler that never checks the flag is never cancelled. `cancel` on an unknown ID returns `Store(NotFound)`.

## Retry policy and attempt budget

When `import_next_batch` returns `ImportError { retryable: true, .. }`, for example on a database connection reset, the service requeues the task with a persisted due time and starts it again later. Defaults are one second initially, doubling per retry, capped at sixty seconds, and three attempts in total. Configure both on the builder:

```rust
use std::time::Duration;

use qubit_task::{RetryPolicy, TaskExecutionServiceBuilder};

let builder = TaskExecutionServiceBuilder::in_memory()
    .retry_policy(RetryPolicy::new(Duration::from_secs(5), Duration::from_secs(300))?)
    .max_attempts(5);
```

`RetryPolicy::new(initial, maximum)` rejects a zero initial delay or a maximum smaller than the initial delay. `max_attempts` counts every start of the task, across process restarts included. When the budget is used up the task becomes `Blocked` rather than retrying forever, and `TaskContext::attempt()` tells the handler which attempt it is on. Each retry uses a normal queue slot; if the waiting queue is full when the failed attempt is requeued, the task becomes `Blocked` with a queue-capacity reason instead of exceeding the limit.

An operator who has fixed the cause (restored the database, installed the missing handler, freed queue capacity) requeues the task with `retry_blocked`:

```rust
use qubit_task::model::TaskState;

pub async fn retry_after_fix(
    tasks: &TaskExecutionService,
    task_id: TaskId,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(summary) = tasks.get_summary(task_id).await? {
        if let TaskState::Blocked { reason } = &summary.state {
            eprintln!("import {task_id} is blocked: {reason}");
        }
    }
    match tasks.retry_blocked(task_id).await {
        Ok(summary) => {
            // Queued again with the retry due time cleared.
            let _ = summary;
            Ok(())
        }
        Err(TaskServiceError::AttemptsExhausted { attempts, limit }) => {
            Err(format!("used {attempts}/{limit} attempts; submit a new task").into())
        }
        Err(TaskServiceError::NotBlocked { actual }) => Err(format!("task is {actual:?}").into()),
        Err(error) => Err(error.into()),
    }
}
```

`retry_blocked` clears the due time and makes the task eligible at once. It fails with `AttemptsExhausted` when the budget is gone; a fresh attempt budget needs a new task ID. Repeated batches from an earlier attempt may already have been written, so `ImportRepository` must be idempotent under retry. Handlers should mark an error retryable only when repeating the operation is safe.

## Recover accepted work after a restart

Three store choices decide what survives a restart:

| Setup | Completed history | Accepted unfinished work after restart |
| --- | --- | --- |
| `TaskExecutionServiceBuilder::in_memory()` | Bounded memory history | Lost when the process exits |
| Custom `TaskStore` with persistent history | Persistent | Depends on the store's declared `restart_recovery` capability |
| `TaskExecutionServiceBuilder::recoverable_sqlite(path)` | SQLite | Queued work is restored; interrupted running work may run again |

The import service uses SQLite. `recoverable_sqlite(path)` opens (or creates) the database, sets `require_recovery(true)`, and returns a builder. On `build()`, the service acquires ownership, checks that the number of unfinished records fits within `queue_capacity + max_running_tasks`, and stages every `Queued` and `Running` record back into the waiting queue. Recovered `Running` records are treated as interrupted attempts: they run again, so the repository must tolerate a repeated batch. Recovery is therefore **at-least-once**.

Observable startup outcomes:

- **Normal**: `build()` returns, the recovered tasks are `Queued`, and the queue may temporarily hold more than `queue_capacity` entries. New submissions receive `QueueFull` until that backlog drains.
- **Too many unfinished records**: `build()` fails with `TaskServiceBuildError::RecoveryCapacityExceeded` and leaves the records intact. Raise `queue_capacity` or `max_running_tasks` and start again.
- **Another process owns the database**: `build()` fails with a store error. There is no fallback to memory; do not open the HTTP listener.
- **A recovered task has no registered handler**: `build()` succeeds and that task is `Blocked` with a reason naming the missing `(task_type, handler_version)`. Register the handler, rebuild against the same database, and call `retry_blocked`. Startup does not requeue it automatically.
- **A recovered task has already used `max_attempts`**: it becomes `Blocked` without starting again; `retry_blocked` reports `AttemptsExhausted`.

Retry due times are persisted with the record, so a restart does not start a retry early. `submit_local` is unavailable on a store that declares restart recovery (`TaskServiceError::UnsupportedCapability`), because a closure cannot be rebuilt from a database; `capabilities().submit_local` reports this. `capabilities().store` reports the `persistent_history` and `restart_recovery` flags of the store that was actually assembled.

SQLite schema 3 stores request metadata, the payload BLOB, and lifecycle JSON in separate columns; summary reads and transitions never select the BLOB. Opening a schema 0, 1, or 2 database migrates it to schema 3 in one transaction and keeps payloads, idempotency keys, and lifecycle values. A newer schema or an unknown record format is rejected explicitly. SQLite runs one blocking database operation at a time on Tokio's blocking pool, so callers must poll from a Tokio runtime. After the service releases ownership during shutdown, an old store handle can no longer write.

### Safe SQLite upgrade

Stop every old service and await successful draining before backing up the database. Deploy the new version, open the same database through a new store instance, inspect recovered summaries, and then reopen the business entry points. A shutdown timeout does not prove a safe handoff; do not run old and new processes concurrently. The lock name appends `.owner.lock` to the complete database filename, for example `jobs.sqlite.owner.lock`. Unix and Windows validate physical file identity; databases with multiple hard links are rejected. Keep the containing directory and lock file trusted and stable. Schema 0, 1, and 2 still migrate to schema 3 without discarding task data.

## Run process-local closures

Some work is short, belongs to the current request, and has no meaning after a restart: rendering a preview of the first lines of the uploaded CSV so the administrator can confirm the column mapping. Such work has a typed in-process result and needs no handler registration. `submit_local` accepts a closure and returns a `LocalTaskHandle<R, E>`:

```rust
use qubit_task::TaskExecutionService;
use qubit_task::model::TaskOutput;
use qubit_task::service::{LocalTaskOutcome, LocalTaskResultError};

pub async fn render_preview(
    tasks: &TaskExecutionService,
    csv_head: Vec<u8>,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let handle = tasks
        .submit_local(move |context| {
            let mut rendered = Vec::new();
            for line in csv_head.split(|byte| *byte == b'\n') {
                if context.is_cancelled() {
                    return LocalTaskOutcome::<Vec<u8>, String>::Cancelled;
                }
                rendered.extend_from_slice(line);
                rendered.push(b'\n');
            }
            LocalTaskOutcome::Succeeded {
                value: rendered,
                summary: TaskOutput { summary: b"preview rendered".to_vec() },
            }
        })
        .await?;
    match handle.result().await {
        Ok(Ok(bytes)) => Ok(bytes),
        Ok(Err(message)) => Err(message.into()),
        Err(LocalTaskResultError::Cancelled) => Err("preview cancelled".into()),
        Err(error) => Err(error.to_string().into()),
    }
}
```

The closure runs on Tokio's blocking pool and returns `LocalTaskOutcome::Succeeded { value, summary }`, `Failed(E)`, or `Cancelled`. `E` must implement `Display` because the service persists its text as the failure diagnostic. `handle.result()` yields `Ok(Ok(value))`, `Ok(Err(error))` with the original typed error, or `Err(LocalTaskResultError)` for cancellation, a panic, a blocked task, or an infrastructure failure. The task still has a record: `handle.task_id()` works with `get_summary` and `cancel`, and `TaskRecord.output` keeps only the `summary`. The typed value exists **only** in this handle. If the request awaiting `submit_local` is cancelled or times out, acceptance may still complete in the background, but the handle is gone and the value cannot be recovered. Work that must be found later belongs in a keyed `submit`.

The in-memory preset (`in_memory()`) uses local execution, one CPU slot per available core (or one), a waiting queue of 1,024 tasks, a terminal history of 1,024 records, and at most 2,048 nonterminal records including `Blocked`. It does not probe for GPUs. To change the memory store's limits, build a `MemoryTaskStore::with_limits(history_capacity, payload_budget, unfinished_limit)` and pass it to `TaskExecutionServiceBuilder::store(Arc::new(...))`; reaching the unfinished limit returns `UnfinishedRecordLimitExceeded`, while replays of retained idempotent tasks still succeed.

## Bound concurrency and resources

### CPU slots, GPUs, and named resources

Capacity is a set of budgets the engine reserves for each running attempt, not operating-system pinning or device discovery. The builder accepts an explicit `ResourceCapacity`; every request states its `ResourceRequest`:

```rust
use std::collections::BTreeMap;
use std::num::NonZeroUsize;

use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::model::{ResourceCapacity, TaskRequest};

let capacity = ResourceCapacity {
    cpu_slots: 8,
    gpus: BTreeMap::from([
        ("gpu-0".into(), vec!["cuda".into()]),
        ("gpu-1".into(), vec!["cuda".into()]),
    ]),
    custom: BTreeMap::from([("import_db_connections".into(), 4)]),
};
let builder = TaskExecutionServiceBuilder::in_memory()
    .capacity(capacity)
    .max_running_tasks(NonZeroUsize::new(16).expect("positive limit"))
    .queue_capacity(4_096);

// The import is I/O-bound: no CPU slot, but one of the four pooled connections.
let mut import = TaskRequest::new("csv-import", "1", payload.clone());
import.resources.cpu_slots = 0;
import.resources.custom.insert("import_db_connections".into(), 1);

// An embedding job needs one CUDA device.
let mut embedding = TaskRequest::new("embedding", "2", payload);
embedding.resources.gpu_count = 1;
embedding.resources.gpu_labels = vec!["cuda".into()];
```

`cpu_slots` is a concurrency budget for CPU-bound handlers. `gpus` maps a device ID to its labels; a request with `gpu_count` and `gpu_labels` is assigned devices carrying every requested label, and the handler reads them from `TaskContext::assigned_resources()`. `custom` holds integer budgets for anything exclusive that the deployment defines consistently: connection pool size, licences, memory units. A request that can never fit the configured capacity is rejected at `submit` with `Unsatisfiable`; a request that fits but finds no free resources waits in the queue. Resource descriptions allow at most 32 GPU labels and 32 custom names, each non-empty and at most 128 UTF-8 bytes; GPU labels require `gpu_count > 0`.

### Running tasks and the waiting queue

Two more limits are independent of resources. `max_running_tasks` caps concurrently running attempts even when they request zero CPU slots; its default is the available parallelism, falling back to one. Set it explicitly to bound concurrent network or database work such as the import above. `queue_capacity` (default 1,024) bounds the waiting queue; a full queue rejects new submissions with `QueueFull`, which the API maps to HTTP 429.

The default fair FIFO policy lets a task that fits current free resources pass a blocked head-of-queue task, then protects a task that has been bypassed a bounded number of times so it eventually runs. The service also caps in-flight write operations (default 64, `OperationLimitExceeded`) and in-flight request payload bytes (default 64 MiB, `PayloadBudgetExceeded`); a single payload may not exceed 16 MiB. `stats()` returns the current `queued`, `running`, `blocked`, and `terminal` counts plus a `ResourceSnapshot` of free capacity; the counts and the snapshot are taken one after another, not atomically.


`submit`, `submit_local`, `cancel`, `retry_blocked`, `abandon_blocked`, and `prune_terminal_before` share `max_inflight_operations` (default 64). `cancel` can also return `OperationLimitExceeded`; apply backoff and retry. Once admitted, dropping or timing out the caller does not cancel the service-owned write worker or undo its store and cancellation side effects. Submission payload bytes have a separate budget.

## Browse history and keep it bounded

History pages come from `list(TaskQuery)`, as shown in [Find a task after the caller stopped waiting](#find-a-task-after-the-caller-stopped-waiting). The in-memory store evicts the oldest terminal records beyond its history capacity. SQLite keeps history until the application removes it. A maintenance job that runs daily can archive old records into the application's own store and then delete terminal rows in bounded batches:

```rust
use std::num::NonZeroUsize;

use qubit_task::TaskExecutionService;
use qubit_task::service::TaskServiceError;

pub async fn prune_old_terminal(
    tasks: &TaskExecutionService,
    now_ms: u64,
) -> Result<usize, TaskServiceError> {
    let cutoff = now_ms.saturating_sub(30 * 24 * 60 * 60 * 1_000);
    let batch = NonZeroUsize::new(100).expect("100 is nonzero");
    let mut total = 0;
    loop {
        let removed = tasks.prune_terminal_before(cutoff, batch).await?;
        total += removed;
        if removed < batch.get() {
            return Ok(total);
        }
    }
}
```

`prune_terminal_before(accepted_before_ms, max_rows)` deletes only terminal records accepted before the cutoff and at most `max_rows` per call. `Queued`, `Running`, and `Blocked` records stay. Deleting a record also releases its idempotency key, so the retry window for that key ends. A store without pruning support reports `UnsupportedCapability`.

`Blocked` records need a decision, not a cutoff. An operator flow lists `Blocked` summaries older than a threshold, abandons the ones nobody will fix with `abandon_blocked(id, state_version)`, and prunes them in a later pass. The version check makes a concurrent `retry_blocked` safe: if the task changed after the page was read, `abandon_blocked` returns `StoreError::Conflict`, and `NotBlocked` if it is no longer blocked. [`examples/blocked_maintenance.rs`](../examples/blocked_maintenance.rs) is the complete flow.

## Publish status changes

Polling `GET /imports/{id}` is enough for many clients. When another module should react to state changes, for example to push a WebSocket update or refresh a per-tenant dashboard, enable the `event-bus` feature and give the builder a `qubit_event_bus::EventBus`. After every state change the service publishes a `TaskEvent { task_id, state_version, state, correlation_key }` on the topic `task.lifecycle`. This release uses `qubit-event-bus` 0.16.

Publication is best effort. It never rolls back a task transition, events may be delayed, repeated, or dropped, and a consumer should treat the service's own query API as the authority and compare `state_version` before overwriting a newer state.

### Subscribe inside the process

```rust
use std::sync::Arc;

use qubit_event_bus::EventBus;
use qubit_event_bus::Subscription;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::{SubscribeRequest, Topic};
use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::event::TaskEvent;
use qubit_task::model::TaskState;

pub trait ImportStatusView: Send + Sync {
    fn record(&self, tenant_id: &str, task_id: &str, state: &TaskState, state_version: u64);
}

pub fn subscribe_status_view(
    bus: &EventBus,
    view: Arc<dyn ImportStatusView>,
) -> Result<Subscription, Box<dyn std::error::Error>> {
    let topic = Topic::<TaskEvent>::new("task.lifecycle")?;
    let request = SubscribeRequest::new("import-status-view", topic)?;
    Ok(bus.subscribe(request, move |delivery| {
        let event = delivery.payload();
        if let Some(tenant_id) = &event.correlation_key {
            // Events may repeat or arrive late; the view keeps the highest state_version.
            view.record(tenant_id, &event.task_id.to_string(), &event.state, event.state_version);
        }
    })?)
}

// Startup: create the bus, subscribe, then build the service with the bus.
let bus = EventBus::local(LocalEventBusConfig::default())?;
let status_subscription = subscribe_status_view(&bus, view)?;
let tasks = TaskExecutionServiceBuilder::in_memory()
    .event_bus(bus.clone())
    .build()
    .await?;
```

`view` is the application's dashboard store. Keep `status_subscription` and cancel it during shutdown. The built-in local bus delivers inside the process only; the `correlation_key` on the event is the tenant ID set in `start_import`, which is what makes a per-tenant view possible without loading the task.

### Publish through Redis Streams

To notify other processes, select a cross-process provider such as `qubit-event-bus-redis`. `TaskEvent` implements serde, but the event-bus facade requires an explicit `EventCodec<TaskEvent>`; register a JSON codec and select the provider by name:

```rust
use std::sync::Arc;

use qubit_event_bus::CodecError;
use qubit_event_bus::EventBusConfig;
use qubit_event_bus::EventBusRegistry;
use qubit_event_bus::codec::CodecRegistry;
use qubit_event_bus::codec::EventCodec;
use qubit_event_bus::facade::EventBusFacadeConfig;
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::SchemaId;
use qubit_event_bus_redis as _;
use qubit_spi::ProviderSelection;
use qubit_task::event::TaskEvent;
use qubit_task::service::TaskExecutionServiceBuilder;

struct TaskEventJsonCodec {
    content_type: ContentType,
    schema_id: SchemaId,
}

impl TaskEventJsonCodec {
    fn new() -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self {
            content_type: ContentType::new("application/json")?,
            schema_id: SchemaId::new("task-event-v1")?,
        })
    }
}

impl EventCodec<TaskEvent> for TaskEventJsonCodec {
    fn content_type(&self) -> &ContentType {
        &self.content_type
    }

    fn schema_id(&self) -> Option<&SchemaId> {
        Some(&self.schema_id)
    }

    fn encode(&self, value: &TaskEvent) -> Result<Arc<[u8]>, CodecError> {
        serde_json::to_vec(value)
            .map(Arc::from)
            .map_err(|source| CodecError::Encode { source: Box::new(source) })
    }

    fn decode(&self, bytes: &[u8]) -> Result<TaskEvent, CodecError> {
        serde_json::from_slice(bytes).map_err(|source| CodecError::Decode { source: Box::new(source) })
    }
}

let mut codecs = CodecRegistry::new();
codecs.register::<TaskEvent>(Arc::new(TaskEventJsonCodec::new()?));
let facade = EventBusFacadeConfig::new().with_codec_registry(Arc::new(codecs));
let config = EventBusConfig::default()
    .with_selection(ProviderSelection::named("redis-streams")?)
    .with_provider_options([
        ("redis.url".into(), "redis://127.0.0.1/".into()),
        ("redis.namespace".into(), "task-service".into()),
    ].into())
    .with_facade_config(facade);
let bus = EventBusRegistry::discover()?.create(&config)?;
let tasks = TaskExecutionServiceBuilder::in_memory()
    .event_bus(bus.clone())
    .build()
    .await?;
```

The Redis provider, `qubit-spi`, and `serde_json` are application dependencies; `use qubit_event_bus_redis as _;` links the provider so `discover()` finds it. The Redis adapter and `serde_json` are application dependencies for this example; the task crate itself also depends on `qubit-spi` and `serde_json`. A successful provider receipt means Redis accepted the publish command, not that a subscriber processed the event. Task state and event publication are not one transaction; use a transactional outbox when they must commit together. The default fixture assembles and closes the provider without publishing, so running it does not prove Redis connectivity. To exercise publication, start Redis at `redis://127.0.0.1:6379/`, submit a task, and inspect its notification receipt and subscriber output. This assembly is compiled in CI with `cargo check --locked --manifest-path tests/fixtures/doc-examples/Cargo.toml` and run with `cargo run --locked --manifest-path tests/fixtures/doc-examples/Cargo.toml`.

### Notification counters and shutdown

The service publishes through `qubit-event-bus`'s `NotificationPublisher`: one serial publisher thread and a bounded queue of 256 entries by default (`event_bus_buffer_capacity(NonZeroUsize)`). State transitions call `try_publish` and never wait on the bus; when the queue is full the new event is dropped, and events attempted after shutdown has closed the queue are dropped too. Neither changes the task result. Publishing runs on a dedicated OS thread, so a synchronous provider never occupies Tokio workers.

`notification_stats()` returns `Some(TaskEventNotificationStats)` when a bus is configured:

| Counter | Meaning |
| --- | --- |
| `enqueued` | Events accepted into the local queue. |
| `queue_full`, `queue_closed` | Events dropped at the queue boundary. |
| `accepted` | Receipts with at least one accepting destination; `partial_rejection` counts those that also had a rejection. |
| `opaque_accepted` | Provider accepted without exposing destinations (Redis). |
| `unaccepted` | Receipts with no accepting destination, including empty destination lists and interceptor drops. |
| `publish_error` | Publish calls that returned an error. |
| `worker_panicked` | The publisher thread panicked; queued events may be lost. |

These are admission and worker counters, not proof that a subscriber ran. They are monotonic, saturate at `u64::MAX`, and the fields of one snapshot are not from the same instant.

`shutdown()` closes notification enqueue after accepted work has settled, then drains the queue. It waits at most 30 seconds for the publisher thread by default (`event_bus_close_timeout(Duration)`); on timeout, or if the thread panicked or its join failed, `shutdown()` returns `TaskServiceError::NotificationClose` while the thread keeps draining what it already holds. Concurrent and later `shutdown` callers receive the same stored result. The service does not shut down the application-owned bus; do that after the service.

## Assemble components with `qubit-spi`

`TaskExecutionServiceBuilder::in_memory()` and `recoverable_sqlite()` are presets over three components: a `TaskStore` (acceptance, reads, transitions), a `SchedulingPolicy` (which queued task runs next), and a `TaskExecutionEngine` (resource reservation and execution). `from_components(store, engine, policy)` accepts any implementations:

```rust
use std::sync::Arc;

use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::scheduling::SchedulingPolicy;
use qubit_task::{TaskExecutionEngine, TaskStore};

pub fn assemble(
    store: Arc<dyn TaskStore>,
    engine: Arc<dyn TaskExecutionEngine>,
    policy: Arc<dyn SchedulingPolicy>,
) -> TaskExecutionServiceBuilder {
    TaskExecutionServiceBuilder::from_components(store, engine, policy)
}
```

`qubit-task` defines SPI service families for all four extension points (`TaskStoreSpec`, `SchedulingPolicySpec`, `TaskExecutionEngineSpec`, `TaskHandlerSpec`) in `qubit_task::spi`. The built-in components have stable provider IDs there: `MEMORY_STORE_PROVIDER_ID` (`qubit.task.store.memory`), `SQLITE_STORE_PROVIDER_ID` (`qubit.task.store.sqlite`), `FAIR_FIFO_PROVIDER_ID` (`qubit.task.scheduler.fair-fifo`), and `LOCAL_ENGINE_PROVIDER_ID` (`qubit.task.engine.local`). Enable the `inventory` feature to collect provider registrations from linked crates through the `discovered_*_registry()` functions. The application still chooses a provider, supplies its configuration, and creates the `Arc<dyn ...>`; discovery does not guess database paths, credentials, or capacity. Reject provider ID conflicts and duplicate handler keys before the service accepts traffic. A service keeps its components for its lifetime.

Implementers of a third-party `TaskStore` must provide `count_states()` as one aggregate, `get_summary` without reading payload bytes, `has_unfinished_over_limit(limit)` without decoding payloads, `transition` returning `TaskSummary`, and summary-based `list` pages; `prune_terminal_before` and `abandon_blocked` may report `UnsupportedCapability`. A `TaskExecutionEngine::try_prepare` is synchronous and must reserve resources promptly without waiting or running handler work; `activate` starts the handler after the service has recorded the attempt as running and must return a trackable execution handle whenever it starts work. The [detailed design](task_execution_service_design.en.md) describes these contracts.

A successful `TaskStore::release_owner(epoch)` is a completion barrier: every write previously admitted under that owner has finished, even if its caller dropped the write future. No old-owner write may commit after release; prevent a new owner from overlapping unfinished old writes. An error is not evidence of a safe handoff. Aggregate counts represent one store consistency boundary, but may already be stale by the time the future returns.

For explicit built-in SPI selection and runnable component assembly, see [`spi_selection.rs`](../tests/fixtures/doc-examples/src/bin/spi_selection.rs). Run it with `cargo run --locked --manifest-path tests/fixtures/doc-examples/Cargo.toml --bin spi_selection`. Linked external-provider registration is exercised by the provider and consumer fixtures.

## Lifecycle and shutdown

Startup order: create the bus and subscriptions if used, build the service (which recovers stored work), then open the business entry points. Shutdown order: stop accepting business requests, shut the service down, then cancel event subscriptions and shut down the bus.

```rust
use std::time::Duration;

use qubit_task::TaskExecutionService;
use qubit_task::service::TaskServiceError;
use tokio::time::Instant;

pub async fn stop(tasks: &TaskExecutionService) -> Result<(), Box<dyn std::error::Error>> {
    match tasks.shutdown_until(Instant::now() + Duration::from_secs(30)).await {
        Ok(()) => Ok(()),
        Err(TaskServiceError::ShutdownTimedOut) => {
            eprintln!("accepted imports are still draining in the background");
            Err(TaskServiceError::ShutdownTimedOut.into())
        }
        Err(TaskServiceError::NotificationClose(reason)) => {
            eprintln!("notifications did not close cleanly: {reason}");
            Err(TaskServiceError::NotificationClose(reason).into())
        }
        Err(error) => Err(error.into()),
    }
}
```

`shutdown()` rejects new writes with `ShuttingDown`, waits for in-flight submissions to finish acceptance, waits for running attempts and the scheduler to settle, releases store ownership, and then drains notifications. `Ok(())` means all of that completed; the SQLite file can then be opened by the next process. `shutdown_until(deadline)` starts the same drain but bounds only this caller's wait; `ShutdownTimedOut` means the drain continues in the background and ownership has not been released yet. Cancellation is still cooperative during shutdown: a handler that ignores its flag holds the drain. Dropping the last service handle also starts an asynchronous drain, but nobody observes its result; call `shutdown()` when completion matters.

`TaskExecutionServiceBuilder::runtime_handle(Handle)` selects the runtime for service-owned background tasks; keep that runtime alive until shutdown or the drain has finished. Cancelling the future that awaits `build()` does not stop the background construction worker: it stops at a recovery page boundary, releases any owner it acquired, and does not start the scheduler.

Two failures change the service permanently. A store failure sets the service to `StoreUnavailable`: new writes fail, `wait` and local handles receive the error at once, and the shared shutdown result still waits for the scheduler and tracked executions before releasing ownership. A scheduler panic or `EngineError::Closed` from `try_prepare` sets `SchedulerUnavailable`; queued records stay in the store for the next start, and the scheduler is not restarted. `last_store_error()` and `last_scheduler_error()` expose the retained diagnostics. If a custom engine panics after starting untracked side effects, terminate and restart the process through the application supervisor.

## Errors, diagnostics, and troubleshooting

| Symptom | What to check |
| --- | --- |
| `submit` returns `QueueFull` | The waiting queue is full, or a restart backlog is still draining. Apply backpressure (HTTP 429) and retry with the same key. Raise `queue_capacity` only if the deployment can hold more pending work. |
| `submit` returns `Unsatisfiable` | The request asks for more than the configured `ResourceCapacity`, for example a GPU label no device carries. Fix the request or the capacity. |
| `submit` returns `InvalidRequest` | An empty task type or handler version, an oversized field, or an invalid resource description. The message names the limit. |
| `submit` returns `Store(IdempotencyConflict)` | The key was reused with a different request. Generate a new key for new work. |
| `submit` returns `MissingHandler` | No handler with that exact `(task_type, handler_version)` was registered before `build()`. |
| `submit_local` returns `UnsupportedCapability` | The store declares restart recovery. Use `TaskRequest` with a registered handler instead. |
| A task stays `Blocked` after a restart | Read the reason in `get_summary`; usually a missing handler or an exhausted attempt budget. Register the handler and `retry_blocked`, or `abandon_blocked`. |
| `retry_blocked` returns `AttemptsExhausted` | The task has used `max_attempts` across restarts. Submit a new task for a fresh budget. |
| A running task does not stop after `cancel` | Cancellation is cooperative. The handler must check `TaskContext::is_cancelled()` and return `TaskRunOutcome::Cancelled`. |
| `wait` returns `Blocked` | The task needs intervention; it will not finish on its own. |
| `build()` fails with `RecoveryCapacityExceeded` | More unfinished records than `queue_capacity + max_running_tasks`. Raise one limit; the records are intact. |
| `build()` fails with `SqliteFeatureDisabled` or a store error | Enable the `sqlite` feature; check the path and that no other process owns the database. There is no fallback to memory. |
| `StoreUnavailable` or `SchedulerUnavailable` everywhere | The service is permanently degraded. Read `last_store_error()` / `last_scheduler_error()`, shut down, fix the cause, restart. |
| No status events arrive | Check `notification_stats()` for `queue_full`, `publish_error`, or `worker_panicked`; confirm the subscriber's topic is `task.lifecycle`. Events are best effort; query the service for authoritative state. |
| `shutdown()` returns `NotificationClose` | The publisher thread did not finish within `event_bus_close_timeout`, or it panicked. Task state is unaffected. |

Persisted diagnostics are bounded: categories to 128 bytes, messages and blocked reasons to 4,096 bytes, truncated at a UTF-8 boundary. Log the original error with the task ID, attempt number, and tenant in the application's own logs.

## Migration from 0.5 and earlier

0.6 removed caller-supplied task IDs, `submit` with closures, thread-pool-specific builder settings, and the old `TaskHandle<R, E>`. Process-local closures now use `submit_local` and receive a `LocalTaskHandle<R, E>`; reconstructable work uses `TaskRequest` with a stable idempotency key, and the service generates the `TaskId`. There is no generic durable handle.

Query and extension contracts also changed: `TaskQuery.states` is `Vec<TaskStateKind>`; history cursors are `TaskCursor { accepted_at_ms, id }`; `SchedulingPolicy` implementations receive `QueuedTask.resources` instead of a full request; `TaskStore` gained `count_states()`, `get_summary()`, `has_unfinished_over_limit(limit)`, `prune_terminal_before`, and `abandon_blocked`, and `transition` returns `TaskSummary`. `max_attempts` now counts starts across process restarts, so a recovered record at the limit becomes `Blocked` instead of running again. Service writes are rejected after shutdown starts, and SQLite writes are fenced by store ownership. Update call sites and custom stores together, then run the application's compile and recovery tests.

| Earlier contract | Current contract |
| --- | --- |
| Submission-only concurrency limit | All six writes share `max_inflight_operations`; saturation returns `OperationLimitExceeded` |
| `StoredTaskPage<StoredTask>` | Payload-free `RecoveryPage<TaskSummary>` |
| Owner release without a drain contract | `release_owner` is a completion barrier for admitted writes |

This line uses Event Bus 0.16 and Redis adapter 0.4. The bounded `NotificationPublisher` and `AdmissionOutcome` APIs used by task notifications already exist in Event Bus 0.14; that integration needs no call-site migration solely for the version bump. Check provider-specific release notes when upgrading adapters.

## Boundaries and a practice checklist

- The service schedules within one process. Multi-node leasing, distributed resource discovery, workflow dependencies, cron scheduling, forced interruption of arbitrary code, and exactly-once business side effects are out of scope. A future distributed engine can implement the same `TaskExecutionEngine` boundary without changing the facade.
- Recovery and retries are at-least-once. Make handlers idempotent or protect side effects with the application's own transactions before marking an error retryable.
- Keep payloads small: parameters and references, not files. Limits are 16 MiB per payload, 64 MiB of in-flight payload, 64 in-flight write operations, and, for the memory store, 64 MiB of retained payloads. Text limits in UTF-8 bytes: `task_type` 128, `handler_version` 64, keys 256, metadata 32 entries with 128-byte keys, 4,096-byte values, and 16,384 bytes combined.
- Store results in the application database and return a bounded `TaskOutput` summary or reference.
- Generate the idempotency key before the first `submit` and keep it until the task record is no longer needed.
- Register every handler version the store may still hold before `build()`; a missing version blocks the task rather than losing it.
- Poll with `get_summary`, not `get`, and page `list` at 256 or fewer. Prune SQLite history on a schedule and review `Blocked` records separately.
- Treat a publish receipt, an event, and a task transition as different stages. Query the service for authoritative state.
- Test the full path: accept, run, retry, block, cancel, restart with recovery, and shutdown under a deadline.

## Further reading

- [README](../README.md) · [Detailed design](task_execution_service_design.en.md) · [API reference](https://docs.rs/qubit-task)
- [`examples/task_service.rs`](../examples/task_service.rs) · [`examples/blocked_maintenance.rs`](../examples/blocked_maintenance.rs)
