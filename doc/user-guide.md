# qubit-task User Guide

[中文版本](user-guide.zh_CN.md)

This guide covers `qubit-task` 0.6.x on Rust 1.94 or later. The crate accepts
work that cannot finish during the caller's request, schedules it against
resource budgets, and lets the application inspect its progress later.

## Choose a storage guarantee

There is one public `TaskExecutionService` facade. The selected `TaskStore`
determines whether history survives restart and whether accepted work can be
recovered.

| Setup | Completed history | Accepted unfinished work after restart |
| --- | --- | --- |
| `TaskExecutionService::in_memory()` | Bounded memory history | Lost when the process exits |
| Custom `TaskStore` with persistent history | Persistent | Depends on the store's declared recovery capability |
| `recoverable_sqlite(path)` | SQLite | Queued work is restored; interrupted running work may run again |

Recovery provides at-least-once execution. A handler can have performed an
external side effect before a process exits, so handlers should use idempotency
keys or their own transaction protocol when repeating that effect is unsafe.

## Run local work in memory

Add the crate and an async runtime to the application:

~~~toml
[dependencies]
qubit-task = "0.6"
tokio = { version = "1.53", features = ["macros", "rt-multi-thread"] }
~~~

`in_memory()` makes the loss and retention behavior visible in the call site.
It uses local execution, available CPU parallelism (or one slot), a waiting
queue of 1024 tasks, and a terminal history of 1024 tasks. It does not probe for
GPUs. `submit_local` accepts an in-process closure and returns a typed
`LocalTaskHandle<R, E>`; closures run on Tokio's blocking pool. The handle gives
the closure's in-process value or original error, while `TaskRecord.output`
retains only the small `TaskOutput` summary. Custom async handlers must move
long CPU-bound or blocking work off async runtime workers themselves.

~~~rust,no_run
use qubit_task::TaskExecutionService;
use qubit_task::model::TaskOutput;
use qubit_task::service::LocalTaskOutcome;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let service = TaskExecutionService::in_memory().await?;
    let handle = service.submit_local(|context| {
        assert_eq!(context.attempt(), 1);
        LocalTaskOutcome::<usize, std::io::Error>::Succeeded {
            value: 21_usize,
            summary: TaskOutput { summary: b"imported 21 rows".to_vec() },
        }
    }).await?;
    let imported_rows = handle.result().await??;
    assert_eq!(imported_rows, 21);
    service.shutdown().await?;
    Ok(())
}
~~~

`submit_local` is intentionally unavailable when the store declares restart
recovery. A closure cannot be reconstructed from a database after process exit.
For a cooperative cancellation, return `LocalTaskOutcome::Cancelled` after
observing `TaskContext::is_cancelled()`; then `handle.result()` returns
`LocalTaskResultError::Cancelled`. A successful `TaskRunOutcome` or
`TaskRecord.output` is not replaced by a late cancellation request.

## Register a versioned handler

Use `TaskRequest` for work that can be reconstructed. The request stores a task
type, exact handler version, opaque payload, resource demand, and optional
correlation and idempotency keys. The handler owns payload decoding, so a stored
request is never silently handed to a newer handler version.

Implement `TaskHandler` and register its `Arc` before calling the asynchronous
builder `build()` method. Duplicate `(task_type, version)` registrations are
rejected. A missing handler discovered during recovery becomes `Blocked` and
remains queryable until the handler is installed and `retry_blocked` is called.

~~~rust,no_run
use std::sync::Arc;
use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::handler::{TaskContext, TaskHandler, TaskHandlerDescriptor, TaskRunOutcome, TaskRunResult};
use qubit_task::model::{TaskOutput, TaskRequest};
use qubit_task::store::TaskFuture;

struct ImportV1;
impl TaskHandler for ImportV1 {
    fn descriptor(&self) -> TaskHandlerDescriptor {
        TaskHandlerDescriptor { task_type: "csv-import".into(), version: "1".into() }
    }

    fn run<'a>(&'a self, payload: &'a [u8], _context: TaskContext)
        -> TaskFuture<'a, TaskRunResult>
    {
        Box::pin(async move {
            Ok(TaskRunOutcome::Succeeded(TaskOutput {
                summary: format!("accepted {} bytes", payload.len()).into_bytes(),
            }))
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let service = TaskExecutionServiceBuilder::in_memory()
        .register_handler(Arc::new(ImportV1))?
        .build().await?;
    let id = service.submit(TaskRequest::new("csv-import", "1", b"...".to_vec())).await?.id;
    let finished = service.wait(id).await?;
    assert!(finished.state.is_terminal());
    service.shutdown().await?;
    Ok(())
}
~~~

`TaskOutput` is a small persisted summary or reference. Store large results in
the business application's own data store and return a bounded reference.
Use `TaskRequest` with an exact handler version for work that must be
reconstructed after restart; the existing SQLite restart-recovery test exercises
that public service path.

## Schedule CPU, GPU, and named resources

The builder accepts an explicit `ResourceCapacity`. CPU slots are concurrency
budgets, not operating-system CPU pinning. GPU devices and labels must be
provided by deployment configuration. Custom integer budgets can represent
memory units, licenses, or other exclusive quotas when the deployment defines
those units consistently.

~~~rust,no_run
use std::collections::BTreeMap;
use qubit_task::model::ResourceCapacity;
use qubit_task::service::TaskExecutionServiceBuilder;

let capacity = ResourceCapacity {
    cpu_slots: 8,
    gpus: BTreeMap::from([
        ("gpu-0".into(), vec!["cuda".into()]),
        ("gpu-1".into(), vec!["cuda".into()]),
    ]),
    custom: BTreeMap::from([("memory_mib".into(), 32_768)]),
};
let builder = TaskExecutionServiceBuilder::in_memory().capacity(capacity);
~~~

Every task request is checked against the capacity reported by the execution
engine. A request that exceeds configured capacity is rejected; a valid request
without currently free resources stays queued. The default fair FIFO policy can let a fitting task
pass a blocked head task, then protects a repeatedly bypassed task after a
bounded number of passes. The queue itself remains bounded; a full queue returns
`QueueFull` so the caller can apply backpressure. A retry that finds the queue
full is stored as `Blocked` and can be explicitly retried after capacity is
available; it never exceeds the queue limit.

## Enable restart recovery with SQLite

Enable the optional feature and configure a durable path:

~~~toml
[dependencies]
qubit-task = { version = "0.6", features = ["sqlite"] }
~~~

~~~rust,ignore
let service = TaskExecutionServiceBuilder::recoverable_sqlite("./state/tasks.sqlite")?
    .register_handler(Arc::new(ImportV1))?
    .require_recovery(true)
    .build().await?;
~~~

SQLite stores accepted task descriptions and state changes transactionally. An
operating-system lock prevents two service processes from executing the same
database at once. The builder acquires ownership and scans unfinished work
before returning. A database lock conflict, missing provider, or unsupported
recovery capability fails startup; there is no fallback to volatile memory. If
a recovered task has no registered handler, it remains stored in the `Blocked`
state and service construction succeeds.

`capabilities()` reports `persistent_history` and `restart_recovery` for the
store that was actually assembled. Persistent history without recovery is a
valid combination for a third-party store. Every `TaskStore` implementation
must implement `count_states()` as one aggregate over its retained records;
`stats()` calls it once, and propagates its error. The task counts and engine
resource snapshot are collected one after the other, so they are adjacent
snapshots rather than one atomic view. Their cost is one aggregate query,
independent of history page count.

## Assemble components with `qubit-spi`

`qubit-task` defines SPI service families for `TaskStore`,
`SchedulingPolicy`, `TaskExecutionEngine`, and `TaskHandler`. Enable the
`inventory` feature to collect provider registrations from linked crates.
Applications choose a provider and pass its created `Arc<dyn ...>` into
`TaskExecutionServiceBuilder::from_components`; provider discovery does not
guess database credentials, filesystem locations, or resource capacity.

The built-in volatile store, fair FIFO policy, and local execution engine have
stable provider IDs exposed from `qubit_task::spi`. Third-party providers use
the corresponding public `*Spec` and `qubit_spi::ServiceProvider` contract.
Application assembly should reject provider ID conflicts and duplicate
handler type/version keys before the service accepts traffic. A service keeps
its selected components for its lifetime.

## Publish status changes

With the `event-bus` feature, inject the concrete `qubit_event_bus::EventBus`
into the builder. The service publishes `TaskEvent` values after state changes.
Publishing is best effort: a publish error does not roll back a task transition.
Events may be repeated, delayed, or missing, so consumers should compare
`state_version` and query the service for authoritative state.
This release pins `qubit-event-bus` 0.12 to revision
`319fffb85c150c0d2b2f83655ee06799b19ef035`; the publisher classifies that
revision's `PublishAcknowledgement` directly and does not depend on newer
admission-check APIs.

The service owns one serial publisher thread and a bounded notification queue.
The default capacity is 256; configure another positive capacity with
`event_bus_buffer_capacity(NonZeroUsize)`. State transitions call `try_send`,
so they do not wait for event-bus publication. If the queue is full, the new
notification is dropped. Notifications attempted after shutdown closes the
queue are also dropped. Neither case changes the task result.

When the service has an event bus, `notification_stats()` returns a snapshot of
the publisher counters; otherwise it returns `None`. `enqueued` counts events
accepted by the local queue, while `queue_full` and `queue_closed` count events
dropped at that queue boundary. `accepted` counts publications for which at
least one reported destination accepted the event; `partial_rejection` counts
those that also had a rejected destination. `opaque_accepted` counts provider
acceptance when destinations are not exposed. `unaccepted` counts receipts with
no reported accepting destination, including empty destination lists and
interceptor drops. `publish_error` counts calls returning an error, and
`worker_panicked` records a publisher-thread panic. These are admission and
worker counters, not evidence that a subscriber handler completed. Counters
are monotonic and saturate at `u64::MAX`; the fields in one snapshot need not
represent the exact same instant.

`shutdown()` closes new admission, waits for in-flight submissions to finish
acceptance, then waits for accepted task work to settle. It closes notification
enqueue, then drains notifications already in the queue before returning. It
does not shut down the application-owned event bus. Publication runs on a
dedicated OS thread, keeping a synchronous provider off Tokio runtime workers;
however, a synchronous provider that never returns can keep that thread busy
and make `shutdown()` wait indefinitely. Dropping the service without calling
`shutdown()` closes the sender and lets the worker drain queued notifications
before exiting, subject to the same provider behavior. If the worker panics,
`worker_panicked` records it and shutdown still observes worker completion, but
notifications remaining in its queue may be lost.

## Query, cancel, and retry

Use `get(TaskId)` for the current record and `list(TaskQuery)` for bounded
history pages. `wait(TaskId)` resolves when the task is terminal and returns a
blocked-task error when intervention is needed. `cancel(TaskId)` can cancel a
queued task immediately. For running work it persists `cancel_requested` and
sets the cancellation signal in `TaskContext`; this is only a request. The
handler must return `TaskRunOutcome::Cancelled` for the service to confirm
cancellation as `TaskState::Cancelled`. If it returns success or failure, that
result remains authoritative. This distinction is also exercised by the
cooperative cancellation integration tests.

Handlers return a `TaskRunError` with a category, diagnostic, and retryable
flag. Non-retryable errors become `Failed`; panics become `Panicked`. Retryable
errors are retried up to the configured maximum (three attempts by default).
If the bounded waiting queue is full when a retry is due, the task becomes
`Blocked` with a queue-capacity reason instead of exceeding the limit. After
capacity becomes available, call `retry_blocked` to enqueue it again.

## Migration from 0.5 and earlier's previous API

This redesign removes caller-supplied IDs, `submit` closures,
thread-pool-specific builder settings, and the old `TaskHandle<R, E>` API. There
is no generic durable handle: `submit_local` now returns
`LocalTaskHandle<R, E>` only for process-local closures, and `TaskRequest` plus
`TaskId` remains the interface for reconstructable work. Third-party
`TaskStore` implementations must add `count_states()` and return all retained
state counts in one aggregate operation. These are intentional source-breaking
changes; update downstream implementations and call sites together. No current
`rs-*` crate in this workspace consumes `rs-task` directly.

## Operational limits

This release schedules tasks within one service process. It does not provide
multi-node leasing, distributed resource discovery, workflow dependencies,
cron scheduling, forced interruption of arbitrary code, or exactly-once
business side effects. A future distributed engine can implement the same
`TaskExecutionEngine` boundary without changing the service facade.
