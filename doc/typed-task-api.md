# Typed task API

[简体中文](typed-task-api.zh_CN.md) · [User guide](user-guide.md)

The 0.12 typed API binds a payload model to a task kind, schema version, and codec before submission. A handler descriptor declares the model and schema versions it accepts; registration checks the payload's `HasModelId` against that descriptor. A codec can support multiple schemas, while compatibility belongs to the handler descriptor.

`TaskKind<T>` derives its model identity from `T: HasModelId` and creates both the descriptor and typed requests. `TaskRequest<T>::encode` resolves a `ValueBytesCodecDescriptor` from `ValueBytesCodecRegistry` and creates an `EncodedPayload<T>`; storage receives its type-erased `StoredPayload`. The bytes registry uses `ValueEncoder<T>` and `ValueDecoder<[u8]>`. Task metadata uses `rs-metadata::Metadata` and is limited to 32 entries and 16 KiB serialized bytes by the task request, in addition to `rs-metadata`'s wire budgets.

The 16 KiB metadata output budget is enforced while JSON is encoded. When `TaskRequestEncodeError::MetadataTooLarge(n)` is returned, `n` is a known lower bound (currently 16,385), not the exact complete encoded size. Callers should use it to identify an over-limit request and must not present it as the exact original size. The enum shape, successful encoding path, and `StoredTaskRequest` wire representation are unchanged; this lower-bound diagnostic is a deliberate error-semantics change.

## End-to-end outline

This example follows a typed request through codec registration, handler execution, acceptance, and terminal-state observation. It uses the in-memory builder for a standalone process; applications that need durable recovery provide a `TaskStore` and ID generator.

```rust,no_run
// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::error::Error;
use std::time::Duration;

use qubit_codec::ValueBytesCodecDescriptor;
use qubit_codec::ValueBytesCodecRegistration;
use qubit_codec::ValueBytesCodecRegistry;
use qubit_codec::ValueCodecId;
use qubit_codec::ValueCodecRegistration;
use qubit_codec::ValueCodecRegistrationSource;
use qubit_codec::ValueDecoder;
use qubit_codec::ValueEncoder;
use qubit_model_id::HasModelId;
use qubit_model_id::ModelId;
use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::TaskKind;
use qubit_task::TaskState;
use qubit_task::handler::TaskContext;
use qubit_task::handler::TaskRunOutcome;
use qubit_task::handler::TaskRunResult;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Deserialize, Serialize)]
struct EchoPayload {
    message: String,
}

impl HasModelId for EchoPayload {
    const MODEL_ID: ModelId = ModelId::new("example.EchoPayload");
}

#[derive(Default)]
struct JsonCodec;

impl ValueEncoder<EchoPayload> for JsonCodec {
    type Output = Vec<u8>;
    type Error = serde_json::Error;

    fn encode(&mut self, value: &EchoPayload) -> Result<Self::Output, Self::Error> {
        serde_json::to_vec(value)
    }
}

impl ValueDecoder<[u8]> for JsonCodec {
    type Output = EchoPayload;
    type Error = serde_json::Error;

    fn decode(&mut self, bytes: &[u8]) -> Result<Self::Output, Self::Error> {
        serde_json::from_slice(bytes)
    }
}

static JSON_DESCRIPTOR: ValueBytesCodecDescriptor =
    ValueBytesCodecDescriptor::of::<JsonCodec, EchoPayload>();
static JSON_CODEC: ValueBytesCodecRegistration = ValueCodecRegistration::new(
    ValueCodecId::new("example.echo.json"),
    &JSON_DESCRIPTOR,
    ValueCodecRegistrationSource::new("example", "rs-task", "readme_quick_start", 1),
);

async fn run_echo(payload: EchoPayload, _context: TaskContext) -> TaskRunResult {
    println!("{}", payload.message);
    Ok(TaskRunOutcome::success())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let codecs = ValueBytesCodecRegistry::from_registrations([&JSON_CODEC])?;
    let echo = TaskKind::<EchoPayload>::new(
        "example.echo",
        1,
        ValueCodecId::new("example.echo.json"),
    );
    let mut builder = TaskExecutionServiceBuilder::in_memory(256, codecs);
    builder.register_handler_fn(echo.descriptor(), run_echo)?;

    let service = builder.build().await?;
    let accepted = service
        .submit(echo.request(EchoPayload {
            message: "typed task completed".into(),
        }))
        .await?;
    println!("accepted={} task={}", accepted.created, accepted.summary.id);

    let terminal = service
        .wait_terminal(accepted.summary.id, Duration::from_secs(5))
        .await?;
    assert_eq!(terminal.state, TaskState::Succeeded);
    println!("task {} is {:?}", terminal.id, terminal.state);

    service.shutdown().await?;
    Ok(())
}
```

## Runtime contracts

`TaskStore` is the public typed persistence contract. `MemoryTaskStore` and `SqliteTaskStore` implement it directly; the former UUID `LegacyTaskStore` adapter and its `RecoveryPage`/`scan_unfinished` API are not part of the current public contract. Open SQLite with `SqliteTaskStore::open_async(path).await`. Typed schema v4 through v7 migrate in place and transactionally to v8, preserving tasks, active outbox rows, and quarantine records while adding terminal state checkpoints. Legacy UUID schemas are rejected without modification.

`TaskExecutionService::get_summary(id)` and typed history queries return `TaskSummary`, which contains lifecycle, routing, metadata, resource, and progress information without loading or decoding the stored payload bytes. `get_state_checkpoint(id)` is a smaller recovery read: it returns only the current state and version needed to repair a consumer projection, including after a terminal SQLite row has been pruned. A checkpoint is not a task summary and cannot restore payload, progress, or intermediate transitions.

- `TaskId` wraps `rs-id::Id`; the service requires an injected `IdGenerator` that produces IDs unique across process restarts. Snowflake cross-process uniqueness depends on distinct node IDs and clock conditions. `to_padded_decimal()` produces a fixed-width decimal key for stable lexical database ordering.
- CPU slots, GPU devices and labels, optional memory/disk bytes, and custom integer units are admission quotas reserved for each attempt. They account for concurrent reservations; they do not pin CPUs, discover or isolate GPUs at the OS level, or enforce actual process memory/disk usage. A request above configured capacity is unsatisfiable. A temporarily unavailable request stays queued while the scheduler considers later resource-compatible work, so ordering is not strict FIFO.
- Queued or blocked tasks can be cancelled directly. For running work, `CancellationMode::Cooperative` signals `TaskContext::is_cancelled()` and the handler must stop at a safe boundary and return `TaskRunOutcome::Cancelled`. `ExternalHook` requires a registered external hook. `Unsupported` cannot stop a running attempt.
- `max_running_tasks` bounds active handlers and `scan_page_size` bounds queued-summary scans. The scheduler keeps queued work in the store and creates execution tasks only when it has a running slot.
- Synchronous payload codec work runs on Tokio's blocking pool: `submit()` encodes before acceptance, and a started attempt decodes before its async handler runs. Cancelling before the encoded request is handed to the store stops acceptance, but does not stop an encode already running; the completed bytes may be discarded. After acceptance is dispatched, the service owns the store operation and scheduler/notification wakeup. Cancelling or timing out the caller's `submit()` future does not cancel that operation, so the task may already be accepted or may be accepted later; shutdown waits for it. Use a stable idempotency key on the first submit and retry the identical request to retrieve the same task. If its ID is known, use `get_summary()` to check it; use `request_cancel()` to request cancellation. A running attempt keeps its slot and resource reservation while decoding, and shutdown waits for preparation as well as the handler. A decoder panic is recorded as that task's `Panicked` outcome when panic unwinding is enabled. Blocking work cannot be forcibly cancelled or given a hard timeout, so a stuck codec can delay shutdown and runtime teardown.
- A handler's `TaskRunError.retryable` controls retries. The default `max_attempts` is three, with `RetryPolicy` delays from one to sixty seconds. Positive retry delays are rounded up to whole milliseconds when persisted; this adds less than one millisecond to the policy delay and can put the stored interval slightly above `max_delay`. It does not promise the same wall-clock wait. The queued state and `retry_not_before_ms` are stored together, so restart recovery honors the deadline. Non-retryable failures, panics, cancellation, and exhausted attempts are terminal.
- `resume_blocked(&observed_summary)` requeues a blocked task after the caller repairs configuration and restarts with a compatible handler or codec. Read the latest summary before retrying a state-version conflict. A pending cancellation prevents resumption.
- Background store failures are latched: writes and scheduling stop, diagnostic reads remain available, and `shutdown()` returns the stored failure after draining active work. SQLite typed schema versions 4 through 7 migrate in place and transactionally to version 8; legacy UUID schemas still require explicit data mapping.
- `TaskContext::progress_builder()` uses `rs-progress::AsyncReporter`. `report_async()` awaits persistence before returning. Current stage and metric snapshots appear on later task reads without changing lifecycle `state_version`; reporting errors are returned to the handler. The reporter allocates its next progress version before awaiting the store and never reuses it after a failed or cancelled report. The asynchronous store may already have committed when its caller is cancelled, so subsequent versions can contain gaps; progress versions are independent of lifecycle `state_version`.
- Each running attempt starts with an empty progress baseline at version `0`; the first `TaskStore::update_progress` command must use a positive version (normally `1`). Versions must increase strictly within the attempt, but may skip values. A direct `MemoryTaskStore` caller that previously wrote version `0` must start at `1`; the built-in reporter already does this.
- Typed history pages sort ascending by `(accepted_at_ms, numeric task id)`. `after` is an exclusive key cursor, not an offset; each query observes its own storage snapshot. `next` is present only when a lookahead row exists. Filters include state, business `category`, and correlation key. `kind_id` routes to a handler and is independent of `category`.

Before persisting terminal-state diagnostics, the service limits `Failed.category` to 128 UTF-8 bytes and other terminal-state diagnostic fields to 4096 UTF-8 bytes. Over-limit text is shortened at a UTF-8 boundary to the longest prefix that fits with an appended `...`. Direct `TaskStore` writes still reject over-limit diagnostics. External cancellation hook messages returned by the service are also limited to 4096 UTF-8 bytes; direct transitions with a longer `cancel_error` return `InvalidRequest`. Existing SQLite rows with longer hook diagnostics remain readable and are normalized by service recovery or finalization when the row is next written. Store the full hook message in an application diagnostic system if it is needed. With panic unwinding enabled, a panic from an application payload decoder while preparing a task becomes that task's `Panicked` outcome; handler panics remain task-local as before. `panic=abort` cannot be caught.

For durable lifecycle notifications, enable `event-bus`, register an application `TaskEvent` codec, and set `TaskExecutionServiceBuilder::event_bus`. The task state and SQLite outbox row are committed atomically, then replayed asynchronously. Until the provider reports `Accepted`, the row is retained and retried, so publication may be duplicated. After `Accepted`, the row is deleted; acceptance does not guarantee Redis persistence or consumer receipt, so notification delivery to consumers is not guaranteed at least once. This is distinct from at-least-once task handler execution. Deduplicate by `(TaskId, state_version)` and use task queries as the authority. Existing lifecycle states are not backfilled when the publisher is enabled. `MemoryTaskStore` does not support a persistent outbox. See the [user guide](user-guide.md#publish-lifecycle-changes) for setup boundaries and monitoring guidance.

Call `TaskStore::prune_terminal_before(finished_before_ms, max_rows)` from an application-owned maintenance job to remove at most `max_rows` terminal records whose finish time is strictly before the cutoff. SQLite skips tasks with active outbox rows and atomically saves a minimal terminal state checkpoint before deleting each task. Checkpoints restore only current state, are not automatically expired, and grow with pruned tasks. The store orders candidates by finish time and task ID and removes each idempotency key in the same operation. Pruned idempotency keys may be reused, but the SQLite `TaskId` remains reserved while its checkpoint exists; `MemoryTaskStore` releases the ID when pruning. Queued and blocked tasks are never pruned.

For consumer version gaps, call `TaskExecutionService::get_state_checkpoint(id)`. It returns the current task state while the task is retained and the terminal checkpoint after SQLite pruning. `None`, a query error, or a checkpoint older than the notification must leave the delivery unacknowledged and on the Retry path. A checkpoint repairs a current-state projection; it does not replay omitted transitions or their side effects.

## 0.12 service, queries, cancellation, and stores

`submit` returns `AcceptOutcome`, which carries the accepted `TaskSummary` and a `created` flag from the atomic store operation. An idempotency hit returns the existing summary with `created == false`. `submit_checked` performs the same acceptance only after validating the current handler, model ID, schema version, and codec. Those checks run before ID generation and acceptance, so a strict failure has no store or notification side effect. Ordinary `submit` intentionally accepts a request with an invalid route; the scheduler records it as `Blocked`. Encoding errors remain structured as `TaskServiceError::RequestEncoding(TaskRequestEncodeError)`.

Use `get_summary(id)` when the caller needs lifecycle, metadata, and progress; use `get_state_checkpoint(id)` when reconciling only current state. `wait_terminal(id, timeout)` reads checkpoints until it sees Succeeded, Failed, Panicked, or Cancelled. Blocked is not terminal. The timeout covers reads and sleeps together; a zero timeout allows a checkpoint read that is immediately ready. SQLite keeps terminal checkpoints after pruning task rows, so this API can still read their final state.

`TaskQuery::default()` requests 50 records. Its fields are private; use `with_state`, `with_states`, `with_category`, `with_correlation_key`, `after`, and the read-only getters. `page_size(n)` accepts only 1 through 256. `service.pages(query)` creates a `TaskPages` iterator whose `next_page()` returns one page at a time. A failed read leaves its cursor unchanged for retry, and the last nonempty page is returned before `None`.

`request_cancel(id)` expresses a request, not a completed outcome. Queued and Blocked tasks can return `CancelledBeforeRun`; cooperative running tasks return `CancellationRequested` and receive a read-only `TaskCancellation` handle from `TaskContext::cancellation()`. `is_requested()` observes a persisted request. For external cancellation, the service publishes that intent to the handle after persisting it and before invoking the hook; hook failure does not clear the intent. `wait_requested()` waits only for cooperative cancellation, so external requests are queryable but do not complete that future. Unsupported running cancellation returns `CancellationUnsupported`. External hooks receive `TaskAttempt`, and concurrent retries for that same attempt share the service-owned hook operation.

Pass the exact summary observed by an operator to `resume_blocked(&summary)`. The service rereads and conditionally updates the current record, so stale snapshots produce a conflict rather than overwriting a newer state. Notification isolation similarly takes a `&NotificationFailure` snapshot and compares its task ID, state version, and event ID atomically.

`MemoryStoreOptions::new(history_capacity)` supplies validated named limits; `max_payload_bytes` and `max_unfinished_records` reject zero. `SqliteTaskStore::open_async(path)` performs the synchronous open on Tokio's blocking pool. For custom wrappers, `delegate_task_store!(inner; except [accept_encoded]);` forwards every other method, including trait defaults, and rejects unknown or duplicate exceptions.


## Submission and external cancellation errors

The submit method reports expected capacity and duplicate-request rejections as `TaskServiceError::SubmissionCapacityExceeded`, `UnfinishedTaskLimitExceeded`, `IdempotencyConflict`, or `DuplicateTaskId`. These rejections affect only that request; the service remains available. Other store failures keep their operational error and may latch the service unavailable.

For a running task using `CancellationMode::ExternalHook`, `request_cancel()` persists the request and starts a service-owned hook operation. Concurrent calls for the same task attempt share that operation. Dropping or aborting a caller waiting for `request_cancel()` does not stop the hook, and `shutdown()` waits for it to finish. If the hook fails, `request_cancel()` returns `TaskServiceError::ExternalCancellationFailed` and the failure message remains in `TaskSummary.cancel_error`, bounded to 4096 UTF-8 bytes; call `request_cancel()` again to retry while the task is still running. A successful retry clears the diagnostic, but the handler remains responsible for reporting its terminal outcome. Make the hook idempotent for each (`TaskId`, `attempt`), because an external effect may have succeeded even when its acknowledgement was lost.
