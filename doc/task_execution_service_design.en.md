# Qubit Task: Typed Task Service Design

[中文设计文档](task_execution_service_design.md)

This document describes the current typed task API. The former byte-oriented request, exact `(task_type, handler_version)` routing, local-closure submission, and legacy service builder are historical implementation details and are not the public contract.

## Model and routing

Each task has a stable `TaskId` wrapping `rs-id::Id`. The application injects an `IdGenerator`; Snowflake-style generators need distinct node IDs across processes and suitable clock configuration.

The service keeps routing, classification, and payload identity separate:

- `kind_id` selects a registered handler.
- `category` is an application query filter and does not affect routing.
- `Payload<T>` binds a value to `type_id`, `schema_version`, and `codec_id`.

A handler registered for a `kind_id` accepts one payload `type_id` and declares the schema versions it supports. The codec registry maps bytes codecs independently; a codec may serve multiple schema versions. Encoding validates the typed request and stores an `EncodedPayload` with its identity and bytes. Decoding and handler dispatch occur after acceptance when the task starts.

## Service and store

`TaskExecutionServiceBuilder` receives a typed `TaskStore`, byte codec registry, and ID generator. `capacity(ResourceCapacity)` configures the local engine's reservation budgets. The builder owns a `TypedTaskHandlerRegistry`; registrations are fixed when the service is built. The service writes accepted task data through the store and returns payload-free `TaskSummary` values for status reads and history pages.

Memory and SQLite stores implement the same typed store contract. SQLite uses its typed numeric-ID schema; an incompatible legacy UUID schema is rejected with a diagnostic rather than silently reinterpreted. Store ownership fences concurrent service instances, and recovery resumes retained queued work after acquiring ownership.

One scheduler scans queued summaries in bounded pages and starts handlers only while a running slot is available. `max_running_tasks` defaults to available parallelism; `scan_page_size` defaults to 128 and is capped at 256. Retry deadlines are persisted in SQLite schema version 6 and observed across restarts. Only explicitly retryable handler failures are retried, up to the configured attempt limit. Blocked tasks can be resumed with their observed state version after operators repair configuration.

## Execution and resource admission

The scheduler reserves requested CPU slots, GPU devices/labels, memory bytes, disk bytes, and custom integer units before the start CAS. A task whose resources are temporarily unavailable remains queued while later ready tasks are considered. This work-conserving policy is not strict FIFO and may let smaller tasks pass an older resource-blocked task. Reservations count only after a successful start CAS and are released on every completion or failed admission path. They do not pin CPU cores, discover or isolate GPUs at the operating-system level, or enforce process memory/disk usage.

The task lifecycle uses `Queued`, `Running`, `Blocked`, `Succeeded`, `Failed`, `Panicked`, and `Cancelled`. State transitions compare the stored state version and attempt to reject stale writes. Queued or blocked tasks can be cancelled directly. A running task can be cancelled only according to its handler's declared mode: cooperative handlers observe `TaskContext::is_cancelled()` and must stop at a safe boundary; an external-hook handler delegates cancellation to its hook. Unsupported running cancellation is reported to the caller.

## Progress and history

Handlers report progress with `rs-progress::AsyncReporter` through their task context. `report_async()` waits for the progress snapshot to be persisted; subsequent task reads expose the current stage and metric values. Progress updates do not advance the task lifecycle state version.

History pages sort ascending by `(accepted_at_ms, numeric task id)`. The exclusive `after` cursor contains that ordering key, not an offset. Each query observes its own store snapshot; concurrent inserts do not turn the cursor into a snapshot token. Filters include lifecycle state, `category`, and correlation key. `kind_id` is deliberately independent from category filtering.

## Reliability boundaries

SQLite persistence supports process restart recovery with at-least-once execution. A crash after an external side effect but before its result is stored can cause the handler to run again, so applications must make effects idempotent or protect them with their own transaction strategy. Scheduling is process-local; the crate does not provide distributed scheduling, forced interruption of arbitrary code, or exactly-once business effects.

Store failures in background scheduling and finalization are latched. New writes stop, and `shutdown()` drains active work before returning the stored failure and releasing ownership. Diagnostic reads continue to use the store.

When configured with `TaskExecutionServiceBuilder::event_bus`, the service enables the store outbox and starts an asynchronous publisher after task recovery. Each lifecycle snapshot is inserted in the same SQLite transaction as its task transition, then published to `task.lifecycle` and removed after accepted publication. This requires persistent outbox support; `MemoryTaskStore` is not durable and rejects this configuration. Only transitions written while the outbox is enabled are captured; startup does not backfill older task states.

Delivery is at-least-once. If a publish result is unknown, or the process stops after the bus accepted the event but before the outbox row is deleted, the same snapshot may be published again. Consumers should deduplicate by `(TaskId, state_version)`, ignore stale versions, and query the service after a gap. Monitor outbox row count and oldest-row age, and check Redis `XLEN` and `XPENDING` when Redis Streams is the provider. See the [typed API guide](typed-task-api.md) and [user guide](user-guide.md#publish-lifecycle-changes) for usage and operational boundaries. Applications explicitly prune terminal history with bounded `TaskStore::prune_terminal_before`; this also releases idempotency keys for reuse.
