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

## Execution and resource admission

The local engine reserves requested CPU slots, GPU devices/labels, memory bytes, disk bytes, and custom integer units before starting a handler. Reservations account for concurrent tasks and are released when execution ends. They do not pin CPU cores, discover or isolate GPUs at the operating-system level, or enforce process memory/disk usage. Requests that exceed configured capacity cannot run; requests that fit wait until enough capacity is free.

The task lifecycle uses `Queued`, `Running`, `Blocked`, `Succeeded`, `Failed`, `Panicked`, and `Cancelled`. State transitions compare the stored state version and attempt to reject stale writes. Queued or blocked tasks can be cancelled directly. A running task can be cancelled only according to its handler's declared mode: cooperative handlers observe `TaskContext::is_cancelled()` and must stop at a safe boundary; an external-hook handler delegates cancellation to its hook. Unsupported running cancellation is reported to the caller.

## Progress and history

Handlers report progress with `rs-progress::AsyncReporter` through their task context. `report_async()` waits for the progress snapshot to be persisted; subsequent task reads expose the current stage and metric values. Progress updates do not advance the task lifecycle state version.

History pages sort ascending by `(accepted_at_ms, numeric task id)`. The exclusive `after` cursor contains that ordering key, not an offset. Each query observes its own store snapshot; concurrent inserts do not turn the cursor into a snapshot token. Filters include lifecycle state, `category`, and correlation key. `kind_id` is deliberately independent from category filtering.

## Reliability boundaries

SQLite persistence supports process restart recovery with at-least-once execution. A crash after an external side effect but before its result is stored can cause the handler to run again, so applications must make effects idempotent or protect them with their own transaction strategy. Scheduling is process-local; the crate does not provide distributed scheduling, forced interruption of arbitrary code, or exactly-once business effects.

The optional Event Bus integration exposes `TaskEvent` transport types and codecs, but lifecycle publication is not currently connected to the typed execution service. Task queries remain the authoritative source of state. See the [typed API guide](typed-task-api.md) for the end-to-end example and concrete API contracts.
