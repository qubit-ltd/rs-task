# User guide

`qubit-task` accepts typed, versioned payloads and runs them through registered handlers. This guide is now centered on the typed API; the former byte-oriented `TaskRequest`, `(task_type, handler_version)` routing, cursor-free query examples, and closure-oriented service API are no longer the public contract.

## Start here

Read the [typed task API guide](typed-task-api.md) for the complete submission and handler example. It covers `TaskId`, `kind_id`, `category`, `Payload<T>`, codec registration, schema compatibility, metadata budgets, resource admission, cancellation, asynchronous progress, and the ordering contract for history pages.

The runnable typed examples are [`examples/task_service.rs`](../examples/task_service.rs) and [`examples/blocked_maintenance.rs`](../examples/blocked_maintenance.rs). The first shows handler registration and submission; the second shows filtered history queries and operator handling.

## Main concepts

- **Task identity:** each accepted task has an `rs-id::Id` wrapped by `TaskId`. Inject an `IdGenerator`; Snowflake generators need distinct node IDs across processes and suitable clock configuration.
- **Routing and classification:** `kind_id` chooses a handler. `category` is an independent business filter for queries.
- **Payload compatibility:** `Payload<T>` binds `type_id`, `schema_version`, and `codec_id` to the value. A handler accepts one payload type ID and an explicit set of schema versions. A codec may serve multiple schema versions.
- **Resource admission:** CPU, GPU, memory, disk, and custom units are concurrency quotas. They reserve capacity inside the execution engine; they do not pin CPUs, isolate GPUs, or enforce operating-system memory or disk use.
- **Lifecycle and cancellation:** queued tasks can be cancelled directly. A running handler must declare cooperative cancellation or an external cancellation hook; a request alone cannot force arbitrary code to stop.
- **Progress:** handlers report through `rs-progress::AsyncReporter`. `report_async()` awaits persistence, and subsequent task reads include stage and metric snapshots.
- **History:** typed pages sort by `(accepted_at_ms, numeric task id)` ascending. The exclusive `after` cursor is a keyset cursor; every query sees its own storage snapshot.

## Features and limits

The `sqlite` feature enables durable task history and recovery. Recovery is at-least-once: work interrupted after an external side effect may run again, so application effects need idempotency or transaction protection. The service schedules in one process; it does not provide distributed scheduling or exactly-once business effects.

The optional `event-bus` integration and Redis provider fixtures demonstrate `TaskEvent` transport and consumer handling. Typed task lifecycle publication is not currently wired into the typed execution service; consumers should not treat transport events as the authoritative task record.

See the [detailed design](task_execution_service_design.en.md), [migration guide](migration-0.8.en.md), and [API reference](https://docs.rs/qubit-task) for more context.
