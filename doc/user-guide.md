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
- **Lifecycle and cancellation:** queued tasks can be cancelled directly. A running handler must declare cooperative cancellation or an external cancellation hook; a request alone cannot force arbitrary code to stop. External hook failures remain queryable and can be retried by calling `cancel()` again while the task is running. Concurrent calls share one hook, caller cancellation does not stop it, and `shutdown()` waits for it. Make hooks idempotent for each (`TaskId`, attempt).
- **Progress:** handlers report through `rs-progress::AsyncReporter`. `report_async()` awaits persistence, and subsequent task reads include stage and metric snapshots.
- **History:** typed pages sort by `(accepted_at_ms, numeric task id)` ascending. The exclusive `after` cursor is a keyset cursor; every query sees its own storage snapshot.

## Features and limits

The `sqlite` feature enables durable task history and recovery. Recovery is at-least-once: work interrupted after an external side effect may run again, so application effects need idempotency or transaction protection. The service schedules in one process; it does not provide distributed scheduling or exactly-once business effects.

## Publish lifecycle changes

Applications that need lifecycle notifications can enable the `event-bus` feature and pass an `Arc<AsyncEventBus>` to `TaskExecutionServiceBuilder::event_bus`. Register the application's `TaskEvent` codec on that bus. The service requires a store with persistent outbox support; `SqliteTaskStore` provides it, while `MemoryTaskStore` returns `UnsupportedCapability` during service construction. Without `event_bus`, lifecycle notifications are not recorded.

With the publisher enabled, each committed lifecycle snapshot is written to SQLite in the same transaction as its task state change. A background worker sends rows in stable order to `task.lifecycle`, then deletes a row after accepted publication. Rows present at startup are replayed after task recovery; states committed before the outbox was enabled are not backfilled. Publication is at-least-once: an unknown result or a crash after the bus accepted an event but before SQLite deleted its row can produce a duplicate. Consumers should keep the highest `state_version` per `TaskId`, ignore duplicates and stale versions, and query the task service if versions are missing. The event is a notification, not an authoritative state record.

On shutdown the worker drains until empty or `notification_shutdown_timeout` expires; a timeout is reported and remaining rows stay durable for a later restart. Monitor SQLite outbox row count and oldest-row age, plus Redis stream `XLEN` and consumer-group `XPENDING`. These measurements distinguish a stalled publisher from stream growth or a consumer that is not acknowledging work.

The service's `notification_stats()` reports process-local queued, published, and failed counts; it does not report the durable backlog. Query SQLite for that backlog and its age:

```sql
SELECT COUNT(*) AS pending,
       CASE WHEN MIN(created_at_ms) IS NULL THEN 0
            ELSE CAST(strftime('%s', 'now') AS INTEGER) * 1000 - MIN(created_at_ms)
       END AS oldest_age_ms
FROM task_event_outbox;
```

See the [detailed design](task_execution_service_design.en.md), [migration guide](migration.md), and [API reference](https://docs.rs/qubit-task) for more context.
