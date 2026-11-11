# User guide

[简体中文](user-guide.zh_CN.md) · [Migration to 0.11](migration.md) · [Typed task API](typed-task-api.md)

`qubit-task` accepts typed, versioned payloads and runs them through registered handlers. This guide is now centered on the typed API; the former byte-oriented `TaskRequest`, `(task_type, handler_version)` routing, cursor-free query examples, and closure-oriented service API are no longer the public contract.

For durable task history, open the SQLite backend with `SqliteTaskStore::open(path)`. It migrates typed schema v4 through v7 in place to v8 while preserving tasks, active outbox rows, and quarantine records; legacy UUID schemas are rejected without being rewritten. See the [0.11 migration guide](migration.md) before upgrading an existing database.

## Start here

Read the [typed task API guide](typed-task-api.md) for the complete submission and handler example. It covers `TaskId`, `kind_id`, `category`, `Payload<T>`, codec registration, schema compatibility, metadata budgets, resource admission, cancellation, asynchronous progress, and the ordering contract for history pages.

The runnable typed examples are [`examples/task_service.rs`](../examples/task_service.rs) and [`examples/blocked_maintenance.rs`](../examples/blocked_maintenance.rs). The first shows handler registration and submission; the second shows filtered history queries and operator handling.

## Main concepts

- **Task identity:** each accepted task has an `rs-id::Id` wrapped by `TaskId`. Inject an `IdGenerator` that produces IDs unique across process restarts; Snowflake generators need distinct node IDs across processes and suitable clock configuration. Synchronous generation runs on Tokio's blocking pool. Cancelling submission does not stop an already-started generator call, so it may consume an ID without accepting a task; uncancelled submissions remain part of the shutdown admission boundary. Custom generators should return eventually and must not depend on executor-local state. Runtime shutdown can wait for a blocking call; this adds scheduling overhead but provides no hard timeout or unlimited-concurrency guarantee.
- **Routing and classification:** `kind_id` chooses a handler. `category` is an independent business filter for queries.
- **Payload compatibility:** `Payload<T>` binds `type_id`, `schema_version`, and `codec_id` to the value. A handler accepts one payload type ID and an explicit set of schema versions. A codec may serve multiple schema versions.
- **Codec work and submit cancellation:** synchronous payload encoding during `submit()` and decoding before a started handler run on Tokio's blocking pool. Cancelling before the encoded request is handed to the store stops acceptance, but cannot stop an encode already in progress; its result may be discarded. After acceptance is dispatched, the service owns the store operation and scheduler/notification wakeup. Cancelling or timing out the caller's `submit()` future does not cancel that operation, so the task may already be accepted or may be accepted shortly afterward; shutdown waits for it. Set a stable idempotency key before the first submit and retry the identical request to retrieve the same task. If you know its ID, use `get()` to check it; call `cancel()` to cancel the task itself. A running attempt keeps its slot and resource reservation during decode, and shutdown waits for decode and handler completion. Blocking work has no hard timeout, so a stuck codec can delay shutdown.
- **Resource admission:** CPU, GPU, memory, disk, and custom units are concurrency quotas. They reserve capacity inside the execution engine; they do not pin CPUs, isolate GPUs, or enforce operating-system memory or disk use.
- **Lifecycle and cancellation:** queued tasks can be cancelled directly. A running handler must declare cooperative cancellation or an external cancellation hook; a request alone cannot force arbitrary code to stop. External hook failures remain queryable and can be retried by calling `cancel()` again while the task is running. Concurrent calls share one hook, caller cancellation does not stop it, and `shutdown()` waits for it. Make hooks idempotent for each (`TaskId`, attempt).
- **Progress:** handlers report through `rs-progress::AsyncReporter`. `report_async()` awaits persistence, and subsequent task reads include stage and metric snapshots. A version is allocated before the store await and is never reused after an error or cancellation; version gaps are valid because progress versions are separate from lifecycle `state_version`.
- **History:** typed pages sort by `(accepted_at_ms, numeric task id)` ascending. The exclusive `after` cursor is a keyset cursor; every query sees its own storage snapshot. `TaskSummary` includes lifecycle and progress data without loading or decoding payload bytes. `get_state_checkpoint()` returns only current state and version for consumer recovery, including after SQLite pruning; it cannot restore payload, progress, or intermediate transitions.

## Start and shut down the service

Build the service during application startup and keep its handle for as long as the application accepts work. On application shutdown, first stop the application's own request or message intake, then await `service.shutdown()` and handle its result. Stopping that intake is application logic; the crate does not expose an application server stop method.

Shutdown stops new task submissions and scheduler dispatch, waits for already-dispatched submissions, running handlers, and external cancellation hooks, drains the notification publisher up to its configured timeout, and then releases store ownership. Queued tasks are left queued for a later service instance; shutdown does not run every queued task. A handler or cancellation hook that never returns can keep shutdown waiting indefinitely. Dropping the final service handle requests the same drain asynchronously, but `Drop` cannot wait for or report its result. If a `build()` future is cancelled after acquiring ownership, its guard queues asynchronous cleanup. That job attempts release immediately and retries returned `Failure` values using the same epoch, starting at 25 ms and doubling to a 5 second cap, indefinitely. There is no completion deadline; a permanent `Failure` keeps retrying and retains the store reference. `OwnerConflict`, another error, or a panic ends that cleanup job, and `OwnerConflict` does not mean the store has no owner. A permanently pending release or process exit is outside this in-process recovery guarantee.

Bound the time the application waits without treating a timeout as proof that shutdown finished:

```rust,no_run
use std::time::Duration;
use tokio::time::timeout;
use qubit_task::service::TaskServiceError;
use qubit_task::TaskExecutionService;

async fn stop_tasks(service: &TaskExecutionService) {
    match timeout(Duration::from_secs(30), service.shutdown()).await {
        Ok(Ok(())) => {}
        Ok(Err(TaskServiceError::NotificationClose(error))) => {
            eprintln!("notification drain failed: {error}");
            // Inspect the durable outbox backlog and alert the operator.
        }
        Ok(Err(TaskServiceError::StoreUnavailable(error))) => {
            eprintln!("task service or owner release failed: {error}");
            // After a transient release failure is addressed, call shutdown again.
        }
        Ok(Err(error)) => eprintln!("task service shutdown failed: {error}"),
        Err(_) => {
            eprintln!("shutdown is still draining; retain the service handle");
            // The supervisor continues. Await shutdown again when appropriate.
        }
    }
}
```

Cancelling or timing out a `shutdown()` waiter does not cancel the supervisor drain. Keep the handle and await `shutdown()` again after a slow handler or hook finishes. A timeout does not guarantee store ownership has been released, so do not build a replacement service on that assumption. Open a replacement service or store only after shutdown reports that ownership was released: `Ok(())` confirms complete shutdown, and `NotificationClose` reports a notification error after owner release succeeds. A later explicit `shutdown()` retries a transient owner-release failure. For a `NotificationClose` error, inspect the durable outbox backlog because remaining notification rows are available for a later service instance. If owner release panics, waiters receive `StoreUnavailable` and the still-armed owner guard is handed to the cleanup worker. That worker retries returned `Failure` errors indefinitely with the same epoch and bounded exponential backoff; it stops on `OwnerConflict`, other errors, or panic. A permanent `Failure` can therefore retain ownership and the store reference indefinitely, while a permanent hang or process exit has no automatic recovery guarantee. Cleanup logs include the epoch and failure attempt information for diagnosis; stderr is not a query interface. Treat a latched store fault separately from a transient release error.

## Features and limits

The `sqlite` feature enables durable task history and recovery. Recovery is at-least-once: work interrupted after an external side effect may run again, so application effects need idempotency or transaction protection. The service schedules in one process; it does not provide distributed scheduling or exactly-once business effects.

## Diagnostics and panics

Before persisting terminal-state diagnostics, the service limits `Failed.category` to 128 UTF-8 bytes and other terminal-state diagnostic fields to 4096 UTF-8 bytes. When a value exceeds its limit, the service retains the longest UTF-8 prefix that fits and appends `...`. This keeps the stored text valid UTF-8 and within the byte limit; it can omit the end of the original message. Direct writes through `TaskStore` still strictly reject over-limit diagnostics.

With the normal unwind panic strategy, a panic raised by an application payload decoder while the service prepares a task is recorded as that task's `Panicked` outcome; later tasks can continue and shutdown remains available. A panic in a handler remains isolated to its task as before. Panics cannot be caught when the process uses `panic=abort`.

External cancellation hook error messages are limited to 4096 UTF-8 bytes before the service returns or stores them. Direct `TaskStore` transitions that write a longer `cancel_error` return `InvalidRequest`; applications that need the full text should store it in their own diagnostic system. Existing SQLite rows with longer values remain readable, and the service shortens them when recovery or task finalization next writes the row. A cancelled or failed progress report also consumes its allocated version; the underlying asynchronous store may already have committed it, so a later report uses a higher version and leaves a gap.

## Publish lifecycle changes

Applications that need lifecycle notifications can enable the `event-bus` feature and pass an `Arc<AsyncEventBus>` to `TaskExecutionServiceBuilder::event_bus`. Register the application's `TaskEvent` codec on that bus. The service requires a store with persistent outbox support and a provider that declares both `DurabilityCapability::Durable` and a successful publish guarantee of at least `PublishGuarantee::Accepted`. `SqliteTaskStore` provides the store capability; `MemoryTaskStore` returns `UnsupportedCapability` during service construction. The built-in local event-bus provider is ephemeral and cannot be used for this publisher. Without `event_bus`, lifecycle notifications are not recorded.

Require durable retention and acceptance when creating the bus. In this startup excerpt, `facade` already contains the `TaskEvent` codec, and the application has linked its Redis provider crate:

```rust
use std::sync::Arc;

use qubit_event_bus::AsyncEventBusRegistry;
use qubit_event_bus::EventBusConfig;
use qubit_event_bus::RequiredCapabilities;
use qubit_event_bus::spi::PublishGuarantee;
use qubit_spi::ProviderSelection;

let config = EventBusConfig::default()
    .with_selection(ProviderSelection::named("redis-streams")?)
    .with_required_capabilities(
        RequiredCapabilities::new()
            .durable()
            .with_publish_guarantee(PublishGuarantee::Accepted),
    )
    .with_facade_config(facade);
let bus = Arc::new(AsyncEventBusRegistry::discover()?.create(&config).await?);
let service = builder.event_bus(bus.clone()).build().await?;
```

The [compilable wiring fixture](../tests/fixtures/doc-examples/src/main.rs) also supplies the provider options and task handler. The registry checks the requirement during bus creation. Before enabling the outbox, the task service independently checks the injected facade's cached durability and publish guarantee, then checks the `task.lifecycle` codec with `check_publish_codec`. An ephemeral provider fails with `NotificationProviderNotDurable`; a durable provider declaring `FireAndForget` fails with `NotificationProviderInsufficientGuarantee`; a missing codec fails with `NotificationCodecUnavailable`. These failures do not start publication or enable the outbox. The codec check confirms only registration in the facade configuration; it does not prove that encoding succeeds for every event or that Redis is reachable or persistent.

With the publisher enabled, each committed lifecycle snapshot is written to SQLite in the same transaction as its task state change. A background worker sends rows in stable order to `task.lifecycle`. Until the provider reports acceptance, the row remains in SQLite and is retried, so a retry may publish duplicates. After `Accepted`, the row is deleted. Rows present at startup are replayed after task recovery; states committed before the outbox was enabled are not backfilled. A successful Redis `XADD` is stream acceptance, not proof of a disk `fsync`, replication, or consumer ACK. `DurabilityCapability::Durable` means the provider retains messages without subscribers; `PublishGuarantee::Accepted` describes the provider's acceptance boundary. Neither promises those later stages. After an acceptance reply, a Redis failure before persistence can still lose a notification whose SQLite outbox row has been deleted; consumers are not guaranteed at-least-once receipt. This notification boundary is separate from the at-least-once task handler recovery described in [Features and limits](#features-and-limits). Consumers should keep the highest `state_version` per `TaskId`, ignore duplicates and stale versions, and query `TaskExecutionService::get_state_checkpoint` if versions are missing. The event is a notification, not an authoritative state record.

### Durable consumer projection

Store each task's highest processed `state_version` and projected business state in a durable table keyed by `TaskId`. In one SQLite transaction, read that checkpoint before touching the business projection. Ignore an event whose version is at or below the checkpoint. Apply version 0 directly when it is the first event; if the first observed version is greater than 0, or a later event skips one or more versions, query `TaskExecutionService::get_state_checkpoint` for the authoritative current state and version. `None`, a query error, or a checkpoint version below the notification version is not safe to apply: roll back and do not ACK. Return a requeueing handler error so the delivery stays pending for Retry. A checkpoint restores only the latest state; it cannot replay each missing transition or its business side effects. Otherwise write the selected state, consumer checkpoint, and any business side effect in that same transaction, then commit. The integration regression in `tests/redis_task_outbox_tests.rs` closes and reopens its SQLite consumer before replaying duplicate Redis records.

Version-gap handling only runs when a later event arrives. A durable notification can still disappear after provider acceptance, leaving a projection at `Running` with no later event to expose the missing terminal state. When the application needs bounded recovery from that tail loss, persist each accepted `TaskId` in an application-owned tracking table and reconcile tracked IDs in bounded batches after a business-chosen staleness threshold. Query `get_state_checkpoint` before opening the consumer write transaction; inside the transaction, re-read the latest consumer version and apply the checkpoint plus its business effect only when its version is higher. If the query fails, returns `None`, or is older than the consumer projection, leave the ID tracked for retry and record a retryable diagnostic. Retire tracking only after the terminal projection commits. A repeated or racing reconciliation must not overwrite a newer event or repeat its business effect. This explicit application loop does not add a library timer or create ACKs for messages that were never delivered. It repairs the current projection only; it cannot recreate each missing transition or its side effects, and it does not promise exactly-once processing. The regression `test_reconcile_missing_terminal_notification` demonstrates a lost terminal notification with no later event, reopening the consumer database, and idempotent reconciliation.

Return success from the delivery handler only after the transaction commits, so the bus can ACK it. On a failed transaction or service query, return a handler error configured to requeue (`FailureDirective::Requeue`); the Redis delivery remains pending for Retry. Never ACK first and repair the checkpoint later. The publisher's stable `EventId` helps correlate repeated wire records; the Redis provider does not deduplicate by `EventId`. The consumer's durable task version check prevents repeat business effects across process restarts.

On shutdown the worker drains until empty or `notification_shutdown_timeout` expires; a timeout is reported and remaining rows stay durable for a later restart. Monitor SQLite outbox row count and oldest-row age, plus Redis stream `XLEN` and consumer-group `XPENDING`. These measurements distinguish a stalled publisher from stream growth or a consumer that is not acknowledging work.

Before rollout and after Redis failover, also review `INFO persistence` and `INFO replication` against the deployment's recovery objectives, then inspect `XLEN <stream-key>`, `XINFO GROUPS <stream-key>`, and `XPENDING <stream-key> <group>` (including pages of pending entries when needed). These observations help separate Redis persistence and replica state from stream growth, group progress, and consumer recovery; do not apply a fixed threshold without workload and recovery requirements. `check_publish_codec` checks whether the facade has a codec registered for `task.lifecycle`; `NotificationCodecUnavailable` reports that it does not. This readiness check proves only facade configuration: it does not prove that each event can be encoded, Redis is reachable, an accepted `XADD` was fsynced or replicated, or a consumer committed its business transaction. See the [Redis deployment readiness checklist](https://github.com/qubit-ltd/rs-event-bus-redis/blob/dev-starfish/doc/user_guide.md) for command details and the provider's persistence boundaries.

The service's `notification_stats()` reports process-local queued, published, and failed counts; the failed count is not a durable backlog measure. Query SQLite for the durable backlog and its age, and inspect `notification_failure()` for the current failed head's task ID, state version, event ID, and error:

```sql
SELECT COUNT(*) AS pending,
       CASE WHEN MIN(created_at_ms) IS NULL THEN 0
            ELSE CAST(strftime('%s', 'now') AS INTEGER) * 1000 - MIN(created_at_ms)
       END AS oldest_age_ms
FROM task_event_outbox;
```

### Recover a failed notification head

Use this sequence when `notification_stats().failed` increases or the durable outbox backlog stops draining:

1. Read `notification_failure()` and record its exact `task_id`, `state_version`, `event_id`, and `error`. The diagnostic is an in-process snapshot of the current failed head, not a durable alert.
2. Check the authoritative task state with `service.get_state_checkpoint(task_id).await` and decide how the missing notification should be handled by the application. Inspect existing quarantine records and follow the application's archive/retention policy before isolating anything. A failed publication can still be a transient provider problem; quarantine is an operator decision.
3. If isolation is approved, call `service.quarantine_failed_notification(expected_task_id, expected_state_version, &expected_event_id, reason).await` with the exact identity returned by the diagnostic and a meaningful reason. A changed head returns a conflict; read the diagnostic again before deciding what to do. The operation atomically archives the original outbox row and removes it from the active queue.
4. Verify the record in `task_event_outbox_quarantine` and confirm that the next active outbox row can be dequeued and published. The archive is evidence only: quarantine intentionally loses that notification and never automatically republishes it.
5. Consumers must detect the resulting `state_version` gap and refresh from the task service's authoritative state. Apply the refreshed state and checkpoint according to the consumer's transaction and acknowledgement policy.

An archive lookup can use the exact key from the failure snapshot:

```sql
SELECT task_id, state_version, event_id, event_json, created_at_ms,
       quarantined_at_ms, reason, last_error
FROM task_event_outbox_quarantine
WHERE task_id = ?1 AND state_version = ?2;
```

Do not quarantine a record merely to bypass normal network retries. SQLite schema v8 retains archived rows independently of terminal task pruning; database retention is managed by the application. Pruning skips tasks with active outbox rows and atomically stores a terminal state checkpoint before removing the task. Checkpoints are not automatically expired, so their count and disk use grow with pruned tasks. SQLite reserves each pruned task ID while its checkpoint exists; `MemoryTaskStore` does not retain pruned identities.

### Maintain history and quarantine archives

Before planning cleanup, record counts for `task_event_outbox`, terminal tasks, `task_event_outbox_quarantine`, and `task_terminal_checkpoints`, plus each table's oldest relevant timestamp. Record `PRAGMA page_count`, `PRAGMA freelist_count`, and `PRAGMA page_size` as database-level indicators. Counts can scan large tables and do not measure disk bytes; choose an observation cadence for the workload.

For a maintenance window, stop the application's request and message intake, await service shutdown and confirm its owner was released, then close database connections and stop every process that could reopen the file. A shutdown timeout is not proof that the owner was released. Ensure no other service or maintenance process can contend for the database. Create a consistent backup with SQLite's backup API or `sqlite3` `.backup`; verify it opens and can be read. Do not copy only the main database file while WAL is active. Keep the archive for the application's audit retention period.

After backup verification, an operator may delete only quarantine rows older than the application's chosen retention cutoff. Bind the actual values with `sqlite3` `.parameter set` or Python parameters; never interpolate input or hard-code a business retention period. The cutoff is exclusive, and selection is stable by timestamp, task ID, and state version. Active outbox rows, tasks, and checkpoints are untouched. Checkpoints remain by default and there is no automatic expiry.

SQLite treats a negative `LIMIT` as unlimited. Validate inputs before opening the transaction, and execute each statement on the same connection so `changes()` reports this batch. On any exception, roll back; otherwise commit and repeat until zero rows are deleted.

```python
if type(cutoff_ms) is not int or not 0 <= cutoff_ms <= 2**63 - 1:
    raise ValueError("cutoff_ms must be a nonnegative SQLite integer")
if type(batch_size) is not int or not 1 <= batch_size <= 2**63 - 1:
    raise ValueError("batch_size must be a positive SQLite integer")

try:
    connection.execute("BEGIN IMMEDIATE;")
    connection.execute(
        "DELETE FROM task_event_outbox_quarantine "
        "WHERE (task_id, state_version) IN ("
        "SELECT task_id, state_version FROM task_event_outbox_quarantine "
        "WHERE quarantined_at_ms < :cutoff_ms "
        "ORDER BY quarantined_at_ms, task_id, state_version LIMIT :batch_size)",
        {"cutoff_ms": cutoff_ms, "batch_size": batch_size},
    )
    deleted_rows = connection.execute("SELECT changes() AS deleted_rows").fetchone()[0]
    connection.execute("COMMIT;")
except Exception:
    connection.execute("ROLLBACK;")
    raise
```

Use this transaction once per batch, then repeat with the same application-selected cutoff until `deleted_rows` is zero. The equivalent SQL is:

```sql
BEGIN IMMEDIATE;
DELETE FROM task_event_outbox_quarantine
WHERE (task_id, state_version) IN (
    SELECT task_id, state_version
    FROM task_event_outbox_quarantine
    WHERE quarantined_at_ms < :cutoff_ms
    ORDER BY quarantined_at_ms, task_id, state_version
    LIMIT :batch_size
);
SELECT changes() AS deleted_rows;
COMMIT;
```

On failure, roll back the batch and investigate before retrying; do not advance the cutoff automatically or continue past data that has not been archived. Deletion makes pages reusable but does not necessarily shrink the file. Run `VACUUM` only explicitly, in a maintenance window, after a verified backup and after confirming sufficient free space; it can require substantial space and block database work. There is no checkpoint deletion SQL or default retention duration. Checkpoints preserve reconciliation state and used task IDs, so plan their growth through capacity and external archive policy rather than routine deletion.

See the [detailed design](task_execution_service_design.en.md), [migration guide](migration.md), and [API reference](https://docs.rs/qubit-task) for more context.
