# User guide

[简体中文](user-guide.zh_CN.md) · [Migration to 0.10](migration.md) · [Typed task API](typed-task-api.md)

`qubit-task` accepts typed, versioned payloads and runs them through registered handlers. This guide is now centered on the typed API; the former byte-oriented `TaskRequest`, `(task_type, handler_version)` routing, cursor-free query examples, and closure-oriented service API are no longer the public contract.

For durable task history, open the SQLite backend with `SqliteTaskStore::open(path)`. It migrates typed schema v4 through v7 in place to v8 while preserving tasks, active outbox rows, and quarantine records; legacy UUID schemas are rejected without being rewritten. See the [0.10 migration guide](migration.md) before upgrading an existing database.

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

Do not quarantine a record merely to bypass normal network retries. SQLite schema v8 retains archived rows independently of terminal task pruning; database retention is managed by the application. Pruning skips tasks with active outbox rows and atomically stores a terminal state checkpoint before removing the task. Checkpoints are not automatically expired, so their count and disk use grow with pruned tasks.

See the [detailed design](task_execution_service_design.en.md), [migration guide](migration.md), and [API reference](https://docs.rs/qubit-task) for more context.
