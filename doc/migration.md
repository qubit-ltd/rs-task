# Migration to task service 0.8

[简体中文](migration.zh_CN.md) · [User guide](user-guide.md)

Task 0.8 adopts the Event Bus 0.17 public type generation. Update application
`qubit-task`, `qubit-event-bus`, and optional Redis provider dependencies together
to 0.8, 0.17, and 0.5, including doc/IoC/application fixtures and lockfiles.
No compatibility alias bridges old and new EventBus types. Applications without
notifications retain the task store, recovery, and scheduling contracts.

## Migrate the notification codec

The application owns its `EventCodec<TaskEvent>`; the task crate does not
provide a global schema registry or public task codec. Change decode to accept
`&EncodedPayload` and read `payload.bytes()`. Default metadata validation now
requires exact content type and optional schema equality.

The [guide's compiled JSON codec](user-guide.md#publish-through-redis-streams)
writes `application/json` plus `task-event-v1`; its explicit override accepts
only that schema or historical `None` with the same content type. Unknown
schemas and other MIME texts return `MetadataMismatch`, stop facade reception,
and leave durable work unsettled. Register the migrated codec at startup;
repair incompatible consumers and create a new durable subscription in the
same group to recover old work. Redis wire version 1 remains readable. Ordinary
JSON `CodecError::Decode` still rejects a bad message rather than preserving it
as a schema mismatch.

Facade encoded publish/receive limits now each default to 1 MiB and require
positive `PayloadLimits`. Redis adds independent positive 8 MiB wire, 1 MiB
payload, and 64 KiB headers defaults. Configure both layers when historical
records need more; do not delete pending records to hide an overflow.

## Observe uncertainty without repeating task transitions

Task lifecycle `NotificationStats` are process-local: `queued` counts writes
signalled to the publisher, `published` counts accepted events removed from
SQLite, `failed` counts failed outbox reads, publications, or deletions, and `dropped` stays
zero because committed outbox rows are retained. These counters do not survive
restart and do not measure subscriber completion. Query SQLite for the durable
backlog and oldest row age. Unknown outcomes retain the row and can cause a
duplicate when the stable EventId is retried.

The core public failure is `PublishFailure` with original EventId, aggregate
effect, and structured cause. Default `DuplicateRiskPolicy::Forbid` stops blind
uncertain retries even when a custom rule asks to continue. A started cancelled
publish may be uncertain; RetryPolicy budgets are soft and do not universally
interrupt in-flight I/O. Redis does not deduplicate by EventId.

Consumers keep the highest `state_version` for each TaskId, ignore same-version
duplicates and stale events, and query the service after a gap. Parallel state
changes need not arrive in increasing version order. Do not repeat a committed
task transition because its notification failed. Facade DLQ forwarding and
source acknowledgement are not atomic, so logical dead-letters also need
consumer deduplication.

## Enable the durable task lifecycle outbox

With the `event-bus` feature enabled, configure
`TaskExecutionServiceBuilder::event_bus` and register the application's
`TaskEvent` codec on the supplied `AsyncEventBus`. The configured store must
implement persistent outbox operations. `SqliteTaskStore` does; `MemoryTaskStore`
does not and service construction returns `UnsupportedCapability` for it.

Typed SQLite schema version 6 adds `task_event_outbox`. Opening a typed schema
version 4 or 5 database migrates it transactionally and preserves existing task
rows. No historical lifecycle snapshots are generated: only transitions made
after the service enables the outbox are captured. The migration does not
remove existing task data. Existing legacy UUID schemas still require explicit
data mapping before they can be opened by the typed API.

Lifecycle writes and outbox inserts share a SQLite transaction. A background
worker publishes rows asynchronously and deletes each row only after accepted
publication. This is at-least-once delivery: an uncertain result or a crash
after the bus accepted an event but before row deletion can cause a duplicate.
Deduplicate using `(TaskId, state_version)` and query the task service for
authoritative state. Monitor the outbox row count and oldest-row age; with Redis
Streams, also inspect stream `XLEN` and consumer-group `XPENDING`. Shutdown drains
until empty or `notification_shutdown_timeout`; pending rows survive a timeout
and are retried after restart.

Run the application compile, notification uncertainty, schema compatibility,
state-version convergence, Redis recovery, and shutdown tests before deploying.
Earlier task API changes remain documented in the user guide.
