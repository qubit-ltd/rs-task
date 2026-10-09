# Qubit Task (`rs-task`)

[![Rust CI](https://github.com/qubit-ltd/rs-task/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-task/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-task/coverage-badge.json)](https://qubit-ltd.github.io/rs-task/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-task.svg?color=blue)](https://crates.io/crates/qubit-task)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![中文文档](https://img.shields.io/badge/文档-中文版-blue.svg)](README.zh_CN.md)

`qubit-task` runs long-lived application work as typed, queryable tasks. Applications submit a typed payload, receive a stable task ID, and can query lifecycle state and live progress. Handlers declare the task kind, payload type, supported schema versions, and cancellation mode. The service provides bounded in-process scheduling and optional SQLite persistence with at-least-once recovery.

Typed payload identity comes from the submitted Rust type's `HasModelId`
implementation. `TaskRequest::new` accepts the kind, schema version, codec ID,
and value, so callers cannot choose a different model ID at construction.

## Typed API

The public API keeps these identifiers separate:

- `TaskId` identifies one task.
- `kind_id` routes a task to a handler.
- `category` classifies tasks for application queries.
- `Payload<T>` binds a typed value to `type_id`, `schema_version`, and `codec_id`.

The [typed task API guide](doc/typed-task-api.md) contains the complete example and contracts for handler registration, codec reuse, resource quotas, cancellation, progress reporting, metadata limits, and keyset pagination. Runnable examples are [`examples/task_service.rs`](examples/task_service.rs) and [`examples/blocked_maintenance.rs`](examples/blocked_maintenance.rs).

## Installation

```toml
[dependencies]
qubit-task = { version = "0.10", features = ["sqlite"] }
tokio = { version = "1.53", features = ["macros", "rt-multi-thread"] }
```

The `sqlite` feature enables durable task history and recovery. The scheduler scans queued summaries in bounded pages and runs at most `max_running_tasks` handlers concurrently. A handler error is retried only when `retryable` is true, with a persisted deadline; the default is three attempts with exponential delay from one to sixty seconds. `resume_blocked` requeues a blocked task using its observed state version after configuration is repaired. Recovery is at-least-once: a task interrupted after an external side effect may run again, so application effects need idempotency or transaction protection. Scheduling is process-local; distributed scheduling and exactly-once business effects are outside the crate's guarantees.

With the optional `event-bus` feature, configure `TaskExecutionServiceBuilder::event_bus` to publish lifecycle snapshots from a durable SQLite outbox. This requires a persistent outbox store and an event-bus provider that declares `DurabilityCapability::Durable` with at least `PublishGuarantee::Accepted`; the built-in local provider is not suitable. Set `EventBusConfig::with_required_capabilities(RequiredCapabilities::new().durable().with_publish_guarantee(PublishGuarantee::Accepted))` when creating the bus. The task service checks the injected facade's cached capabilities again before enabling the outbox and rejects an ephemeral provider or a durable provider with only `FireAndForget` publication. The schema v6 migration preserves existing task rows. Delivery is asynchronous and at-least-once: consumers should deduplicate by `(TaskId, state_version)` and query the service for authoritative state. `MemoryTaskStore` does not provide durable outbox support.

## Documentation

- [User guide](doc/user-guide.md)
- [Detailed design](doc/task_execution_service_design.en.md)
- [Migration guide to 0.10](doc/migration.md)
- [API reference](https://docs.rs/qubit-task)

## Task notification delivery

`PublishGuarantee::Accepted` describes the provider's acceptance boundary. It does not prove that an accepted notification has reached disk or a replica; a Redis failure before persistence can still lose a notification after its SQLite outbox row has been deleted.

The publisher replays committed outbox rows after service recovery and drains them on shutdown up to `notification_shutdown_timeout`. An uncertain publish receipt or a crash after Redis accepts an event but before the outbox row is deleted can cause a duplicate. A successful Redis `XADD` means the stream accepted the entry; it does not prove a disk `fsync` or a consumer ACK. `DurabilityCapability::Durable` describes message retention without a subscriber, not those stronger guarantees. Consumers should retain the highest `state_version` per task and ignore duplicate or stale events. The stable `EventId` is for correlation; the Redis provider does not deduplicate by it. For the transactional checkpoint, version-gap refresh, and ACK/Retry order, see the [durable consumer projection guide](doc/user-guide.md#durable-consumer-projection). Enabling the publisher does not backfill lifecycle states committed before the service started with it. Monitor pending outbox row count and oldest-row age, together with Redis stream `XLEN` and consumer-group `XPENDING`.

The scheduler scans ready queued work in keyset order. A task waiting for a temporarily unavailable resource can be bypassed by newer tasks with available resources. After 32 successful bypasses by default, the scheduler gives the older task priority; configure the positive limit with `TaskExecutionServiceBuilder::max_resource_bypasses`. Reaching the limit can leave an unrelated resource idle while the older task waits. The count is process-local and resets after a restart. This is neither strict FIFO nor a bound on waiting time. Applications can call `TaskStore::prune_terminal_before(finished_before_ms, max_rows)` to explicitly remove a bounded batch of old terminal history. Pruning also releases idempotency keys for reuse; queued and blocked tasks are retained.

Call `shutdown().await` when the caller needs to wait for active handlers and the notification drain, release store ownership, and receive the shared shutdown result, including errors. Dropping the last external service handle requests the same drain asynchronously; `Drop` does not wait or return its result. Cancelling a shutdown waiter does not cancel the drain. A handler that never finishes can keep ownership held. If owner release panics, waiters receive `StoreUnavailable` and the still-armed guard is handed to the cleanup worker; after a transient owner-release error, a later explicit `shutdown()` retries release. Cancelling a `build()` future after ownership is acquired also leaves release to the prestarted cleanup worker. That cleanup is eventual, so an immediate second build may briefly report an owner conflict.

## Checks

```bash
cargo test --all-features
./.infra/bin/align-ci.sh
./.infra/bin/ci-check.sh
```
