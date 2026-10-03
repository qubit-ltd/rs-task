# Qubit Task (`rs-task`)

[![Rust CI](https://github.com/qubit-ltd/rs-task/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-task/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-task/coverage-badge.json)](https://qubit-ltd.github.io/rs-task/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-task.svg?color=blue)](https://crates.io/crates/qubit-task)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![中文文档](https://img.shields.io/badge/文档-中文版-blue.svg)](README.zh_CN.md)

`qubit-task` runs long-lived application work as typed, queryable tasks. Applications submit a typed payload, receive a stable task ID, and can query lifecycle state and live progress. Handlers declare the task kind, payload type, supported schema versions, and cancellation mode. The service provides bounded in-process scheduling and optional SQLite persistence with at-least-once recovery.

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

With the optional `event-bus` feature, configure `TaskExecutionServiceBuilder::event_bus` to publish lifecycle snapshots from a durable SQLite outbox. The schema v6 migration preserves existing task rows. Delivery is asynchronous and at-least-once: consumers should deduplicate by `(TaskId, state_version)` and query the service for authoritative state. `MemoryTaskStore` does not provide durable outbox support.

## Documentation

- [User guide](doc/user-guide.md)
- [Detailed design](doc/task_execution_service_design.en.md)
- [Migration guide](doc/migration-0.8.en.md)
- [API reference](https://docs.rs/qubit-task)

## Task notification delivery

The publisher replays committed outbox rows after service recovery and drains them on shutdown up to `notification_shutdown_timeout`. An uncertain publish receipt or a crash after Redis accepts an event but before the outbox row is deleted can cause a duplicate. Consumers should retain the highest `state_version` per task and ignore duplicate or stale events. Enabling the publisher does not backfill lifecycle states committed before the service started with it. Monitor pending outbox row count and oldest-row age, together with Redis stream `XLEN` and consumer-group `XPENDING`.

The scheduler scans ready queued work in keyset order. Tasks blocked on a temporarily unavailable resource are skipped so a later task with a satisfiable request can start; scheduling is therefore not strict FIFO. Applications can call `TaskStore::prune_terminal_before(finished_before_ms, max_rows)` to explicitly remove a bounded batch of old terminal history. Pruning also releases idempotency keys for reuse; queued and blocked tasks are retained.

## Checks

```bash
cargo test --all-features
./align-ci.sh
./ci-check.sh
```
