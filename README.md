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
qubit-task = { version = "0.8", features = ["sqlite"] }
tokio = { version = "1.53", features = ["macros", "rt-multi-thread"] }
```

The `sqlite` feature enables durable task history and recovery. The scheduler scans queued summaries in bounded pages and runs at most `max_running_tasks` handlers concurrently. A handler error is retried only when `retryable` is true, with a persisted deadline; the default is three attempts with exponential delay from one to sixty seconds. `resume_blocked` requeues a blocked task using its observed state version after configuration is repaired. Recovery is at-least-once: a task interrupted after an external side effect may run again, so application effects need idempotency or transaction protection. Scheduling is process-local; distributed scheduling and exactly-once business effects are outside the crate's guarantees.

With the optional `event-bus` feature, configure `event_notifications(bus, topic, queue_capacity, shutdown_flush_timeout)` on the builder to publish persisted lifecycle snapshots. Delivery is best-effort: a full local queue drops snapshots, provider failures are counted, and shutdown flushes only until its configured timeout. Queries remain authoritative; consumers should reconcile by `(TaskId, state_version)`. Progress updates do not publish lifecycle events.

## Documentation

- [User guide](doc/user-guide.md)
- [Detailed design](doc/task_execution_service_design.en.md)
- [Migration guide](doc/migration-0.8.en.md)
- [API reference](https://docs.rs/qubit-task)

## Task notification delivery

Task notifications are best-effort and are not backed by a transactional outbox. `notification_stats()` reports queued, published, dropped, and failed snapshots. `shutdown()` attempts to drain queued snapshots before releasing store ownership; timeout drops the remaining notifications and does not fail task shutdown. A successful publish receipt reports provider admission only, not subscriber processing or handler completion.

The scheduler scans ready queued work in keyset order. Tasks blocked on a temporarily unavailable resource are skipped so a later task with a satisfiable request can start; scheduling is therefore not strict FIFO. Applications can call `TaskStore::prune_terminal_before(finished_before_ms, max_rows)` to explicitly remove a bounded batch of old terminal history. Pruning also releases idempotency keys for reuse; queued and blocked tasks are retained.

## Checks

```bash
cargo test --all-features
./align-ci.sh
./ci-check.sh
```
