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

The `sqlite` feature enables durable task history and recovery. Omit it for in-memory execution. Recovery is at-least-once: a task interrupted after an external side effect may run again, so application effects need idempotency or transaction protection. Scheduling is process-local; distributed scheduling and exactly-once business effects are outside the crate's guarantees.

The optional `event-bus` feature provides `TaskEvent` transport integration. The typed execution service does not currently publish lifecycle events; query the service for authoritative state.

## Documentation

- [User guide](doc/user-guide.md)
- [Detailed design](doc/task_execution_service_design.en.md)
- [Migration guide](doc/migration-0.8.en.md)
- [API reference](https://docs.rs/qubit-task)

## Task notification delivery

Task notifications are best-effort. A successful publisher `close` means the local queue worker drained and stopped; inspect notification statistics to determine provider publication outcomes. Do not treat close success as destination admission or handler completion.

## Checks

```bash
cargo test --all-features
./align-ci.sh
./ci-check.sh
```
