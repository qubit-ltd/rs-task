# Qubit Task

[![Rust CI](https://github.com/qubit-ltd/rs-task/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-task/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-task/coverage-badge.json)](https://qubit-ltd.github.io/rs-task/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-task.svg?color=blue)](https://crates.io/crates/qubit-task)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![中文文档](https://img.shields.io/badge/文档-中文版-blue.svg)](README.zh_CN.md)

`qubit-task` lets Rust services accept work that outlives a request, schedule it against CPU, GPU, and named resource budgets, and expose one queryable task service. It is for applications that need bounded background execution and task history without coupling business code to a particular store or execution engine. Storage, scheduling, execution, and versioned handlers can be assembled directly or selected through `qubit-spi`.

## Install

```toml
[dependencies]
qubit-task = "0.6"
tokio = { version = "1.53", features = ["macros", "rt-multi-thread"] }
```

## Start with volatile local work

This named preset keeps task state in memory. Pending tasks are lost when the
process exits; completed history is bounded to 1024 records.

```rust,no_run
use qubit_task::TaskExecutionService;
use qubit_task::model::TaskOutput;
use qubit_task::service::LocalTaskOutcome;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let service = TaskExecutionService::in_memory().await?;
    let handle = service.submit_local(|_| LocalTaskOutcome::<String, std::io::Error>::Succeeded {
        value: "finished".to_owned(),
        summary: TaskOutput { summary: b"finished".to_vec() },
    }).await?;
    let value = handle.result().await??;
    assert_eq!(value, "finished");
    service.shutdown().await?;
    Ok(())
}
```

For work that must survive restart, enable `sqlite` and use
`TaskExecutionServiceBuilder::recoverable_sqlite(path)`. Register a handler for
each stored `(task_type, handler_version)` before building the service. See the
[user guide](doc/user-guide.md) for recovery, resource scheduling, SPI assembly,
and event notifications. Optional lifecycle notifications use a bounded queue
(256 entries by default); a full queue drops the notification, and shutdown
drains queued notifications unless the publisher worker panics. A synchronous
event-bus provider can block the dedicated publisher thread, so shutdown can
wait indefinitely on such a provider.

`LocalTaskHandle<R, E>` returns the full process-local value or application
error. For running work, `LocalTaskResultError::Cancelled` is delivered only
after the handler acknowledges cancellation with `LocalTaskOutcome::Cancelled`;
`cancel_requested` alone is only a request. Queued tasks cancelled before
execution are finalized directly by the service. Durable work instead uses a
versioned `TaskRequest`, and its small persisted `TaskRecord.output` is a
summary or reference rather than the full result.
Custom `TaskStore` providers must implement `count_states()` as one aggregate
over retained records. `stats()` uses that aggregate once and then reads engine
resources; these are adjacent snapshots, not one atomic snapshot.

## Why this project exists

A request handler can submit an import, return a task ID, and let a versioned handler process the payload under the service's resource limits. The caller can then query progress without keeping the original request open. Choose `submit_local` when the result must return to code in the same process; use `TaskRequest` when the work must be reconstructable or recoverable after restart.

The crate provides bounded queues, resource-aware scheduling, local or pluggable execution, query and cancellation APIs, and optional SQLite recovery and lifecycle notifications. It does not provide distributed multi-node scheduling, workflow dependencies, cron scheduling, forced interruption of arbitrary code, or exactly-once business side effects.

## Project documents

- [User guide](doc/user-guide.md)
- [中文 README](README.zh_CN.md)
- [Detailed TaskExecutionService design](doc/task_execution_service_design.md)
- [中文用户指南](doc/user-guide.zh_CN.md)

## API and storage contracts

`TaskQuery.states` uses `TaskStateKind`; migrate callers that previously built
filters from payload-bearing `TaskState` values. Request text limits are
measured in UTF-8 bytes: task type 128, handler version 64, correlation and
idempotency keys 256 each, and metadata 32 entries, 128-byte keys, 4096-byte
values, and 16384 combined bytes. Persisted diagnostic categories are limited
to 128 bytes and messages to 4096 bytes; execution diagnostics are truncated at
a UTF-8 boundary. SQLite runs one blocking database operation at a time.
Shutdown rejects new service writes, and a SQLite handle cannot write after
releasing its ownership.

## Testing

```bash
# Run tests with the default feature set
cargo test

# Run tests with all declared features
cargo test --all-features

# Project CI checks
./ci-check.sh

# Check code coverage
./coverage.sh
```

## License

Copyright (c) 2025 - 2026. Haixing Hu. All rights reserved.

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for the
full license text.

## Contributing

Contributions are welcome. Please follow the Rust API guidelines, keep public
API documentation and tests current, and run `./align-ci.sh` to format code and
`./ci-check.sh` to satisfy CI requirements before submitting a pull request.

## Author

**Haixing Hu** - *Qubit Co. Ltd.*

Repository: [https://github.com/qubit-ltd/rs-task](https://github.com/qubit-ltd/rs-task)
