# Qubit Task (`rs-task`)

[![Rust CI](https://github.com/qubit-ltd/rs-task/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-task/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-task/coverage-badge.json)](https://qubit-ltd.github.io/rs-task/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-task.svg?color=blue)](https://crates.io/crates/qubit-task)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![中文文档](https://img.shields.io/badge/文档-中文版-blue.svg)](README.zh_CN.md)

`qubit-task` runs long-lived application work as typed, queryable tasks. Applications submit a typed payload, receive a stable task ID, and can query lifecycle state and live progress. Handlers declare the task kind, payload type, supported schema versions, and cancellation mode. The service provides bounded in-process scheduling and optional SQLite persistence with at-least-once handler recovery.

Typed payload identity comes from the submitted Rust type's `HasModelId` implementation. `TaskKind<T>` binds the task kind, schema version, and codec ID to `T`; its `request` method builds a request without a separately supplied model ID.

## Typed API

The public API keeps these identifiers separate:

- `TaskId` identifies one task.
- `kind_id` routes a task to a handler.
- `category` classifies tasks for application queries.
- `TaskKind<T>` binds a kind, schema version, codec, and payload model; `Payload<T>` carries that model identity with the encoded value.

The [typed task API guide](doc/typed-task-api.md) contains the complete example and contracts for handler registration, codec reuse, resource quotas, cancellation, progress reporting, metadata limits, and keyset pagination. The [user guide](doc/user-guide.md#start-and-shut-down-the-service) covers application startup and shutdown operations, and its [history and quarantine maintenance section](doc/user-guide.md#maintain-history-and-quarantine-archives) documents bounded archive cleanup. Runnable examples are [`examples/task_service.rs`](examples/task_service.rs) and [`examples/blocked_maintenance.rs`](examples/blocked_maintenance.rs).

## Installation

```toml
[dependencies]
qubit-task = { version = "0.12" }
qubit-codec = { version = "0.17", features = ["registry"] }
qubit-model-id = "0.2"
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
tokio = { version = "1.54", features = ["macros", "rt-multi-thread", "time"] }
```

Enable the `sqlite` feature when task history and recovery must survive a process restart. The scheduler scans queued summaries in bounded pages and runs at most `max_running_tasks` handlers concurrently. A handler error is retried only when `retryable` is true, with a persisted deadline; positive delays are rounded up to whole milliseconds for that deadline. The default is three attempts with exponential delay from one to sixty seconds. After repairing configuration, pass the observed `TaskSummary` to `resume_blocked` to requeue a blocked task. Recovery is at-least-once: a task interrupted after an external side effect may run again, so application effects need idempotency or transaction protection. Scheduling is process-local; distributed scheduling and exactly-once business effects are outside the crate's guarantees.

## Quick start

This in-memory example models a small application task: register a typed payload and handler, submit work, wait up to five seconds for completion, check the terminal state, and shut the service down. It is an onboarding path; use the [user guide](doc/user-guide.md) for SQLite recovery and [durable lifecycle notifications](doc/user-guide.md#publish-lifecycle-changes). The same source is compiled as a standalone fixture at [`tests/fixtures/readme-quick-start`](tests/fixtures/readme-quick-start).

```rust
// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::error::Error;
use std::time::Duration;

use qubit_codec::ValueBytesCodecDescriptor;
use qubit_codec::ValueBytesCodecRegistration;
use qubit_codec::ValueBytesCodecRegistry;
use qubit_codec::ValueCodecId;
use qubit_codec::ValueCodecRegistration;
use qubit_codec::ValueCodecRegistrationSource;
use qubit_codec::ValueDecoder;
use qubit_codec::ValueEncoder;
use qubit_model_id::HasModelId;
use qubit_model_id::ModelId;
use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::TaskKind;
use qubit_task::TaskState;
use qubit_task::handler::TaskContext;
use qubit_task::handler::TaskRunOutcome;
use qubit_task::handler::TaskRunResult;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Deserialize, Serialize)]
struct EchoPayload {
    message: String,
}

impl HasModelId for EchoPayload {
    const MODEL_ID: ModelId = ModelId::new("example.EchoPayload");
}

#[derive(Default)]
struct JsonCodec;

impl ValueEncoder<EchoPayload> for JsonCodec {
    type Output = Vec<u8>;
    type Error = serde_json::Error;

    fn encode(&mut self, value: &EchoPayload) -> Result<Self::Output, Self::Error> {
        serde_json::to_vec(value)
    }
}

impl ValueDecoder<[u8]> for JsonCodec {
    type Output = EchoPayload;
    type Error = serde_json::Error;

    fn decode(&mut self, bytes: &[u8]) -> Result<Self::Output, Self::Error> {
        serde_json::from_slice(bytes)
    }
}

static JSON_DESCRIPTOR: ValueBytesCodecDescriptor = ValueBytesCodecDescriptor::of::<JsonCodec, EchoPayload>();
static JSON_CODEC: ValueBytesCodecRegistration = ValueCodecRegistration::new(
    ValueCodecId::new("example.echo.json"),
    &JSON_DESCRIPTOR,
    ValueCodecRegistrationSource::new("example", "rs-task", "readme_quick_start", 1),
);

async fn run_echo(payload: EchoPayload, _context: TaskContext) -> TaskRunResult {
    println!("{}", payload.message);
    Ok(TaskRunOutcome::success())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let codecs = ValueBytesCodecRegistry::from_registrations([&JSON_CODEC])?;
    let echo = TaskKind::<EchoPayload>::new("example.echo", 1, ValueCodecId::new("example.echo.json"));
    let mut builder = TaskExecutionServiceBuilder::in_memory(256, codecs);
    builder.register_handler_fn(echo.descriptor(), run_echo)?;

    let service = builder.build().await?;
    let accepted = service
        .submit(echo.request(EchoPayload {
            message: "typed task completed".into(),
        }))
        .await?;
    println!("accepted={} task={}", accepted.created, accepted.summary.id);

    let terminal = service
        .wait_terminal(accepted.summary.id, Duration::from_secs(5))
        .await?;
    assert_eq!(terminal.state, TaskState::Succeeded);
    println!("task {} is {:?}", terminal.id, terminal.state);

    service.shutdown().await?;
    Ok(())
}
```

`MemoryTaskStore` makes this suitable for a first run, not process-restart recovery. Use [`SqliteTaskStore`](doc/user-guide.md) when accepted work must survive a restart. A successful `submit()` means the request was accepted; the example separately waits for and asserts the handler's terminal result.

With the optional `event-bus` feature, configure `TaskExecutionServiceBuilder::event_bus` to publish lifecycle snapshots from a durable SQLite outbox. The task state and outbox row commit atomically. Until the provider reports acceptance, the row is retained and retried, so publication may be duplicated. After `Accepted`, the row is deleted; acceptance does not guarantee Redis persistence or consumer receipt, so consumer delivery is not guaranteed at least once. This is separate from at-least-once handler execution described above. The schema v8 migration preserves existing task, active outbox, and quarantine rows. `MemoryTaskStore` does not provide durable outbox support.

## Documentation

- [User guide](doc/user-guide.md)
- [Detailed design](doc/task_execution_service_design.en.md)
- [API reference](https://docs.rs/qubit-task)

## Task notification delivery

`PublishGuarantee::Accepted` describes the provider's acceptance boundary. It does not prove that an accepted notification has reached disk or a replica; a Redis failure before persistence can still lose a notification after its SQLite outbox row has been deleted. Consumers may also fail before receiving it, so this boundary is not an at-least-once delivery guarantee.

The publisher replays committed outbox rows after service recovery and drains them on shutdown up to `notification_shutdown_timeout`. An uncertain publish receipt or a crash after Redis accepts an event but before the outbox row is deleted can cause a duplicate. A successful Redis `XADD` means the stream accepted the entry; it does not prove a disk `fsync` or a consumer ACK. `DurabilityCapability::Durable` describes message retention without a subscriber, not those stronger guarantees. Consumers should retain the highest `state_version` per task and ignore duplicate or stale events. The stable `EventId` is for correlation; the Redis provider does not deduplicate by it. A persistent consumer should also register each accepted `TaskId` in its own durable reconciliation set and periodically check bounded batches after the application's chosen staleness threshold. Query `TaskExecutionService::get_state_checkpoint` outside the consumer transaction, then re-read the consumer version inside the transaction and write only if the authoritative version is newer. Keep tracking after a query error, `None`, or stale checkpoint; retire it after a terminal projection commits. A refresh converges the projection to current state but cannot replay missing transitions or their business side effects, and it does not provide exactly-once processing. For the transaction and ACK/Retry path, see the [durable consumer projection guide](doc/user-guide.md#durable-consumer-projection). Enabling the publisher does not backfill lifecycle states committed before the service started with it. Monitor pending outbox row count and oldest-row age, together with Redis stream `XLEN` and consumer-group `XPENDING`.

The scheduler scans ready queued work in keyset order. A task waiting for a temporarily unavailable resource can be bypassed by newer tasks with available resources. After 32 successful bypasses by default, the scheduler gives the older task priority; configure the positive limit with `TaskExecutionServiceBuilder::max_resource_bypasses`. Reaching the limit can leave an unrelated resource idle while the older task waits. The count is process-local and resets after a restart. This is neither strict FIFO nor a bound on waiting time. Applications can call `TaskStore::prune_terminal_before(finished_before_ms, max_rows)` to explicitly prune a bounded batch of old terminal tasks. SQLite skips tasks with active outbox rows and stores a small terminal state checkpoint before deleting each task; checkpoints support current-state refresh after notification gaps and grow as terminal tasks are pruned. Pruning releases idempotency keys for reuse; queued and blocked tasks are retained. Applications must generate task IDs that remain unique across process restarts; SQLite reserves pruned IDs while their checkpoints exist, but `MemoryTaskStore` does not retain pruned identities.

Stop the application's own request or message intake before calling `shutdown().await`. Shutdown waits for active handlers and hooks, drains notifications, releases store ownership, and returns its result. Queued tasks remain for later recovery. Dropping the last service handle only requests an asynchronous drain; cancelling a shutdown waiter does not cancel that drain. A timeout does not prove ownership is released, and a handler that never finishes can keep it held. Wait for shutdown to confirm owner release before opening a replacement service; inspect the durable outbox after a notification close error. If owner release panics, waiters receive `StoreUnavailable` and the still-armed guard goes to the cleanup worker; after a transient owner-release error, a later explicit `shutdown()` retries. See the [user guide shutdown steps](doc/user-guide.md#start-and-shut-down-the-service) before deciding when to build a replacement service.

## Testing

Run the project checks from the repository root:

```bash
cargo test
cargo test --all-features
./.infra/bin/ci-check.sh
./.infra/bin/coverage.sh
```

## License

This project is licensed under the [Apache License, Version 2.0](LICENSE).

## Contributing

Contributions should follow the repository's CI formatting and validation rules. Run `./.infra/bin/align-ci.sh` to align files with CI before submitting a change, then run the relevant checks above.

## Author

Haixing Hu · [qubit-ltd/rs-task](https://github.com/qubit-ltd/rs-task)
