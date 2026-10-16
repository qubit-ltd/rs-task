# Qubit Task (`rs-task`)

[![Rust CI](https://github.com/qubit-ltd/rs-task/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-task/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-task/coverage-badge.json)](https://qubit-ltd.github.io/rs-task/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-task.svg?color=blue)](https://crates.io/crates/qubit-task)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![中文文档](https://img.shields.io/badge/文档-中文版-blue.svg)](README.zh_CN.md)

`qubit-task` runs long-lived application work as typed, queryable tasks. Applications submit a typed payload, receive a stable task ID, and can query lifecycle state and live progress. Handlers declare the task kind, payload type, supported schema versions, and cancellation mode. The service provides bounded in-process scheduling and optional SQLite persistence with at-least-once handler recovery.

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
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use qubit_codec::ValueBytesCodecDescriptor;
use qubit_codec::ValueBytesCodecRegistration;
use qubit_codec::ValueBytesCodecRegistry;
use qubit_codec::ValueCodecId;
use qubit_codec::ValueCodecRegistration;
use qubit_codec::ValueCodecRegistrationSource;
use qubit_codec::ValueDecoder;
use qubit_codec::ValueEncoder;
use qubit_id::Id;
use qubit_id::IdGenerationError;
use qubit_id::IdGenerator;
use qubit_model_id::HasModelId;
use qubit_model_id::ModelId;
use qubit_model_id::ModelIdBuf;
use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::TaskHandler;
use qubit_task::handler::CancellationMode;
use qubit_task::handler::TaskContext;
use qubit_task::handler::TaskHandlerDescriptor;
use qubit_task::handler::TaskRunOutcome;
use qubit_task::handler::TaskRunResult;
use qubit_task::model::ResourceCapacity;
use qubit_task::model::TaskOutput;
use qubit_task::model::TaskRequest;
use qubit_task::model::TaskState;
use qubit_task::store::MemoryTaskStore;
use qubit_task::store::TaskFuture;
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

struct EchoHandler;

impl TaskHandler<EchoPayload> for EchoHandler {
    fn run<'a>(&'a self, payload: EchoPayload, _context: TaskContext) -> TaskFuture<'a, TaskRunResult> {
        Box::pin(async move {
            println!("{}", payload.message);
            Ok(TaskRunOutcome::Succeeded(TaskOutput::default()))
        })
    }
}

struct SequentialIds(AtomicU64);

impl IdGenerator for SequentialIds {
    fn generate(&self) -> Result<Id, IdGenerationError> {
        Ok(Id::new(self.0.fetch_add(1, Ordering::Relaxed)))
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    runtime.block_on(async {
        let codec_registry = Arc::new(ValueBytesCodecRegistry::from_registrations([&JSON_CODEC])?);
        let mut builder = TaskExecutionServiceBuilder::new(
            Arc::new(MemoryTaskStore::new(256)),
            codec_registry,
            Arc::new(SequentialIds(AtomicU64::new(1))),
        )
        .capacity(ResourceCapacity {
            cpu_slots: 4,
            ..ResourceCapacity::default()
        });

        builder.handlers_mut().register::<EchoPayload, _>(
            TaskHandlerDescriptor {
                kind_id: "example.echo".into(),
                payload_type_id: ModelIdBuf::try_from("example.EchoPayload")?,
                accepted_schema_versions: vec![1],
                cancellation_mode: CancellationMode::Cooperative,
            },
            Arc::new(EchoHandler),
        )?;

        let service = builder.build().await?;
        let request = TaskRequest::new(
            "example.echo",
            1,
            ValueCodecId::new("example.echo.json"),
            EchoPayload {
                message: "typed task completed".into(),
            },
        );
        let accepted = service.submit(request).await?;
        let summary = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let summary = service.get(accepted.id).await?.expect("accepted task exists");
                if summary.state.is_terminal() {
                    return Ok::<_, Box<dyn std::error::Error>>(summary);
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await??;
        assert_eq!(summary.state, TaskState::Succeeded);
        println!("task {} is {:?}", accepted.id, summary.state);
        service.shutdown().await?;
        Ok::<(), Box<dyn std::error::Error>>(())
    })
}
```

`MemoryTaskStore` makes this suitable for a first run, not process-restart recovery. Use [`SqliteTaskStore`](doc/user-guide.md) when accepted work must survive a restart. A successful `submit()` means the request was accepted; the example separately waits for and asserts the handler's terminal result.

With the optional `event-bus` feature, configure `TaskExecutionServiceBuilder::event_bus` to publish lifecycle snapshots from a durable SQLite outbox. The task state and outbox row commit atomically. Until the provider reports acceptance, the row is retained and retried, so publication may be duplicated. After `Accepted`, the row is deleted; acceptance does not guarantee Redis persistence or consumer receipt, so consumer delivery is not guaranteed at least once. This is separate from at-least-once handler execution described above. The schema v8 migration preserves existing task, active outbox, and quarantine rows. `MemoryTaskStore` does not provide durable outbox support.

## Documentation

- [User guide](doc/user-guide.md)
- [Detailed design](doc/task_execution_service_design.en.md)
- [Migration guide to 0.10](doc/migration.md)
- [API reference](https://docs.rs/qubit-task)

## Task notification delivery

`PublishGuarantee::Accepted` describes the provider's acceptance boundary. It does not prove that an accepted notification has reached disk or a replica; a Redis failure before persistence can still lose a notification after its SQLite outbox row has been deleted. Consumers may also fail before receiving it, so this boundary is not an at-least-once delivery guarantee.

The publisher replays committed outbox rows after service recovery and drains them on shutdown up to `notification_shutdown_timeout`. An uncertain publish receipt or a crash after Redis accepts an event but before the outbox row is deleted can cause a duplicate. A successful Redis `XADD` means the stream accepted the entry; it does not prove a disk `fsync` or a consumer ACK. `DurabilityCapability::Durable` describes message retention without a subscriber, not those stronger guarantees. Consumers should retain the highest `state_version` per task and ignore duplicate or stale events. The stable `EventId` is for correlation; the Redis provider does not deduplicate by it. For the transactional checkpoint, version-gap refresh, and ACK/Retry order, see the [durable consumer projection guide](doc/user-guide.md#durable-consumer-projection). Enabling the publisher does not backfill lifecycle states committed before the service started with it. Monitor pending outbox row count and oldest-row age, together with Redis stream `XLEN` and consumer-group `XPENDING`.

The scheduler scans ready queued work in keyset order. A task waiting for a temporarily unavailable resource can be bypassed by newer tasks with available resources. After 32 successful bypasses by default, the scheduler gives the older task priority; configure the positive limit with `TaskExecutionServiceBuilder::max_resource_bypasses`. Reaching the limit can leave an unrelated resource idle while the older task waits. The count is process-local and resets after a restart. This is neither strict FIFO nor a bound on waiting time. Applications can call `TaskStore::prune_terminal_before(finished_before_ms, max_rows)` to explicitly prune a bounded batch of old terminal tasks. SQLite skips tasks with active outbox rows and stores a small terminal state checkpoint before deleting each task; checkpoints support current-state refresh after notification gaps and grow as terminal tasks are pruned. Pruning releases idempotency keys for reuse; queued and blocked tasks are retained.

Call `shutdown().await` when the caller needs to wait for active handlers and the notification drain, release store ownership, and receive the shared shutdown result, including errors. Dropping the last external service handle requests the same drain asynchronously; `Drop` does not wait or return its result. Cancelling a shutdown waiter does not cancel the drain. A handler that never finishes can keep ownership held. If owner release panics, waiters receive `StoreUnavailable` and the still-armed guard is handed to the cleanup worker; after a transient owner-release error, a later explicit `shutdown()` retries release. Cancelling a `build()` future after ownership is acquired also leaves release to the prestarted cleanup worker. That cleanup is eventual, so an immediate second build may briefly report an owner conflict.

## Checks

```bash
cargo test --all-features
./.infra/bin/align-ci.sh
./.infra/bin/ci-check.sh
```
