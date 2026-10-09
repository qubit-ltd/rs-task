# Migration to qubit-task 0.10

[简体中文](migration.zh_CN.md) · [User guide](user-guide.md) · [Typed task API](typed-task-api.md)

This is the current migration guide for the typed task API in `qubit-task` 0.10. The former byte-oriented UUID API is not supported by this guide. If you are migrating from 0.8, use the [historical 0.8 notes](migration-0.8.en.md) only to identify changes made in that release; verify every example against this 0.10 guide.

## Replace the SQLite constructor

The typed SQLite store is opened with `SqliteTaskStore::open`:

```rust,no_run
use qubit_task::store::SqliteTaskStore;

let store = SqliteTaskStore::open("tasks.sqlite")?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

Replace `SqliteTaskStore::open_next` with `SqliteTaskStore::open`. The 0.10 API has one typed SQLite entry point; do not keep a fallback to the old UUID store. Opening a typed schema version 4 or 5 migrates it transactionally to version 6 and preserves task rows. A database using the legacy UUID schema is rejected with an explicit error and remains unchanged. Map legacy records into a new typed database through an application-controlled migration before opening it with 0.10.

## Implement the current `TaskStore` contract

The public `TaskStore` is the typed persistence contract implemented directly by `MemoryTaskStore` and `SqliteTaskStore`. Custom implementations should use the current `qubit_task::model` types and support typed acceptance, task and summary reads, lifecycle transitions with expected state versions, progress updates, keyset history queries, ownership fencing, and terminal pruning as required by the methods they implement. Remove adapters based on the former UUID request and recovery types; `LegacyTaskStore`, `RecoveryPage`, and `scan_unfinished` are not part of the 0.10 public contract.

Use `TaskId` for task identity, `kind_id` for handler routing, `category` for application filtering, and `Payload<T>` for the typed payload identity (`type_id`, `schema_version`, and `codec_id`). A history page uses an exclusive keyset cursor ordered by `(accepted_at_ms, numeric TaskId)`, not an offset. See the [typed task API guide](typed-task-api.md) for the full workflow and current method details.

## Preserve notification delivery boundaries

When lifecycle notifications are enabled, SQLite writes the task transition and outbox row in one transaction. The publisher removes a row only after the provider reports acceptance. Delivery is at-least-once: an uncertain result or a crash after provider acceptance but before row deletion can produce duplicates. Consumers should deduplicate by `(TaskId, state_version)` and query the task service for authoritative state. This does not provide exactly-once delivery or prove that an accepted event reached disk, a replica, or a consumer.

The outbox is available with `SqliteTaskStore`; `MemoryTaskStore` does not provide durable outbox support. Enabling publication does not backfill lifecycle states committed before the outbox was enabled. For provider requirements, consumer checkpoints, monitoring, and shutdown behavior, see [publishing lifecycle changes](user-guide.md#publish-lifecycle-changes).

## Validate an application migration

Update application code, custom stores, examples, and lockfiles together. Compile against the 0.10 public API and verify at least: typed submission and handler lookup; version-checked state updates; keyset pagination; reopening the migrated SQLite database; explicit rejection without modification of a legacy UUID database; and duplicate-safe outbox consumption if notifications are enabled. Do not treat the historical 0.8 API snippets as 0.10 examples.
