# Migrating to qubit-task 0.8

Version 0.8 contains intentional breaking changes to store pagination, typed scheduling queries, terminal pruning, and task notifications. Update downstream `TaskStore` implementations and callers together; no compatibility overloads are retained.

## Implement the typed scheduler and retention methods

Custom `TaskStore` implementations must add `list_ready_queued(after, limit, now_ms)`, `next_retry_deadline(now_ms)`, and `prune_terminal_before(finished_before_ms, max_rows)`. Ready scans return only `Queued` records with no retry deadline or a deadline at or before `now_ms`, ordered by `(accepted_at_ms, numeric TaskId)` and bounded to 1–256 rows. `next_retry_deadline` returns the minimum queued deadline strictly after `now_ms`. Pruning removes at most the requested count of `Succeeded`, `Failed`, `Panicked`, or `Cancelled` records whose `finished_at_ms` is strictly before the cutoff, ordered by `(finished_at_ms, TaskId)`. Remove idempotency keys atomically with rows; a pruned key becomes reusable. Queued and blocked records are never eligible.

## Configure lifecycle notifications explicitly

`TaskEvent.task_id` is now typed `TaskId` and each payload includes `schema_version = 1`. Update JSON codecs and consumers together. With the `event-bus` feature, opt in through `event_notifications(async_bus, topic, queue_capacity, shutdown_flush_timeout)`. The service publishes snapshots only after store commits; progress is excluded. A full bounded queue drops snapshots, provider errors increment `failed`, and `notification_stats()` exposes cumulative counts. `shutdown()` attempts to flush before releasing store ownership; after timeout it aborts the publisher, counts remaining items as dropped, and still completes task shutdown. Notifications have no transactional outbox and may be lost, duplicated, or reordered; reconcile from task queries and deduplicate by `(TaskId, state_version)`. Scheduling can let a later resource-compatible task pass an earlier blocked task, so do not rely on strict FIFO.

## Replace task-ID recovery cursors

`TaskId` identifies one task. It cannot identify a position in a history that can contain tasks accepted during the same millisecond. `TaskCursor` carries both the acceptance timestamp and the ID tie-breaker. Update custom store signatures:

```rust
// Before
fn scan_unfinished<'a>(&'a self, cursor: Option<TaskId>) -> TaskFuture<'a, Result<StoredTaskPage, StoreError>>;

// After
fn scan_unfinished<'a>(&'a self, cursor: Option<TaskCursor>) -> TaskFuture<'a, Result<RecoveryPage, StoreError>>;
```

Import `TaskCursor` and `RecoveryPage` from `qubit_task::model`. `RecoveryPage.next` is `Option<TaskCursor>`; `TaskPage.next` and `TaskQuery.after` use the same cursor type. Cursors are exclusive and history is ordered by `(accepted_at_ms ASC, id ASC)`. The breaking field change is `RecoveryPage.next: Option<TaskId>` → `RecoveryPage.next: Option<TaskCursor>`.

Copy `page.next` into the next request; do not synthesize a cursor from an ID alone:

```rust
let mut after = None;
loop {
    let page = store.scan_unfinished(after).await?;
    process(page.tasks).await?;
    after = page.next;
    if after.is_none() {
        break;
    }
}
```

`next` is absent on the terminal page, including a full page. Recovery results contain payload-free summaries, and implementations must preserve strict ordering and exclusive continuation.

## Treat finalizer faults as service faults

The attempt finalizer is supervised independently of the handler future. If its transition or bookkeeping code panics, the service latches `StoreUnavailable`, closes admission, and proactively starts the shared shutdown coordinator. It reports the task and attempt in diagnostics, reclaims in-process finalizer accounting, and keeps store ownership until shutdown barriers are satisfied. A panic does not manufacture a successful or terminal task state. Observe and await `shutdown()`, then repair or restart; do not open the same durable store in another process while shutdown is incomplete.

## SQLite schema 3 indexes

Opening an existing schema-3 database now ensures the required history and unfinished-work indexes. If a known partial index has an older same-name definition, SQLite rebuilds it transactionally during open. The schema and record format remain version 3; this index repair does not rewrite task records or metadata. Back up the database before deployment as for any storage upgrade, and allow the first open to finish before serving traffic.

## Validate the typed store API from an external crate

The standalone [typed-store consumer](../tests/fixtures/typed-store-consumer/) exercises `accept_encoded`, `get_encoded_task`, start/transition, and query through the public `TaskStore` trait. It always checks the memory store; enabling `sqlite` also checks a disposable `SqliteTaskStore::open_next` database and removes its temporary directory on exit. Run it with `cargo run --manifest-path tests/fixtures/typed-store-consumer/Cargo.toml --features sqlite,typed-contract`. This is an API smoke test, not a substitute for backend-specific crash, write-draining, and transaction-interruption tests.

`.infra/tools/verify-packaged-consumer.sh` checks the packed crate and registry-resolved consumer with no sibling path override when explicitly run. A script that has not run successfully is not evidence that package validation passed.

The strict package flow first reviews `cargo package --list`, then runs Cargo verification without `--no-verify`:

```sh
cargo package --manifest-path /path/to/rs-task/Cargo.toml \
  --target-dir /tmp/superpowers-rs-task-gc6n4m1u/package-target \
  --locked --allow-dirty --no-default-features --features sqlite
```

`--allow-dirty` includes reviewed worktree changes; it does not disable package verification. Extract `package/qubit-task-0.8.0.crate` under a dedicated validated temporary workspace, point a separate consumer manifest at that extracted directory, and keep the consumer manifest free of `[patch]`. Use a fresh `CARGO_HOME`, copy the user's Cargo registry/authentication configuration and credentials without printing them, reject path patches in its config and in Cargo config files along the command's working-directory ancestry, and run the consumer's no-default, single-feature, combined-feature and all-features checks plus the contract suites. Print the resolved registry sources. If registry resolution fails, report it as blocked; never substitute a sibling checkout or claim a package pass.

Recovery remains at-least-once. A process can perform an external business side effect and crash before storing its terminal task transition, so the task may run again. Use idempotency keys or an application transaction/outbox; exactly-once business effects are not provided by the task store.
