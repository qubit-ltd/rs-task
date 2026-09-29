# qubit-task: Resource-Aware Asynchronous Task Service Design

[中文设计文档](task_execution_service_design.md)

## Runtime reliability details

Cloned `TaskExecutionService` handles share one lease. Dropping the final
handle starts the common drain coordinator, which waits for accepted work,
closes the notification publisher, and releases recoverable-store ownership.
Drop cannot wait for or report shutdown errors; callers that need the result
must await `shutdown()`. An injected Tokio runtime must remain alive until the
drain finishes.

The scheduler loop is supervised for panics. A panic from the policy or engine
becomes `TaskServiceError::SchedulerUnavailable`, wakes task waiters, closes
admission, and stops future scheduling; the loop is not restarted. Execution
handles already returned by the engine remain tracked until they finish, before
store ownership is released. A custom engine must return a trackable handle
after starting side effects; if it panics after starting work without
returning one, the service cannot prove that work stopped, so the application
must terminate the process and recover under external supervision.

`TaskStore::has_unfinished_over_limit(limit)` checks only queued and running
records, uses strict greater-than semantics, and does not read payloads. SQLite
uses a state-index existence query; paged recovery recounts and validates
cursors to handle changes between precheck and scan. Custom stores must
implement this check and `count_states()`.

`ResourceRequest.cpu_slots = 0` is suitable for asynchronous I/O, but such work
still consumes a `max_running_tasks` slot. CPU-bound work should request at
least one CPU slot and run blocking code on `spawn_blocking` or a dedicated
backend.

## 1. Purpose and boundaries

`qubit-task` accepts work that cannot finish during the caller's request, schedules it against local CPU, GPU, and named resource budgets, and exposes task state through `TaskExecutionService`. The service is process-local. It does not provide distributed scheduling, workflow dependencies, cron scheduling, forced interruption of arbitrary code, or exactly-once business side effects.

Applications may assemble a `TaskStore`, `TaskExecutionEngine`, `SchedulingPolicy`, and versioned `TaskHandler` directly, or use the typed `qubit-spi` provider families. The selected store defines whether history is volatile, persistent, or restart-recoverable. Capabilities are observable through `capabilities()`; the facade does not silently substitute a weaker backend.

## 2. Task model and lifecycle

A `TaskRequest` contains a stable task type, exact handler version, bounded opaque payload, resource demand, and optional correlation and idempotency keys. The service creates a stable `TaskId`. A repeated idempotency key returns the retained original record only when the request matches; callers should persist the key before submission and retry an uncertain submission with the same key and request.

Handlers are registered by `(task_type, handler_version)` before build. A missing handler during recovery moves the task to `Blocked` and preserves it for inspection. To recover it, stop the old service, register the exact handler version, build again against the same database, query the task's `Blocked` summary, and call `retry_blocked`. The handler registry is fixed after build; restart does not automatically requeue the task.

`submit_local` is for process-local closures and returns a typed `LocalTaskHandle`; it is unavailable with a restart-recoverable store. Durable work uses versioned `TaskRequest` data. `TaskSummary` omits payload, while `get` is the explicit full-record read. Large results belong in application-managed storage; task output is a bounded summary or reference.

The public lifecycle is `Queued`, `Running`, `Blocked`, `Succeeded`, `Failed`, `Panicked`, and `Cancelled`. Conditional transitions use revisions and attempts to reject stale updates. `cancel` immediately finalizes queued work; running work receives a cooperative cancellation request. A handler must acknowledge cancellation for the task to become `Cancelled`; a successful or failed handler result remains authoritative.

## 3. Resource scheduling

`ResourceCapacity` configures CPU slots, GPU devices and labels, and named integer resources. A request that can never fit is rejected; a valid request waits when resources are temporarily occupied. `try_prepare` must reserve all requested resources promptly and synchronously, without invoking handler work. The service persists `Running` before `activate`, so user code does not begin before the state transition succeeds. A prepared reservation dropped before activation releases its resources.

A single resource ledger mutex protects current usage, token allocation, and active allocations. Release removes an allocation and updates usage in the same critical section. Handler completion releases engine resources before the service persists the terminal result and wakes waiters. `cpu_slots = 0` is useful for asynchronous I/O, but such tasks still count against `max_running_tasks`.

The scheduler uses a bounded ready queue and an ordered retry-deadline queue. It examines at most `scan_budget` eligible candidates per pass. Fitting work may pass an unavailable head task, while bounded bypass accounting reserves capacity for repeatedly skipped work. The queue's candidate extraction runs in O(N + K) for N queued tasks and K selected IDs, preserving the caller's requested result order.

Resource descriptions are bounded: at most 32 GPU labels and 32 custom names, each non-empty and at most 128 UTF-8 bytes. GPU labels require `gpu_count > 0`. Task type, version, correlation and idempotency keys, payload, metadata, and output have their own documented limits. Both built-in stores apply request validation on acceptance.

## 4. External writes, cancellation, and shutdown

`submit`, `submit_local`, `cancel`, `retry_blocked`, `abandon_blocked`, and `prune_terminal_before` share a default budget of 64 in-flight write operations. Submission payloads also share a separate 64 MiB in-flight byte budget. A full operation budget promptly returns `OperationLimitExceeded`; cancellation is subject to that same limit and may need to be retried later.

Once a write worker is admitted, dropping or timing out the caller only drops its response wait. The service-owned worker retains its admission and budget reservations and completes store and local side effects. This is especially important for running cancellation: after `cancel_requested` is committed, the worker still signals the matching task attempt even if the caller stops waiting. Cancellation remains cooperative; arbitrary user code is not forcibly terminated.

Shutdown closes admission and waits for accepted write operations, scheduler work, tracked attempts, and store owner release. A shutdown deadline limits only that caller's wait. It does not release ownership while work is still running. The injected Tokio runtime must remain alive until asynchronous draining finishes. Store `release_owner(epoch)` is a completion barrier: a recoverable provider must not leave an earlier write running after successful release.

## 5. Store and SQLite ownership

`TaskStore` separates full record reads from payload-free summaries. `scan_unfinished` returns `RecoveryPage<TaskSummary>` so recovery does not load task payloads. SQLite selects only summary columns and bounds each page to 256 tasks, querying one extra summary to determine whether another page exists. Third-party stores must preserve this payload-free contract, validate new requests, implement aggregate state counts, and ensure `release_owner` drains outstanding writes.

SQLite opens the canonical database path and takes an OS lock associated with the canonical physical file before schema access. The `.owner.lock` suffix is appended to the complete filename (`jobs.sqlite.owner.lock`); database names need not be UTF-8. Unix and Windows implementations verify physical identity and link count. Databases with multiple hard links are rejected, as are platforms where a reliable identity cannot be obtained. The containing directory and lock file must remain trusted and stable while the service is open.

Opening the database holds the physical-file lock; `TaskStore::acquire_owner` separately issues the service epoch. Re-acquiring through a released store instance is unsupported; reopen a new instance. The lock-name change requires an offline upgrade: stop old processes, let them drain, back up the database, deploy the new version, and reopen the same database. The task table schema remains unchanged, and schema 0, 1, and 2 migrations to schema 3 remain supported.

## 6. Lifecycle notifications

When the optional `event-bus` feature is enabled, the service publishes `TaskEvent` after state changes. Publication is best effort and is not part of the store transaction. Events may be delayed, repeated, or missing; `state_version` helps consumers detect stale notifications, and the task store remains authoritative. The crate does not claim a transactional outbox or exactly-once delivery. A provider receipt means the provider accepted the publish operation, not that a subscriber completed processing.

## 7. Core ordering invariants

```text
Submit: validate -> reserve operation/payload budget -> persist acceptance -> publish local scheduling signal
Start: select candidate -> reserve engine resources -> persist Running -> activate handler
Finish: handler completes -> release engine resources -> persist terminal state -> wake waiters -> publish notification
Cancel: persist request -> signal the matching running attempt -> return response when available
Recover: acquire physical owner -> acquire owner epoch -> scan summary pages -> repair interrupted states -> build queue -> start scheduler
```

Locks protecting resource accounting or queue state are not held across user callbacks, store awaits, or event publishing. State transitions compare both revision and attempt where relevant. A stale completion must not overwrite a newer attempt. Store failure pauses acceptance and scheduling rather than pretending an uncertain write was rolled back.

## 8. Validation and migration

The CI matrix checks all eight combinations of `sqlite`, `inventory`, and `event-bus`; default features remain empty. SQLite has an explicit provider registry so applications do not need `inventory` merely to select the built-in store. The inventory feature adds linked-provider discovery.

The public API intentionally replaces the submission-only limit with `max_inflight_operations` and `OperationLimitExceeded`, and replaces `StoredTaskPage<StoredTask>` with `RecoveryPage<TaskSummary>`. No compatibility aliases are retained. The design documents and user guides describe caller cancellation, shared budgets, resource limits, owner-lock prerequisites, and recovery of missing handlers.

## 9. Payload-free status and blocked-task operations

History pages, `wait`, `retry_blocked`, and `get_summary` return
`TaskSummary`, which omits the payload. `get` is the explicit full-record
query, and `get_by_idempotency_key` also returns a payload-free summary; call
`get(summary.id)` only when payload access is needed. SQLite schema 3 separates
request metadata, payload BLOB, and lifecycle JSON, so history, wait checks,
and transitions avoid selecting or decoding payloads. Schema 0, 1, and 2
migrations preserve payload, idempotency, lifecycle, ordering, and ownership.

A store failure wakes waiters and local handles immediately. Shared shutdown
still waits for scheduler termination and tracked execution handles before
releasing ownership; `shutdown_until` only limits the caller's wait. Operators
select aged blocked summaries and pass the observed `state_version` to
`abandon_blocked`. A stale version conflicts, and only terminal records can
be pruned in bounded batches.
