# qubit-task User Guide

Use `qubit_id::Id` for every task identifier. `TaskExecutionService::submit_callable` returns a `TaskHandle` that owns the typed result while the service records lifecycle status.

```rust
use qubit_id::Id;
use qubit_task::service::TaskExecutionService;

let service = TaskExecutionService::new()?;
let handle = service.submit_callable(Id::new(42), || Ok::<_, ()>(21))?;
assert_eq!(handle.get()?, 21);
service.wait_for_idle();
```

`wait_for_current_tasks` waits for the snapshot observed at entry; `wait_for_idle` waits until no reservation or accepted task remains. IDs can be reused after terminal completion. Cancellation only succeeds before execution starts.
