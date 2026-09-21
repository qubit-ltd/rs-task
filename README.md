# Qubit Task

[![Rust CI](https://github.com/qubit-ltd/rs-task/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-task/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-task/coverage-badge.json)](https://qubit-ltd.github.io/rs-task/coverage/)

Task-oriented execution services built on `qubit-executor` and `qubit-thread-pool`.

`TaskExecutionService` accepts a caller-provided task ID, runs a synchronous
callable on a thread pool, and keeps an in-memory status for lookup and
pre-start cancellation. The returned `TaskHandle` owns the typed result.

```rust
use qubit_task::service::{Id, TaskExecutionService, TaskStatus};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let service = TaskExecutionService::builder()
        .completed_history_capacity(128)
        .build()?;
    let id = Id::new(42);
    let handle = service.submit_callable(id, || Ok::<u32, std::io::Error>(7))?;
    assert_eq!(handle.get()?, 7);
    assert_eq!(service.status(id), Some(TaskStatus::Succeeded));
    service.shutdown();
    service.wait_termination();
    Ok(())
}
```

The service retains the latest 1024 terminal statuses by default. Set
`completed_history_capacity(0)` to retain none. History is bounded and is not
persistent storage: `status(id)` returns `None` after eviction. A task ID can
be reused as soon as its previous submission has finished; the new submission
replaces its prior status. `stats().total` counts currently visible accepted
and retained records, not all tasks ever submitted.

`wait_for_idle()` and `wait_for_current_tasks()` wait for registry
transitions. A result may still be publishing to its handle when they return;
use `TaskHandle::get()` or await the handle when the result is required.
