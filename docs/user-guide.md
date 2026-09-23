# qubit-task User Guide

[中文版本](user-guide.zh_CN.md)

This guide targets Rust applications that submit work to a thread pool and need a caller-owned task ID, a typed result, and an inspectable lifecycle status. It describes qubit-task 0.5.x and requires Rust 1.94 or later.

## Purpose and Audience

Use this crate when an application needs to answer both questions for one submission: what did the task return, and where is the task in its lifecycle? The service registry is in memory and bounded; it is not a durable job database.

## Conceptual Model

Each accepted submission has two related views:

| View | API | Meaning |
| --- | --- | --- |
| Typed outcome | TaskHandle<R, E> | The task's R value or E/executor error. |
| Service status | TaskStatus | Submitted, Running, or a terminal status. |

Id is supplied by the caller. An ID cannot be submitted again while its previous task is active or being accepted. After terminal completion it may be reused. status and stats expose active records plus terminal records kept by the configured history capacity.

## Scenario

Suppose an application accepts an import request numbered 42. It needs to submit the import, return its typed count to the caller, and later show Succeeded or a failure status in an operator view. The minimal flow is:

1. Build one service and choose how many terminal statuses to retain.
2. Submit the callable with Id::new(42).
3. Read the result from TaskHandle::get and inspect status.
4. Shut down the service after all accepted work is complete.

## Installation and Minimal Configuration

Add the crate to Cargo.toml:

~~~toml
[dependencies]
qubit-task = "0.5"
qubit-id = "0.6"
~~~

The default constructor uses the default qubit-thread-pool settings. Use the builder when the application needs a custom pool or bounded history:

~~~rust
use qubit_task::service::TaskExecutionService;

let service = TaskExecutionService::builder()
    .completed_history_capacity(128)
    .build()?;
# Ok::<(), Box<dyn std::error::Error>>(())
~~~

## Core Workflow

Submit a callable and consume its handle when the typed result is required:

~~~rust
use qubit_id::Id;
use qubit_task::service::{TaskExecutionService, TaskStatus};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let service = TaskExecutionService::new()?;
    let id = Id::new(42);
    let handle = service.submit_callable(id, || Ok::<u32, std::io::Error>(21))?;

    assert_eq!(handle.get()?, 21);
    assert_eq!(service.status(id), Some(TaskStatus::Succeeded));

    service.shutdown();
    service.wait_termination();
    Ok(())
}
~~~

submit is the convenience form for a runnable that returns Result<(), E>. TaskHandle::try_get checks without blocking, is_done reports whether the handle is terminal, and TaskHandle can also be awaited through its IntoFuture implementation.

## Advanced Usage

### Configure the backing pool

Pass a ThreadPoolBuilder to tune pool properties supported by qubit-thread-pool:

Add the pool crate as a direct dependency when using this option:

~~~toml
[dependencies]
qubit-thread-pool = "0.10"
~~~

~~~rust
use qubit_task::service::TaskExecutionService;
use qubit_thread_pool::ThreadPoolBuilder;

let service = TaskExecutionService::builder()
    .thread_pool(ThreadPoolBuilder::default().pool_size(4).queue_capacity(256))
    .build()?;
# Ok::<(), Box<dyn std::error::Error>>(())
~~~

### Pause intake and cancel queued work

suspend rejects new submissions with TaskExecutionServiceError::Suspended, while accepted work continues. Call resume to accept new submissions again. cancel(id) returns true only when the task is cancelled before a worker starts it; cancellation races with the thread pool, so a running task returns false.

### Wait for work

wait_for_current_tasks waits for the active-ID snapshot seen at entry. wait_for_idle waits until the registry has no reservation or accepted active task. Neither method replaces TaskHandle::get when the result publication itself must be observed.

## Errors and Diagnostics

There are two error layers:

- Submission errors are returned by submit and submit_callable. They may be DuplicateTask, Suspended, Rejected, or AcceptancePanicked.
- The accepted task's result is returned by its handle. A callable's own Err(E) is a failed task; a panic is reported as a panicked task by the executor result type.

Use status(id) for one task and stats() for a snapshot. TaskExecutionStats counts visible active tasks and retained terminal records, not lifetime submissions. A terminal status can disappear after eviction, in which case status(id) returns None.

## Troubleshooting

| Symptom | Check |
| --- | --- |
| Submission returns DuplicateTask | Wait for the existing task to reach a terminal state, or choose another Id. |
| Submission returns Suspended | Call is_suspended() and resume() when intake may resume. |
| cancel returns false | The ID may be unknown, terminal, or already running. Check status(id). |
| status(id) returns None after success | The record was never accepted or was evicted from bounded history. |
| Shutdown does not mean the handle is ready | Call TaskHandle::get for the result, then wait_termination for worker termination. |

## Limitations and Best Practices

- Keep the service alive while submitting and observing tasks; it owns the registry and backing thread pool.
- Treat submit success as acceptance, not as task success. Always inspect or consume the returned handle when the result matters.
- Choose completed_history_capacity according to the amount of status data that must remain queryable. This history is memory-only and bounded.
- Do not use cancel as a way to stop a callable that has already started.
- Call shutdown for graceful pool shutdown and wait_termination when the application must wait for worker termination. Use stop only when the backing pool's immediate-stop behavior is intended.

## Further Reading

- [Project README](../README.md)
- [中文用户指南](user-guide.zh_CN.md)
- [Design notes](design.md)
- [API documentation](https://docs.rs/qubit-task)
