// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use super::TaskContext;
use super::TaskHandlerDescriptor;
use super::TaskRunResult;
use crate::store::TaskFuture;

/// Async function contract used by handler implementations.
///
/// Handlers receive an opaque payload and per-attempt context. Long blocking
/// work must run on a blocking pool or dedicated backend so the async runtime
/// can continue scheduling other tasks.
///
/// # Thread safety and sharing
///
/// The service registers each handler once per exact [`TaskHandlerDescriptor`]
/// and keeps it as `Arc<dyn TaskHandler>`. Every accepted task with that
/// `(task_type, version)` resolves to the **same** `Arc` for the lifetime of
/// the service. When concurrency limits allow, several tasks of the same type
/// may be `Running` at once; the scheduler clones that shared `Arc` and may
/// call [`TaskHandler::run`] on different Tokio workers **in parallel**, each
/// with `&self` pointing at the same handler instance.
///
/// The trait therefore requires [`Send`] + [`Sync`]:
///
/// - **`Sync`**: shared references (`&self`) must be safe across threads
///   because concurrent attempts use one handler object.
/// - **`Send`**: handler state must be safe to move onto worker threads.
/// - **`run` futures must be `Send`**: [`TaskFuture`] is
///   `Pin<Box<dyn Future<Output = T> + Send + 'a>>`, so the returned future is
///   polled on the runtime's workers.
///
/// Treat each implementation as a **process-wide singleton** for its
/// descriptor, not as per-task state:
///
/// - Share dependencies through `Arc`, pools, and other already synchronized
///   handles (for example `Arc<dyn ImportRepository>`).
/// - Keep per-attempt data in the payload, [`TaskContext`], locals inside the
///   async block, or internal maps keyed by `task_id` with appropriate
///   locking.
/// - Do not store "the task currently running" in unsynchronized mutable
///   fields on the handler struct; that races when two tasks run together.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
///
/// use qubit_task::handler::TaskContext;
/// use qubit_task::handler::TaskHandler;
/// use qubit_task::handler::TaskHandlerDescriptor;
/// use qubit_task::handler::TaskHandlerRegistry;
/// use qubit_task::handler::TaskRunOutcome;
/// use qubit_task::handler::TaskRunResult;
/// use qubit_task::model::TaskOutput;
/// use qubit_task::store::TaskFuture;
///
/// struct Echo;
///
/// impl TaskHandler for Echo {
///     fn descriptor(&self) -> TaskHandlerDescriptor {
///         TaskHandlerDescriptor { task_type: "echo".into(), version: "1".into() }
///     }
///
///     fn run<'a>(&'a self, _payload: &'a [u8], _context: TaskContext) -> TaskFuture<'a, TaskRunResult> {
///         Box::pin(async { Ok(TaskRunOutcome::Succeeded(TaskOutput::default())) })
///     }
/// }
///
/// let mut registry = TaskHandlerRegistry::new();
/// registry.register(Arc::new(Echo)).unwrap();
/// assert!(registry.resolve("echo", "1").is_some());
/// ```
pub trait TaskHandler: Send + Sync {
    /// Identifies the exact task type and payload version this handler accepts.
    ///
    /// # Returns
    ///
    /// The stable task type and handler version.
    #[must_use]
    fn descriptor(&self) -> TaskHandlerDescriptor;

    /// Executes one attempt with a cooperative cancellation context.
    ///
    /// The same handler instance may serve this method concurrently for
    /// different tasks (see [thread safety and
    /// sharing](#thread-safety-and-sharing)). Each call receives its own
    /// `payload` and `context`; implementations must not assume exclusive
    /// access to `self`.
    ///
    /// A cancellation signal is only a request; implementations return
    /// `TaskRunOutcome::Cancelled` when they actually stop work. Successful
    /// work returns `TaskRunOutcome::Succeeded` even if a request arrived
    /// during execution.
    ///
    /// Implementations must not perform long CPU-bound or blocking operations
    /// directly on the async runtime worker. Use an appropriate blocking pool
    /// or a dedicated execution backend for that work.
    ///
    /// # Parameters
    ///
    /// * `payload` - Opaque bytes accepted with the task request.
    /// * `context` - Identity, attempt, resources, and cancellation state for
    ///   this execution attempt.
    ///
    /// # Returns
    ///
    /// A future resolving to the task outcome or a classified handler error.
    ///
    /// # Errors
    ///
    /// The future resolves to a [`crate::model::TaskRunError`] when the handler
    /// reports a classified failure.
    fn run<'a>(&'a self, payload: &'a [u8], context: TaskContext) -> TaskFuture<'a, TaskRunResult>;
}
