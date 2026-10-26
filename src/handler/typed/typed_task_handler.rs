// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use crate::handler::TaskRunResult;
use crate::handler::typed::TypedTaskContext;
use crate::store::TaskFuture;

/// Asynchronous business logic for one concrete task payload type.
///
/// The execution service calls the handler after decoding a task payload and
/// supplies a context for that attempt. The context carries the task identity,
/// cancellation state, and progress reporter owned by the service.
/// Implementations must be thread-safe and have no non-static borrows so the
/// registry can retain them for task execution.
///
/// # Type Parameters
///
/// * `T` - Decoded payload type accepted by this handler.
///
/// # Examples
///
/// ```
/// use qubit_task::handler::TaskContext;
/// use qubit_task::handler::TaskHandler;
/// use qubit_task::handler::TaskRunOutcome;
/// use qubit_task::handler::TaskRunResult;
/// use qubit_task::model::TaskOutput;
/// use qubit_task::store::TaskFuture;
///
/// struct EchoHandler;
///
/// impl TaskHandler<String> for EchoHandler {
///     fn run<'a>(
///         &'a self,
///         input: String,
///         _context: TaskContext,
///     ) -> TaskFuture<'a, TaskRunResult> {
///         Box::pin(async move {
///             let _message = input;
///             Ok(TaskRunOutcome::Succeeded(TaskOutput::default()))
///         })
///     }
/// }
///
/// async fn execute_handler(
///     handler: &EchoHandler,
///     input: String,
///     context: TaskContext,
/// ) -> TaskRunResult {
///     // The task execution service supplies this context for each attempt.
///     handler.run(input, context).await
/// }
/// ```
pub trait TypedTaskHandler<T>: Send + Sync + 'static {
    /// Executes one attempt with an already decoded input value.
    ///
    /// The execution service provides `context` after decoding a stored task
    /// payload and passes the returned future to its asynchronous runtime.
    ///
    /// # Parameters
    ///
    /// * `input` - Payload value decoded for this task attempt.
    /// * `context` - Attempt identity and cooperative cancellation/progress
    ///   facilities supplied by the execution service.
    ///
    /// # Returns
    ///
    /// A sendable future that completes with the attempt outcome or a
    /// classified task error.
    ///
    /// # Errors
    ///
    /// The future returns the handler's classified error if the attempt fails.
    fn run<'a>(&'a self, input: T, context: TypedTaskContext) -> TaskFuture<'a, TaskRunResult>;
}
