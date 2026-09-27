// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Mutex;

use tokio::task::spawn_blocking;

use super::TaskContext;
use super::TaskHandler;
use super::TaskHandlerDescriptor;
use super::TaskRunResult;
use crate::model::TaskRunError;
use crate::store::TaskFuture;

/// One-shot local task closure with its per-attempt context.
type LocalTaskClosure = Box<dyn FnOnce(TaskContext) -> TaskRunResult + Send>;

/// Adapter that turns a one-shot local closure into a non-recoverable handler.
///
/// # Examples
///
/// ```
/// use qubit_task::handler::LocalTaskHandler;
/// use qubit_task::handler::TaskHandler;
/// use qubit_task::handler::TaskHandlerDescriptor;
/// use qubit_task::handler::TaskRunOutcome;
/// use qubit_task::model::TaskOutput;
///
/// let handler = LocalTaskHandler::new(
///     TaskHandlerDescriptor { task_type: "once".into(), version: "1".into() },
///     |_| Ok(TaskRunOutcome::Succeeded(TaskOutput::default())),
/// );
/// assert_eq!(handler.descriptor().task_type, "once");
/// ```
pub struct LocalTaskHandler {
    /// Descriptor used to resolve this handler.
    descriptor: TaskHandlerDescriptor,
    /// Closure consumed by the first execution attempt.
    closure: Mutex<Option<LocalTaskClosure>>,
}

impl LocalTaskHandler {
    /// Creates a one-shot handler for a closure submitted directly to a
    /// volatile service.
    ///
    /// # Type Parameters
    ///
    /// * `F` - One-shot closure type used to execute the task.
    ///
    /// # Parameters
    ///
    /// * `descriptor` - Stable task type and version handled by the closure.
    /// * `closure` - One-shot operation run on the blocking pool.
    ///
    /// # Returns
    ///
    /// A handler that consumes the closure on its first attempt.
    #[must_use]
    pub fn new<F>(descriptor: TaskHandlerDescriptor, closure: F) -> Self
    where
        F: FnOnce(TaskContext) -> TaskRunResult + Send + 'static,
    {
        Self {
            descriptor,
            closure: Mutex::new(Some(Box::new(closure))),
        }
    }
}

impl TaskHandler for LocalTaskHandler {
    /// Returns the descriptor supplied when this adapter was created.
    ///
    /// # Returns
    ///
    /// The stable type and version key used by the service registry.
    fn descriptor(&self) -> TaskHandlerDescriptor {
        self.descriptor.clone()
    }

    /// Runs the closure once on the blocking pool.
    ///
    /// # Parameters
    ///
    /// * `_payload` - Ignored because local closures do not consume stored
    ///   payloads.
    /// * `context` - Per-attempt identity, resources, and cancellation signal.
    ///
    /// # Returns
    ///
    /// A future resolving to the closure result or an error if the closure was
    /// already consumed or its blocking task failed.
    ///
    /// # Errors
    ///
    /// Returns a non-retryable `local_handler` error if the closure was
    /// already consumed, or a retryable `engine` error if the blocking worker
    /// stopped before returning.
    ///
    /// # Panics
    ///
    /// Resumes a panic raised by the closure so the execution engine can
    /// classify the attempt as panicked.
    fn run<'a>(&'a self, _payload: &'a [u8], context: TaskContext) -> TaskFuture<'a, TaskRunResult> {
        Box::pin(async move {
            let closure = self
                .closure
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
            match closure {
                Some(closure) => match spawn_blocking(move || closure(context)).await {
                    Ok(result) => result,
                    Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
                    Err(error) => Err(TaskRunError {
                        category: "engine".into(),
                        message: error.to_string(),
                        retryable: true,
                    }),
                },
                None => Err(TaskRunError {
                    category: "local_handler".into(),
                    message: "one-shot local closure ran more than once".into(),
                    retryable: false,
                }),
            }
        })
    }
}
