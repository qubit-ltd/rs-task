// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Versioned task handler contracts and registry.

type LocalTaskClosure = Box<dyn FnOnce(TaskContext) -> TaskRunResult + Send>;

mod context;
mod registry;

pub use context::TaskContext;
pub use registry::RegistryError;
pub use registry::TaskHandlerDescriptor;
pub use registry::TaskHandlerRegistry;
pub use registry::TaskRunOutcome;
pub use registry::TaskRunResult;

/// Adapter that turns a one-shot local closure into a non-recoverable handler.
pub struct LocalTaskHandler {
    descriptor: TaskHandlerDescriptor,
    closure: std::sync::Mutex<Option<LocalTaskClosure>>,
}

impl LocalTaskHandler {
    /// Creates a one-shot handler for a closure submitted directly to a
    /// volatile service.
    #[must_use]
    pub fn new<F>(descriptor: TaskHandlerDescriptor, closure: F) -> Self
    where
        F: FnOnce(TaskContext) -> TaskRunResult + Send + 'static,
    {
        Self {
            descriptor,
            closure: std::sync::Mutex::new(Some(Box::new(closure))),
        }
    }
}

impl TaskHandler for LocalTaskHandler {
    fn descriptor(&self) -> TaskHandlerDescriptor {
        self.descriptor.clone()
    }

    fn run<'a>(&'a self, _payload: &'a [u8], context: TaskContext) -> TaskFuture<'a, TaskRunResult> {
        Box::pin(async move {
            let closure = self
                .closure
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
            match closure {
                Some(closure) => tokio::task::spawn_blocking(move || closure(context))
                    .await
                    .unwrap_or_else(|error| {
                        Err(crate::model::TaskRunError {
                            category: "panic".into(),
                            message: error.to_string(),
                            retryable: false,
                        })
                    }),
                None => Err(crate::model::TaskRunError {
                    category: "local_handler".into(),
                    message: "one-shot local closure ran more than once".into(),
                    retryable: false,
                }),
            }
        })
    }
}

use crate::store::TaskFuture;

/// Task handler factory contract shared by SPI providers and direct
/// registration.
pub trait TaskHandlerProvider: Send + Sync {
    /// Returns the stable handler type and version supplied by this provider.
    fn descriptor(&self) -> TaskHandlerDescriptor;
    /// Builds the handler instance during application assembly.
    fn create(&self) -> Result<std::sync::Arc<dyn TaskHandler>, String>;
}

/// Async function contract used by handler implementations.
///
/// Handlers receive an opaque payload and per-attempt context. Long blocking
/// work must run on a blocking pool or dedicated backend so the async runtime
/// can continue scheduling other tasks.
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
    fn descriptor(&self) -> TaskHandlerDescriptor;
    /// Executes one attempt with a cooperative cancellation context.
    ///
    /// A cancellation signal is only a request; implementations return
    /// `TaskRunOutcome::Cancelled` when they actually stop work.
    /// Successful work returns `TaskRunOutcome::Succeeded` even if a request
    /// arrived during execution.
    ///
    /// Implementations must not perform long CPU-bound or blocking operations
    /// directly on the async runtime worker. Use an appropriate blocking pool
    /// or a dedicated execution backend for that work.
    fn run<'a>(&'a self, payload: &'a [u8], context: TaskContext) -> TaskFuture<'a, TaskRunResult>;
}
