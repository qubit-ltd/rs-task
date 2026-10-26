// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::fmt::Debug;
use std::fmt::Formatter;

use crate::handler::TaskRunResult;
use crate::handler::typed::TypedTaskContext;
use crate::store::TaskFuture;

/// Type-erased, single-use adapter for an already validated typed task handler.
type RunPrepared = Box<dyn FnOnce(TypedTaskContext) -> TaskFuture<'static, TaskRunResult> + Send>;

/// A single-use handler payload that has passed routing, schema, codec, and
/// Rust value-type checks.
///
/// Calling [`run`](Self::run) consumes the payload and starts its handler with
/// the supplied task context. This keeps validation outside the execution
/// attempt while ensuring the handler can be invoked at most once.
#[must_use = "a prepared task must be run to start its handler"]
pub struct PreparedTask {
    /// Type-erased handler adapter invoked when this task attempt starts.
    run: RunPrepared,
}

impl PreparedTask {
    /// Creates a prepared task from the registry's erased handler adapter.
    ///
    /// # Parameters
    ///
    /// * `run` is the adapter that invokes the validated handler once.
    ///
    /// # Returns
    ///
    /// A task payload that can start the prepared handler attempt.
    pub(crate) fn new(run: RunPrepared) -> Self {
        Self { run }
    }

    /// Starts the already validated attempt using its task context.
    ///
    /// # Parameters
    ///
    /// * `context` is supplied by the task execution flow for this attempt.
    ///
    /// # Returns
    ///
    /// A future that completes when the prepared handler attempt finishes.
    pub fn run(self, context: TypedTaskContext) -> TaskFuture<'static, TaskRunResult> {
        (self.run)(context)
    }
}

impl Debug for PreparedTask {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("PreparedTask").finish_non_exhaustive()
    }
}
