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

type RunPrepared = Box<dyn FnOnce(TypedTaskContext) -> TaskFuture<'static, TaskRunResult> + Send>;

/// A payload that passed routing, schema, codec, and Rust value-type checks.
pub struct PreparedTask {
    run: RunPrepared,
}

impl PreparedTask {
    /// Creates a prepared task from the registry's erased handler adapter.
    pub(crate) fn new(run: RunPrepared) -> Self {
        Self { run }
    }

    /// Starts the already validated attempt using its task context.
    #[must_use]
    pub fn run(self, context: TypedTaskContext) -> TaskFuture<'static, TaskRunResult> {
        (self.run)(context)
    }
}

impl std::fmt::Debug for PreparedTask {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PreparedTask")
            .finish_non_exhaustive()
    }
}
