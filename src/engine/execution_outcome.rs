// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use crate::handler::TaskRunResult;

/// Outcome reported by an execution backend for one started attempt.
///
/// # Examples
///
/// ```
/// use qubit_task::engine::ExecutionOutcome;
/// use qubit_task::handler::TaskRunOutcome;
/// use qubit_task::model::TaskOutput;
///
/// let outcome = ExecutionOutcome::Returned(Ok(TaskRunOutcome::Succeeded(TaskOutput::default())));
/// assert!(matches!(outcome, ExecutionOutcome::Returned(Ok(_))));
/// ```
#[derive(Debug)]
#[must_use]
pub enum ExecutionOutcome {
    /// The handler returned a result, including a classified application error.
    Returned(TaskRunResult),
    /// The handler panicked while constructing or polling its future.
    Panicked(String),
    /// The execution worker stopped before it could report a handler result.
    WorkerStopped(String),
}
