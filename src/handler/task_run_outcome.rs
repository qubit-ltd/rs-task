// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use crate::model::TaskOutput;
use crate::model::TaskRunError;

/// Explicit outcome of one handler execution attempt.
///
/// Cancellation is acknowledged only after the handler has stopped its work.
///
/// # Examples
///
/// ```
/// use qubit_task::handler::TaskRunOutcome;
/// use qubit_task::model::TaskOutput;
///
/// let outcome = TaskRunOutcome::Succeeded(TaskOutput::default());
/// assert!(matches!(outcome, TaskRunOutcome::Succeeded(_)));
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub enum TaskRunOutcome {
    /// Execution completed successfully with a small persisted summary.
    Succeeded(
        /// Bounded summary persisted with the successful task record.
        TaskOutput,
    ),
    /// The handler acknowledged cancellation and stopped work.
    Cancelled,
}

/// Handler result containing an explicit outcome or classified failure.
///
/// The service persists a successful outcome or the error's category and
/// diagnostic, subject to the documented size limits.
///
/// # Examples
///
/// ```
/// use qubit_task::handler::TaskRunOutcome;
/// use qubit_task::handler::TaskRunResult;
///
/// let result: TaskRunResult = Ok(TaskRunOutcome::Cancelled);
/// assert!(result.is_ok());
/// ```
pub type TaskRunResult = Result<TaskRunOutcome, TaskRunError>;
