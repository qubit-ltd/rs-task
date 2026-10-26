// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use super::TaskId;
use crate::model::TaskOutput;
use crate::model::TaskState;

/// Compare-and-set transition for one typed task attempt.
///
/// Stores apply this command only when `expected_state_version` and
/// `expected_attempt` still match the persisted task. Execution services build
/// commands from the attempt they are completing; callers implementing a store
/// can construct the same value from their persisted state.
///
/// # Examples
///
/// ```
/// use qubit_task::model::typed::TaskId;
/// use qubit_task::model::TaskOutput;
/// use qubit_task::model::TaskState;
/// use qubit_task::model::TransitionCommand;
///
/// let command = TransitionCommand {
///     id: TaskId::from_id(qubit_id::Id::new(42)),
///     expected_state_version: 3,
///     expected_attempt: 1,
///     retry_not_before_ms: None,
///     state: TaskState::Succeeded,
///     cancel_requested: false,
///     cancel_error: None,
///     finished_at_ms: Some(1_700_000_000_000),
///     output: Some(TaskOutput {
///         summary: b"stored result key".to_vec(),
///     }),
/// };
/// assert_eq!(command.expected_attempt, 1);
/// assert!(matches!(command.state, TaskState::Succeeded));
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransitionCommand {
    /// Task being transitioned.
    pub id: TaskId,
    /// Expected lifecycle revision.
    pub expected_state_version: u64,
    /// Expected running attempt, or zero for pre-start cancellation.
    pub expected_attempt: u32,
    /// Earliest execution time when transitioning back to `Queued`.
    pub retry_not_before_ms: Option<u64>,
    /// Desired lifecycle state.
    pub state: TaskState,
    /// Whether an unacknowledged cancellation request remains outstanding.
    pub cancel_requested: bool,
    /// Diagnostic from failed external cancellation, if any.
    pub cancel_error: Option<String>,
    /// Unix epoch milliseconds when terminal state was reached.
    pub finished_at_ms: Option<u64>,
    /// Bounded result summary; accepted only with the succeeded state.
    pub output: Option<TaskOutput>,
}
