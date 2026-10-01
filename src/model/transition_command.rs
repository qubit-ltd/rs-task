// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use super::TaskOutput;
use super::TaskState;
use super::legacy::TaskId;

/// Conditional task state update guarded by state version and attempt.
///
/// Stores reject stale commands with a conflict, preventing a delayed worker
/// from overwriting a newer task lifecycle revision.
///
/// # Examples
///
/// ```
/// use qubit_task::model::TaskId;
/// use qubit_task::model::TaskState;
/// use qubit_task::model::TransitionCommand;
///
/// let command = TransitionCommand {
///     id: TaskId::generate(),
///     expected_version: 0,
///     expected_attempt: 0,
///     state: TaskState::Running,
///     retry_not_before_ms: None,
///     output: None,
///     assigned_resources: Vec::new(),
///     cancel_requested: false,
/// };
/// assert!(!command.cancel_requested);
/// ```
#[derive(Debug, Clone)]
#[cfg_attr(test, allow(dead_code))]
pub struct TransitionCommand {
    /// Target task identity.
    pub id: TaskId,
    /// Expected previous state version.
    pub expected_version: u64,
    /// Expected execution generation.
    pub expected_attempt: u32,
    /// New observable state.
    pub state: TaskState,
    /// Earliest start time for a queued retry, if delayed.
    pub retry_not_before_ms: Option<u64>,
    /// Optional result summary.
    pub output: Option<TaskOutput>,
    /// Assigned device identifiers.
    pub assigned_resources: Vec<String>,
    /// Whether this transition requests cancellation.
    pub cancel_requested: bool,
}
