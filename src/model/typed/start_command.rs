// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use super::TaskId;

/// Compare-and-set command that starts a queued typed task attempt.
///
/// The task store accepts this command only when the task is still queued and
/// its lifecycle version matches `expected_state_version`.
///
/// # Examples
///
/// ```
/// use qubit_task::model::StartCommand;
/// use qubit_task::model::TaskId;
///
/// let command = StartCommand {
///     id: TaskId::from_id(qubit_id::Id::new(42)),
///     expected_state_version: 3,
///     started_at_ms: 1_700_000_000_000,
/// };
///
/// assert_eq!(command.id.to_padded_decimal(), "00000000000000000042");
/// assert_eq!(command.expected_state_version, 3);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StartCommand {
    /// Identifies the queued task whose attempt should start.
    pub id: TaskId,
    /// Lifecycle version the caller expects, used to reject a start if the task
    /// has changed since it was observed.
    pub expected_state_version: u64,
    /// Unix epoch time in milliseconds to record as the attempt start time.
    pub started_at_ms: u64,
}
