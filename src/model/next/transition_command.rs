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
