// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Serialized lifecycle fields needed to construct a task summary.

use serde::Deserialize;
use serde::Serialize;

use crate::model::TaskOutput;
use crate::model::TaskState;
use crate::model::typed::TaskProgressSnapshot;

/// Persisted task lifecycle data excluding the separately stored payload.
#[derive(Deserialize, Serialize)]
pub(super) struct Lifecycle {
    /// Current persisted task state.
    pub(super) state: TaskState,
    /// Whether a cancellation request is pending.
    #[serde(default)]
    pub(super) cancel_requested: bool,
    /// Latest external cancellation diagnostic, if any.
    #[serde(default)]
    pub(super) cancel_error: Option<String>,
    /// Monotonic revision used to reject stale state changes.
    pub(super) state_version: u64,
    /// Number of handler attempts made for this task.
    pub(super) attempt: u32,
    /// Retry eligibility time for queued work, if delayed.
    #[serde(default)]
    pub(super) retry_not_before_ms: Option<u64>,
    /// Time when the task was accepted.
    pub(super) accepted_at_ms: u64,
    /// Time when the current attempt started, if it has started.
    pub(super) started_at_ms: Option<u64>,
    /// Time when the task reached a terminal state, if finished.
    pub(super) finished_at_ms: Option<u64>,
    /// Latest progress snapshot for the current attempt.
    pub(super) progress: Option<TaskProgressSnapshot>,
    /// Terminal output summary, if the task produced one.
    #[serde(default)]
    pub(super) output: Option<TaskOutput>,
}
