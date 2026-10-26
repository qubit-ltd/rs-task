// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use serde::Deserialize;
use serde::Serialize;

use crate::model::TaskOutput;
use crate::model::TaskState;
use crate::model::typed::TaskProgressSnapshot;

/// Mutable lifecycle values kept independent from immutable request data.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(super) struct StoredTypedLifecycle {
    /// Current externally visible task state.
    pub(super) state: TaskState,
    /// Whether cancellation has been requested for the task.
    #[serde(default)]
    pub(super) cancel_requested: bool,
    /// Optional diagnostic explaining a cancellation outcome.
    #[serde(default)]
    pub(super) cancel_error: Option<String>,
    /// Monotonic revision used by compare-and-set lifecycle updates.
    pub(super) state_version: u64,
    /// Number of execution attempts started for this task.
    pub(super) attempt: u32,
    /// Earliest time at which a queued task may be retried.
    #[serde(default)]
    pub(super) retry_not_before_ms: Option<u64>,
    /// Acceptance timestamp in Unix milliseconds.
    pub(super) accepted_at_ms: u64,
    /// Timestamp of the most recently started attempt, if any.
    pub(super) started_at_ms: Option<u64>,
    /// Timestamp at which the task entered a terminal state, if any.
    pub(super) finished_at_ms: Option<u64>,
    /// Most recently persisted progress snapshot, if any.
    pub(super) progress: Option<TaskProgressSnapshot>,
    /// Result produced by a successful task, if one has been stored.
    #[serde(default)]
    pub(super) output: Option<TaskOutput>,
}
