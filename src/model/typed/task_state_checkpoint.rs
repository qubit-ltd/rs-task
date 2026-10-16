// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use super::TaskId;
use crate::model::TaskState;

/// Authoritative current lifecycle state retained for a task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskStateCheckpoint {
    /// Stable task identity.
    pub id: TaskId,
    /// Revision of the task lifecycle state.
    pub state_version: u64,
    /// Current lifecycle state.
    pub state: TaskState,
}
