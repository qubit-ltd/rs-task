// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
// =============================================================================
use super::TaskId;

/// Identifies one execution attempt of a task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TaskAttempt {
    task_id: TaskId,
    attempt: u32,
}

impl TaskAttempt {
    /// Creates an attempt identity from its task ID and one-based attempt
    /// number.
    #[must_use]
    pub const fn new(task_id: TaskId, attempt: u32) -> Self {
        Self { task_id, attempt }
    }

    /// Returns the task ID.
    #[must_use]
    pub const fn task_id(self) -> TaskId {
        self.task_id
    }

    /// Returns the one-based attempt number.
    #[must_use]
    pub const fn attempt(self) -> u32 {
        self.attempt
    }
}
