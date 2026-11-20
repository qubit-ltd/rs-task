// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use super::TaskId;

/// Identifies one execution attempt of a task.
///
/// Attempt numbers are one-based and pair with a task ID to distinguish
/// retries of the same submitted task.
///
/// # Examples
///
/// ```
/// use qubit_id::Id;
/// use qubit_task::TaskAttempt;
/// use qubit_task::TaskId;
///
/// let attempt = TaskAttempt::new(TaskId::from_id(Id::new(42)), 2);
/// assert_eq!(attempt.attempt(), 2);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TaskAttempt {
    task_id: TaskId,
    attempt: u32,
}

impl TaskAttempt {
    /// Creates an attempt identity from its task ID and one-based attempt
    /// number.
    ///
    /// # Parameters
    ///
    /// * `task_id` identifies the submitted task.
    /// * `attempt` is the one-based execution attempt number.
    ///
    /// # Returns
    ///
    /// An identity for that task attempt.
    #[must_use]
    pub const fn new(task_id: TaskId, attempt: u32) -> Self {
        Self { task_id, attempt }
    }

    /// Returns the task ID.
    ///
    /// # Returns
    ///
    /// The ID of the submitted task.
    #[must_use]
    pub const fn task_id(self) -> TaskId {
        self.task_id
    }

    /// Returns the one-based attempt number.
    ///
    /// # Returns
    ///
    /// The execution attempt number stored in this identity.
    #[must_use]
    pub const fn attempt(self) -> u32 {
        self.attempt
    }
}
