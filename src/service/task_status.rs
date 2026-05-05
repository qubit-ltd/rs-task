/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/

/// Status of a task managed by [`TaskExecutionService`](super::TaskExecutionService).
///
/// The status describes service-level progress, not the typed task result
/// stored in [`TaskHandle`](qubit_executor::TaskHandle). The handle remains the source of truth for the
/// task's success value or error value.
///
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStatus {
    /// The task was accepted but has not started running.
    Submitted,

    /// A worker has started running the task.
    Running,

    /// The task completed successfully.
    Succeeded,

    /// The task ran and returned its own error value.
    Failed,

    /// The task panicked while running.
    Panicked,

    /// The task was cancelled before it started.
    Cancelled,
}

impl TaskStatus {
    /// Returns whether this status represents in-flight work.
    ///
    /// # Returns
    ///
    /// `true` for submitted or running tasks.
    #[inline]
    pub const fn is_active(self) -> bool {
        matches!(self, Self::Submitted | Self::Running)
    }
}
