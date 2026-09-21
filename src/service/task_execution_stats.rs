// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use super::task_status::TaskStatus;

/// Count snapshot for a [`TaskExecutionService`](super::TaskExecutionService).
///
/// Counters are derived from accepted active tasks and bounded terminal
/// records retained for inspection. They are not lifetime totals.
///
/// # Examples
///
/// ```
/// use qubit_task::service::TaskExecutionStats;
///
/// let stats = TaskExecutionStats::default();
/// assert_eq!(stats.total, 0);
/// ```
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
#[must_use = "task statistics should be inspected or explicitly discarded"]
pub struct TaskExecutionStats {
    /// Number of accepted active and retained terminal tasks currently visible.
    pub total: usize,

    /// Number of accepted tasks not yet started.
    pub submitted: usize,

    /// Number of tasks currently running.
    pub running: usize,

    /// Number of tasks that completed successfully.
    pub succeeded: usize,

    /// Number of tasks that returned an error.
    pub failed: usize,

    /// Number of tasks that panicked.
    pub panicked: usize,

    /// Number of tasks cancelled before start.
    pub cancelled: usize,
}

impl TaskExecutionStats {
    /// Adds one task status to this snapshot.
    pub(crate) fn add_status(&mut self, status: TaskStatus) {
        self.total += 1;
        match status {
            TaskStatus::Submitted => self.submitted += 1,
            TaskStatus::Running => self.running += 1,
            TaskStatus::Succeeded => self.succeeded += 1,
            TaskStatus::Failed => self.failed += 1,
            TaskStatus::Panicked => self.panicked += 1,
            TaskStatus::Cancelled => self.cancelled += 1,
        }
    }
}
