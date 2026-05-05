/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
use super::task_status::TaskStatus;

/// Count snapshot for a [`TaskExecutionService`](super::TaskExecutionService).
///
/// Counters are derived from the service registry and therefore include
/// terminal task records retained for inspection.
///
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct TaskExecutionStats {
    /// Number of tasks accepted by the service.
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
