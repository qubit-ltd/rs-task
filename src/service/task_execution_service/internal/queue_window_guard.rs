// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;

use super::super::QueuedTask;
use super::super::ServiceCore;

/// Restores unprocessed tasks when a scheduler round exits early.
pub(in crate::service::task_execution_service) struct QueueWindowGuard {
    /// Shared queue to which unfinished tasks are restored.
    core: Arc<ServiceCore>,
    /// Scheduler candidates not yet started or otherwise consumed.
    tasks: Option<Vec<QueuedTask>>,
}

impl QueueWindowGuard {
    /// Owns one bounded scheduler window until it is restored.
    ///
    /// # Parameters
    ///
    /// * `core` - Service state owning the shared queue.
    /// * `tasks` - Candidate window removed from that queue.
    ///
    /// # Returns
    ///
    /// A guard that restores the window unless explicitly consumed.
    pub(super) fn new(core: Arc<ServiceCore>, tasks: Vec<QueuedTask>) -> Self {
        Self {
            core,
            tasks: Some(tasks),
        }
    }

    /// Borrows the current window for policy ordering and execution.
    ///
    /// # Returns
    ///
    /// Mutable access to candidates held by this guard.
    ///
    /// # Panics
    ///
    /// Panics if the window has already been restored or consumed.
    pub(super) fn tasks_mut(&mut self) -> &mut Vec<QueuedTask> {
        self.tasks.as_mut().expect("scheduler window is active")
    }

    /// Returns all unprocessed work to the front of the shared queue.
    pub(super) fn restore(&mut self) {
        if let Some(tasks) = self.tasks.take() {
            self.core.queue.lock().restore_front(tasks);
        }
    }

    /// Returns unstarted work to the back so the scheduler can inspect later
    /// windows.
    pub(super) fn restore_back(&mut self) {
        if let Some(tasks) = self.tasks.take() {
            self.core.queue.lock().restore_back(tasks);
        }
    }
}

impl Drop for QueueWindowGuard {
    /// Restores the window if scheduler control exits early.
    fn drop(&mut self) {
        self.restore();
    }
}
