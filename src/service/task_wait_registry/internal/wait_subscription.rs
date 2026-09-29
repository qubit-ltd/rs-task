// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;

use tokio::sync::Notify;
use tokio::sync::futures::Notified;

use super::super::TaskWaitRegistry;
use crate::model::TaskId;

/// Keeps one task notification registered until the subscription is dropped.
#[must_use]
pub(in crate::service) struct WaitSubscription {
    /// Registry whose subscriber count this value owns.
    registry: Arc<TaskWaitRegistry>,
    /// Task whose notifications this value observes.
    id: TaskId,
    /// Shared notification primitive for this task.
    notify: Arc<Notify>,
}

impl WaitSubscription {
    /// Creates a subscription after the registry has incremented its count.
    ///
    /// # Parameters
    ///
    /// * `registry` - Registry tracking this subscription's lifetime.
    /// * `id` - Task whose notifications this value observes.
    /// * `notify` - Shared notification primitive for the task.
    ///
    /// # Returns
    ///
    /// A subscription that unregisters when dropped.
    pub(in crate::service::task_wait_registry) fn new(
        registry: Arc<TaskWaitRegistry>,
        id: TaskId,
        notify: Arc<Notify>,
    ) -> Self {
        Self { registry, id, notify }
    }

    /// Creates the notification future used to await a task update.
    ///
    /// # Returns
    ///
    /// A future that completes after the next notification for this task.
    #[inline]
    pub(in crate::service) fn notified(&self) -> Notified<'_> {
        self.notify.notified()
    }
}

impl Drop for WaitSubscription {
    /// Removes the registry entry after its final subscriber is gone.
    fn drop(&mut self) {
        let mut entries = self.registry.entries.lock();
        if let Some(entry) = entries.get_mut(&self.id) {
            entry.subscribers -= 1;
            if entry.subscribers == 0 {
                entries.remove(&self.id);
            }
        }
    }
}
