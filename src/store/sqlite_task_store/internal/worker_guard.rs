// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
#[cfg(test)]
use std::sync::Arc;

#[cfg(test)]
use super::WorkerCounts;

/// Decrements an active-worker count when a blocking operation exits.
#[cfg(test)]
#[must_use = "the guard must live until the blocking operation exits"]
pub(in crate::store::sqlite_task_store) struct WorkerGuard(
    /// Shared counters updated while the guarded operation is active.
    pub(in crate::store::sqlite_task_store) Arc<WorkerCounts>,
);

#[cfg(test)]
impl WorkerGuard {
    /// Records one active worker and updates the observed concurrency peak.
    ///
    /// # Parameters
    ///
    /// * `counts` - Shared counters owned by the store under test.
    ///
    /// # Returns
    ///
    /// A guard that removes this worker from the active count when dropped.
    pub(in crate::store::sqlite_task_store) fn enter(counts: Arc<WorkerCounts>) -> Self {
        let active = counts.active.fetch_add(1, std::sync::atomic::Ordering::AcqRel) + 1;
        counts.peak.fetch_max(active, std::sync::atomic::Ordering::AcqRel);
        Self(counts)
    }
}

#[cfg(test)]
impl Drop for WorkerGuard {
    /// Removes the exiting worker from the active count.
    fn drop(&mut self) {
        let Self(counts) = self;
        counts.active.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
    }
}
