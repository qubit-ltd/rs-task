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

#[cfg(test)]
pub(in crate::store::sqlite) struct WorkerGuard(pub(in crate::store::sqlite) Arc<WorkerCounts>);

#[cfg(test)]
impl WorkerGuard {
    pub(in crate::store::sqlite) fn enter(counts: Arc<WorkerCounts>) -> Self {
        let active = counts.active.fetch_add(1, std::sync::atomic::Ordering::AcqRel) + 1;
        counts.peak.fetch_max(active, std::sync::atomic::Ordering::AcqRel);
        Self(counts)
    }
}

#[cfg(test)]
impl Drop for WorkerGuard {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
    }
}
