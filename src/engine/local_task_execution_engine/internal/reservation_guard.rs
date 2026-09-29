// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
/// Releases a prepared reservation when its execution worker exits or unwinds.
#[must_use]
pub(in crate::engine::local_task_execution_engine) struct ReservationGuard(
    /// Callback that releases the attempt's reserved resources.
    pub(in crate::engine::local_task_execution_engine) Option<Box<dyn FnOnce() + Send>>,
);

impl Drop for ReservationGuard {
    /// Releases the held resource reservation exactly once.
    fn drop(&mut self) {
        if let Some(release) = self.0.take() {
            release();
        }
    }
}
