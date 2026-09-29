// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::atomic::Ordering;

use super::super::ServiceCore;

/// Decrements the tracked count when finalization exits or its future is dropped.
///
/// Constructed before spawning, after the scheduler increments the count, so
/// runtime cancellation before the first poll cannot leak the reservation.
pub(in crate::service::task_execution_service) struct AttemptInFlightGuard {
    /// Service whose active-attempt count this guard owns.
    pub(in crate::service::task_execution_service) core_ref: std::sync::Weak<ServiceCore>,
}

impl Drop for AttemptInFlightGuard {
    /// Wakes scheduler-failure shutdown when the last attempt finalizes.
    fn drop(&mut self) {
        if let Some(core) = self.core_ref.upgrade() {
            core.attempts_in_flight.fetch_sub(1, Ordering::AcqRel);
            core.attempts_changed.notify_waiters();
        }
    }
}
