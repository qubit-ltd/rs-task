// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::TypedServiceCore;

/// Decrements active-operation counters and wakes shutdown waiters on drop.
pub(in crate::service::typed_task_execution_service) struct InFlightGuard(
    /// Shared counters and wakeups updated when this operation ends.
    Arc<TypedServiceCore>,
);

impl Drop for InFlightGuard {
    /// Decrements the active-operation count and wakes shutdown and scheduler
    /// waiters.
    fn drop(&mut self) {
        self.0.in_flight.fetch_sub(1, Ordering::AcqRel);
        self.0.in_flight_changed.notify_waiters();
        self.0.scheduler_changed.notify_one();
    }
}
