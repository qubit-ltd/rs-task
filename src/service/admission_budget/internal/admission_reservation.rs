// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;

use super::super::AdmissionBudget;

/// Releases one payload and submission reservation when its admission worker
/// ends.
#[must_use]
pub(in crate::service) struct AdmissionReservation {
    /// Budget whose usage is decremented when this reservation is dropped.
    pub(in crate::service) budget: Arc<AdmissionBudget>,
    /// Payload bytes charged to this reservation.
    pub(in crate::service) payload_bytes: usize,
}

impl Drop for AdmissionReservation {
    /// Returns this reservation's worker and payload accounting to the budget.
    fn drop(&mut self) {
        let mut usage = self.budget.usage.lock();
        debug_assert!(usage.operations > 0);
        debug_assert!(usage.payload_bytes >= self.payload_bytes);
        usage.operations -= 1;
        usage.payload_bytes -= self.payload_bytes;
    }
}
