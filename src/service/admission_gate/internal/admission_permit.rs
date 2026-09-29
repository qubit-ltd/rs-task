// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use super::super::AdmissionGate;

/// Keeps one accepted operation in the gate until all its side effects finish.
#[must_use]
pub(in crate::service) struct AdmissionPermit<'a> {
    /// Gate whose active-operation count this permit holds.
    pub(in crate::service::admission_gate) gate: &'a AdmissionGate,
}

impl Drop for AdmissionPermit<'_> {
    /// Decrements the active count and wakes tasks waiting for idle state.
    fn drop(&mut self) {
        let mut state = self.gate.state.lock();
        state.active -= 1;
        self.gate.changed.notify_waiters();
    }
}
