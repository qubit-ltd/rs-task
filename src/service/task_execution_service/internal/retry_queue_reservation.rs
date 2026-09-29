// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;

use super::ServiceCore;
use super::release_core_queue_slot;
use super::super::try_reserve_core_queue_slot;

/// Owns a retry queue slot until the corresponding queue entry is installed.
pub(super) struct RetryQueueReservation {
    /// State whose reserved queue count is released on early exit.
    core: Arc<ServiceCore>,
    /// Whether the reservation still owns the count.
    armed: bool,
}

impl RetryQueueReservation {
    /// Reserves one slot, returning `None` when the queue is full.
    pub(super) fn try_new(core: &Arc<ServiceCore>) -> Option<Self> {
        try_reserve_core_queue_slot(core).then(|| Self { core: Arc::clone(core), armed: true })
    }

    /// Transfers the count to an entry already installed in the queue.
    pub(super) fn commit_to_queue(mut self) {
        self.armed = false;
    }
}

impl Drop for RetryQueueReservation {
    /// Reclaims a slot after cancellation, panic, conflict exit, or failure.
    fn drop(&mut self) {
        if self.armed { release_core_queue_slot(&self.core); }
    }
}
