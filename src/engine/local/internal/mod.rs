// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
// Tracks resource usage and releases reservations when execution ends.
mod reservation_guard;
mod resource_ledger;
mod usage;

pub(in crate::engine::local) use reservation_guard::ReservationGuard;
pub(in crate::engine::local) use resource_ledger::ResourceLedger;
pub(in crate::engine::local) use resource_ledger::release_reservation;
