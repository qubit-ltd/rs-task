// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
/// Current worker count and payload bytes held under the admission lock.
pub(in crate::service) struct BudgetUsage {
    /// Number of external write operations holding reservations.
    pub(in crate::service) operations: usize,
    /// Payload bytes retained by those workers.
    pub(in crate::service) payload_bytes: usize,
}
