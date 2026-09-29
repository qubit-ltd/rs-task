// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
/// Reason an in-flight admission could not reserve bounded resources.
///
/// The caller can reject promptly and report the relevant configured limit.
#[must_use]
pub(in crate::service) enum AdmissionBudgetError {
    /// The request payload would exceed the aggregate retained-byte budget.
    PayloadBytesExceeded {
        /// Payload bytes requested by the new reservation.
        requested: usize,
        /// Payload bytes remaining under the configured budget.
        available: usize,
    },
    /// The number of concurrent admissions has reached its configured limit.
    OperationLimitExceeded {
        /// Maximum number of concurrent external write operations.
        limit: usize,
    },
}
