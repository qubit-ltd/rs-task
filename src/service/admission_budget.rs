// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
// Owns the private reservation and accounting types used by the budget.
mod internal;

use std::num::NonZeroUsize;
use std::sync::Arc;

pub(super) use internal::AdmissionBudgetError;
pub(super) use internal::AdmissionReservation;
use internal::BudgetUsage;
use parking_lot::Mutex;

/// Bounds the request payload and count held by detached external write
/// operations.
#[must_use]
pub(super) struct AdmissionBudget {
    /// Maximum aggregate payload bytes held by in-flight admissions.
    max_payload_bytes: NonZeroUsize,
    /// Maximum number of concurrent external write operations.
    max_operations: NonZeroUsize,
    /// Current worker count and payload bytes.
    usage: Mutex<BudgetUsage>,
}

impl AdmissionBudget {
    /// Creates an empty in-flight budget with fixed payload and worker limits.
    ///
    /// # Parameters
    ///
    /// * `max_payload_bytes` - Aggregate payload byte limit.
    /// * `max_operations` - Concurrent admission worker limit.
    ///
    /// # Returns
    ///
    /// A budget with no active reservations.
    pub(super) fn new(max_payload_bytes: NonZeroUsize, max_operations: NonZeroUsize) -> Self {
        Self {
            max_payload_bytes,
            max_operations,
            usage: Mutex::new(BudgetUsage {
                operations: 0,
                payload_bytes: 0,
            }),
        }
    }

    /// Reserves one worker and its payload size without waiting for capacity.
    ///
    /// # Parameters
    ///
    /// * `payload_bytes` - Payload bytes requested by one admission.
    ///
    /// # Returns
    ///
    /// A reservation that releases usage when dropped.
    ///
    /// # Errors
    ///
    /// Returns a budget error if the worker or payload limit would be exceeded.
    pub(super) fn try_reserve(
        self: &Arc<Self>,
        payload_bytes: usize,
    ) -> Result<AdmissionReservation, AdmissionBudgetError> {
        let mut usage = self.usage.lock();
        if usage.operations >= self.max_operations.get() {
            return Err(AdmissionBudgetError::OperationLimitExceeded {
                limit: self.max_operations.get(),
            });
        }
        let Some(next_payload_bytes) = usage.payload_bytes.checked_add(payload_bytes) else {
            return Err(AdmissionBudgetError::PayloadBytesExceeded {
                requested: payload_bytes,
                available: self.max_payload_bytes.get().saturating_sub(usage.payload_bytes),
            });
        };
        if next_payload_bytes > self.max_payload_bytes.get() {
            return Err(AdmissionBudgetError::PayloadBytesExceeded {
                requested: payload_bytes,
                available: self.max_payload_bytes.get() - usage.payload_bytes,
            });
        }
        usage.operations += 1;
        usage.payload_bytes = next_payload_bytes;
        drop(usage);
        Ok(AdmissionReservation {
            budget: Arc::clone(self),
            payload_bytes,
        })
    }
}
