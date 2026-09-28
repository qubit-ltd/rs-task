// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
// qubit-style: allow multiple-public-types
use std::num::NonZeroUsize;
use std::sync::Arc;

use parking_lot::Mutex;

/// Reason an in-flight admission could not reserve bounded resources.
///
/// The caller can reject promptly and report the relevant configured limit.
#[must_use]
pub(super) enum AdmissionBudgetError {
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

/// Current worker count and payload bytes held under the admission lock.
struct BudgetUsage {
    /// Number of external write operations holding reservations.
    operations: usize,
    /// Payload bytes retained by those workers.
    payload_bytes: usize,
}

/// Bounds the request payload and count held by detached external write
/// operations.
pub(super) struct AdmissionBudget {
    /// Maximum aggregate payload bytes held by in-flight admissions.
    max_payload_bytes: NonZeroUsize,
    /// Maximum number of concurrent external write operations.
    max_operations: NonZeroUsize,
    /// Current worker count and payload bytes.
    usage: Mutex<BudgetUsage>,
}

/// Releases one payload and submission reservation when its admission worker
/// ends.
pub(super) struct AdmissionReservation {
    /// Budget whose usage is decremented when this reservation is dropped.
    budget: Arc<AdmissionBudget>,
    /// Payload bytes charged to this reservation.
    payload_bytes: usize,
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
