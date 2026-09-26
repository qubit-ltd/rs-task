// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
// =============================================================================
use std::num::NonZeroUsize;
use std::sync::Arc;

use parking_lot::Mutex;

pub(super) enum AdmissionBudgetError {
    PayloadBytesExceeded { requested: usize, available: usize },
    SubmissionLimitExceeded { limit: usize },
}

struct BudgetUsage {
    submissions: usize,
    payload_bytes: usize,
}

/// Bounds the request payload and count held by detached admission workers.
pub(super) struct AdmissionBudget {
    max_payload_bytes: NonZeroUsize,
    max_submissions: NonZeroUsize,
    usage: Mutex<BudgetUsage>,
}

/// Releases one payload and submission reservation when its admission worker
/// ends.
pub(super) struct AdmissionReservation {
    budget: Arc<AdmissionBudget>,
    payload_bytes: usize,
}

impl AdmissionBudget {
    /// Creates an empty in-flight budget with fixed payload and worker limits.
    pub(super) fn new(max_payload_bytes: NonZeroUsize, max_submissions: NonZeroUsize) -> Self {
        Self {
            max_payload_bytes,
            max_submissions,
            usage: Mutex::new(BudgetUsage {
                submissions: 0,
                payload_bytes: 0,
            }),
        }
    }

    /// Reserves one worker and its payload size without waiting for capacity.
    pub(super) fn try_reserve(
        self: &Arc<Self>,
        payload_bytes: usize,
    ) -> Result<AdmissionReservation, AdmissionBudgetError> {
        let mut usage = self.usage.lock();
        if usage.submissions >= self.max_submissions.get() {
            return Err(AdmissionBudgetError::SubmissionLimitExceeded {
                limit: self.max_submissions.get(),
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
        usage.submissions += 1;
        usage.payload_bytes = next_payload_bytes;
        drop(usage);
        Ok(AdmissionReservation {
            budget: Arc::clone(self),
            payload_bytes,
        })
    }
}

impl Drop for AdmissionReservation {
    fn drop(&mut self) {
        let mut usage = self.budget.usage.lock();
        debug_assert!(usage.submissions > 0);
        debug_assert!(usage.payload_bytes >= self.payload_bytes);
        usage.submissions -= 1;
        usage.payload_bytes -= self.payload_bytes;
    }
}
