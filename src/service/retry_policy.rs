// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::time::Duration;

/// Retry delay configuration with an exponential cap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    initial_delay: Duration,
    max_delay: Duration,
}

use super::retry_policy_error::RetryPolicyError;

impl RetryPolicy {
    /// Creates a policy after checking that both delays form a valid range.
    pub fn new(initial_delay: Duration, max_delay: Duration) -> Result<Self, RetryPolicyError> {
        if initial_delay.is_zero() {
            return Err(RetryPolicyError::ZeroInitialDelay);
        }
        if max_delay < initial_delay {
            return Err(RetryPolicyError::MaximumBelowInitial);
        }
        Ok(Self {
            initial_delay,
            max_delay,
        })
    }

    /// Returns the exponential delay for a failed attempt, capped at
    /// `max_delay`.
    #[must_use]
    pub fn delay_for_attempt(self, attempt: u32) -> Duration {
        let mut delay = self.initial_delay;
        let mut remaining_doublings = attempt.saturating_sub(1);
        while remaining_doublings > 0 && delay < self.max_delay {
            delay = delay.saturating_mul(2).min(self.max_delay);
            remaining_doublings -= 1;
        }
        delay
    }

    /// Returns the configured initial delay.
    #[must_use]
    pub fn initial_delay(self) -> Duration {
        self.initial_delay
    }

    /// Returns the configured maximum delay.
    #[must_use]
    pub fn max_delay(self) -> Duration {
        self.max_delay
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            initial_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(60),
        }
    }
}
