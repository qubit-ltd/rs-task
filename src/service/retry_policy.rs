// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::time::Duration;

use super::retry_policy_error::RetryPolicyError;

/// Retry delay configuration with an exponential cap.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use qubit_task::RetryPolicy;
///
/// let policy = RetryPolicy::new(Duration::from_secs(1), Duration::from_secs(8)).unwrap();
/// assert_eq!(policy.delay_for_attempt(4), Duration::from_secs(8));
/// ```
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Delay assigned to the first retry.
    initial_delay: Duration,
    /// Upper bound applied after exponential delay growth.
    max_delay: Duration,
}

impl RetryPolicy {
    /// Creates a policy after checking that both delays form a valid range.
    ///
    /// # Parameters
    ///
    /// * `initial_delay` - Positive delay used for the first retry.
    /// * `max_delay` - Upper bound applied to all retry delays.
    ///
    /// # Returns
    ///
    /// A policy with the requested retry delay range.
    ///
    /// # Errors
    ///
    /// Returns [`RetryPolicyError::ZeroInitialDelay`] when the initial delay
    /// is zero, or [`RetryPolicyError::MaximumBelowInitial`] when the maximum
    /// is less than the initial delay.
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
    ///
    /// # Parameters
    ///
    /// * `attempt` - One-based attempt number; zero uses the first delay.
    ///
    /// # Returns
    ///
    /// The exponentially increased delay, never greater than `max_delay`.
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

    /// Returns the delay in whole milliseconds, rounding positive fractions up.
    ///
    /// The millisecond timestamp used for persisted retry deadlines cannot
    /// represent sub-millisecond delays. Rounding up keeps every positive
    /// configured delay positive after conversion. `max_delay` caps the raw
    /// `Duration`; this rounding can make the persisted interval exceed it by
    /// less than one millisecond.
    ///
    /// # Parameters
    ///
    /// * `attempt` - One-based attempt number; zero uses the first delay.
    ///
    /// # Returns
    ///
    /// The capped exponential delay rounded up to milliseconds and saturated
    /// at `u64::MAX`.
    #[must_use]
    pub(crate) fn delay_millis_for_attempt(self, attempt: u32) -> u64 {
        let delay = self.delay_for_attempt(attempt);
        let millis = delay.as_millis() + u128::from(!delay.subsec_nanos().is_multiple_of(1_000_000));
        millis.min(u128::from(u64::MAX)) as u64
    }

    /// Returns the configured initial delay.
    ///
    /// # Returns
    ///
    /// The delay used for the first retry.
    #[must_use]
    #[inline]
    pub fn initial_delay(self) -> Duration {
        self.initial_delay
    }

    /// Returns the configured maximum delay.
    ///
    /// # Returns
    ///
    /// The largest delay this policy returns.
    #[must_use]
    #[inline]
    pub fn max_delay(self) -> Duration {
        self.max_delay
    }
}

impl Default for RetryPolicy {
    /// Returns the service's default one-second to sixty-second backoff.
    ///
    /// # Returns
    ///
    /// A retry policy with an exponential delay capped at sixty seconds.
    fn default() -> Self {
        Self {
            initial_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(60),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::RetryPolicy;

    #[test]
    fn test_delay_millis_for_attempt_rounds_positive_fractional_milliseconds_up() {
        for (delay, expected_ms) in [
            (Duration::from_nanos(1), 1),
            (Duration::from_nanos(999_999), 1),
            (Duration::from_millis(1), 1),
            (Duration::from_nanos(1_000_001), 2),
        ] {
            let policy = RetryPolicy::new(delay, delay).unwrap();
            assert_eq!(policy.delay_millis_for_attempt(1), expected_ms);
            assert_eq!(policy.delay_for_attempt(1), delay);
        }
    }

    #[test]
    fn test_delay_millis_for_attempt_rounds_after_growth_and_handles_extremes() {
        let policy = RetryPolicy::new(Duration::from_nanos(600_001), Duration::from_nanos(1_500_001)).unwrap();
        assert_eq!(policy.delay_millis_for_attempt(0), 1);
        assert_eq!(policy.delay_millis_for_attempt(2), 2);
        assert_eq!(policy.delay_millis_for_attempt(3), 2);
        assert_eq!(policy.delay_millis_for_attempt(u32::MAX), 2);

        let maximum = RetryPolicy::new(Duration::MAX, Duration::MAX).unwrap();
        assert_eq!(maximum.delay_millis_for_attempt(u32::MAX), u64::MAX);
    }
}
