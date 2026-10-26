// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public retry delay configuration contract.

use std::time::Duration;

use qubit_task::RetryPolicy;
use qubit_task::RetryPolicyError;

#[test]
fn test_new_rejects_maximum_below_initial() {
    assert_eq!(
        RetryPolicy::new(Duration::from_secs(2), Duration::from_secs(1)),
        Err(RetryPolicyError::MaximumBelowInitial)
    );
}

#[test]
fn test_default_has_documented_retry_bounds() {
    let policy = RetryPolicy::default();

    assert_eq!(policy.initial_delay(), Duration::from_secs(1));
    assert_eq!(policy.max_delay(), Duration::from_secs(60));
    assert_eq!(policy.delay_for_attempt(1), Duration::from_secs(1));
    assert_eq!(policy.delay_for_attempt(7), Duration::from_secs(60));
}

#[test]
fn test_new_rejects_zero_initial_delay_before_range_check() {
    assert_eq!(
        RetryPolicy::new(Duration::ZERO, Duration::ZERO),
        Err(RetryPolicyError::ZeroInitialDelay)
    );
}

#[test]
fn test_delay_for_attempt_covers_initial_growth_cap_and_extreme_attempt() {
    let policy =
        RetryPolicy::new(Duration::from_millis(125), Duration::from_millis(900)).expect("valid retry interval");

    assert_eq!(policy.delay_for_attempt(0), Duration::from_millis(125));
    assert_eq!(policy.delay_for_attempt(1), Duration::from_millis(125));
    assert_eq!(policy.delay_for_attempt(2), Duration::from_millis(250));
    assert_eq!(policy.delay_for_attempt(3), Duration::from_millis(500));
    assert_eq!(policy.delay_for_attempt(4), Duration::from_millis(900));
    assert_eq!(policy.delay_for_attempt(u32::MAX), Duration::from_millis(900));
}

#[test]
fn test_delay_for_attempt_with_equal_bounds_is_constant() {
    let delay = Duration::from_millis(17);
    let policy = RetryPolicy::new(delay, delay).expect("equal bounds are valid");

    for attempt in [0, 1, 2, 31, u32::MAX] {
        assert_eq!(policy.delay_for_attempt(attempt), delay);
    }
    assert_eq!(policy.initial_delay(), delay);
    assert_eq!(policy.max_delay(), delay);
}
