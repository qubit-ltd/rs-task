// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
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
