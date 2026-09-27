// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
/// Invalid retry delay configuration.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
///
/// use qubit_task::RetryPolicy;
/// use qubit_task::service::RetryPolicyError;
///
/// let error = RetryPolicy::new(Duration::ZERO, Duration::from_secs(1)).unwrap_err();
/// assert_eq!(error, RetryPolicyError::ZeroInitialDelay);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[must_use]
pub enum RetryPolicyError {
    /// The initial delay must be positive.
    #[error("initial retry delay must be greater than zero")]
    ZeroInitialDelay,
    /// The maximum delay must be at least the initial delay.
    #[error("maximum retry delay must be at least the initial delay")]
    MaximumBelowInitial,
}
