// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
// =============================================================================
/// Invalid retry delay configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RetryPolicyError {
    /// The initial delay must be positive.
    #[error("initial retry delay must be greater than zero")]
    ZeroInitialDelay,
    /// The maximum delay must be at least the initial delay.
    #[error("maximum retry delay must be at least the initial delay")]
    MaximumBelowInitial,
}
