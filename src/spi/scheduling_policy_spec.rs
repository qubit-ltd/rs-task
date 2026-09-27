// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::convert::Infallible;
use std::sync::Arc;

use qubit_spi::ServiceSpec;
use qubit_spi::SyncServiceSpec;

use crate::scheduling::SchedulingPolicy;

/// Service family for an ordering policy.
///
/// # Examples
///
/// ```
/// use qubit_spi::ServiceSpec;
/// use qubit_task::spi::SchedulingPolicySpec;
///
/// let config: <SchedulingPolicySpec as ServiceSpec>::Config = ();
/// assert_eq!(config, ());
/// ```
pub struct SchedulingPolicySpec;

impl ServiceSpec for SchedulingPolicySpec {
    /// Empty configuration because the built-in policy has no construction
    /// options.
    type Config = ();
    /// Infallible marker because policy construction has no provider-specific
    /// failure.
    type Error = Infallible;
}

impl SyncServiceSpec for SchedulingPolicySpec {
    /// Shared scheduling policy instance created for the selected provider.
    type Output = Arc<dyn SchedulingPolicy>;
}
