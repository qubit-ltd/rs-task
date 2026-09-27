// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::convert::Infallible;
use std::sync::Arc;

use qubit_spi::ProviderDescriptor;
use qubit_spi::ProviderMetadata;
use qubit_spi::ServiceProvider;
use qubit_spi::error::ProviderFailure;
use qubit_spi::provider_descriptor;

use crate::scheduling::FairFifoPolicy;
use crate::scheduling::SchedulingPolicy;
use crate::spi::SchedulingPolicySpec;

/// Supplies the built-in fair FIFO scheduling policy.
pub(crate) struct FairFifoProvider;

impl ProviderMetadata for FairFifoProvider {
    /// Returns the stable provider identifier used by application assembly.
    ///
    /// # Returns
    ///
    /// The fair FIFO provider descriptor.
    fn descriptor(&self) -> ProviderDescriptor {
        provider_descriptor!("qubit.task.scheduler.fair-fifo")
    }
}

impl ServiceProvider<SchedulingPolicySpec> for FairFifoProvider {
    /// Creates the default fair FIFO policy, which has no runtime settings.
    ///
    /// # Parameters
    ///
    /// * `_config` - Unit configuration because the policy has no settings.
    ///
    /// # Returns
    ///
    /// A shared fair FIFO scheduling policy.
    ///
    /// # Errors
    ///
    /// The infallible provider does not return a creation error.
    fn create_configured(&self, _config: &()) -> Result<Arc<dyn SchedulingPolicy>, ProviderFailure<Infallible>> {
        Ok(Arc::new(FairFifoPolicy::default()))
    }
}
