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

use crate::engine::LocalTaskExecutionEngine;
use crate::engine::TaskExecutionEngine;
use crate::model::ResourceCapacity;
use crate::spi::TaskExecutionEngineSpec;

/// Supplies the built-in single-process execution engine.
pub(crate) struct LocalEngineProvider;

impl ProviderMetadata for LocalEngineProvider {
    /// Returns the stable provider identifier used by application assembly.
    ///
    /// # Returns
    ///
    /// The local execution engine provider descriptor.
    fn descriptor(&self) -> ProviderDescriptor {
        provider_descriptor!("qubit.task.engine.local")
    }
}

impl ServiceProvider<TaskExecutionEngineSpec> for LocalEngineProvider {
    /// Creates a local engine using the supplied resource capacity.
    ///
    /// # Parameters
    ///
    /// * `config` - CPU, GPU, and custom resource limits for the engine.
    ///
    /// # Returns
    ///
    /// A shared local execution engine.
    ///
    /// # Errors
    ///
    /// The infallible provider does not return a creation error.
    fn create_configured(
        &self,
        config: &ResourceCapacity,
    ) -> Result<Arc<dyn TaskExecutionEngine>, ProviderFailure<Infallible>> {
        Ok(Arc::new(LocalTaskExecutionEngine::new(config.clone())))
    }
}
