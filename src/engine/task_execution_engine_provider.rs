// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;

use crate::engine::TaskExecutionEngine;
use crate::model::ResourceCapacity;

/// Factory contract used by the application or SPI assembly.
///
/// This crate-internal extension point is used by engine assembly; the engine
/// module is not part of the crate's public API.
pub trait TaskExecutionEngineProvider: Send + Sync {
    /// Creates an engine with the supplied capacity.
    ///
    /// # Parameters
    ///
    /// * `capacity` - Requested resource capacity for the engine.
    ///
    /// # Returns
    ///
    /// The constructed engine or a provider diagnostic.
    ///
    /// # Errors
    ///
    /// Returns an error when the provider cannot construct the engine.
    fn create(&self, capacity: ResourceCapacity) -> Result<Arc<dyn TaskExecutionEngine>, String>;
}
