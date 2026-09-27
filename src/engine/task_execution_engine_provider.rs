// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;

use super::TaskExecutionEngine;
use crate::model::ResourceCapacity;

/// Factory contract used by the application or SPI assembly.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
///
/// use qubit_task::engine::LocalTaskExecutionEngine;
/// use qubit_task::engine::TaskExecutionEngine;
/// use qubit_task::engine::TaskExecutionEngineProvider;
/// use qubit_task::model::ResourceCapacity;
///
/// struct Provider;
/// impl TaskExecutionEngineProvider for Provider {
///     fn create(&self, capacity: ResourceCapacity) -> Result<Arc<dyn TaskExecutionEngine>, String> {
///         Ok(Arc::new(LocalTaskExecutionEngine::new(capacity)))
///     }
/// }
///
/// assert!(Provider.create(ResourceCapacity::default()).is_ok());
/// ```
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
