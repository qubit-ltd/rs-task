// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;

use super::TaskHandler;
use super::TaskHandlerDescriptor;

/// Task handler factory contract shared by SPI providers and direct
/// registration.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
///
/// use qubit_task::handler::LocalTaskHandler;
/// use qubit_task::handler::TaskHandler;
/// use qubit_task::handler::TaskHandlerDescriptor;
/// use qubit_task::handler::TaskHandlerProvider;
/// use qubit_task::handler::TaskRunOutcome;
/// use qubit_task::model::TaskOutput;
///
/// struct Provider;
/// impl TaskHandlerProvider for Provider {
///     fn descriptor(&self) -> TaskHandlerDescriptor {
///         TaskHandlerDescriptor { task_type: "once".into(), version: "1".into() }
///     }
///
///     fn create(&self) -> Result<Arc<dyn TaskHandler>, String> {
///         Ok(Arc::new(LocalTaskHandler::new(self.descriptor(), |_| {
///             Ok(TaskRunOutcome::Succeeded(TaskOutput::default()))
///         })))
///     }
/// }
///
/// assert!(Provider.create().is_ok());
/// ```
pub trait TaskHandlerProvider: Send + Sync {
    /// Returns the stable handler type and version supplied by this provider.
    ///
    /// # Returns
    ///
    /// The descriptor used to select the handler for stored requests.
    #[must_use]
    fn descriptor(&self) -> TaskHandlerDescriptor;

    /// Builds the handler instance during application assembly.
    ///
    /// # Returns
    ///
    /// The constructed handler or a provider diagnostic.
    ///
    /// # Errors
    ///
    /// Returns an error when provider configuration prevents handler creation.
    fn create(&self) -> Result<Arc<dyn TaskHandler>, String>;
}
