// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::collections::HashMap;
use std::sync::Arc;

use super::RegistryError;
use super::TaskHandler;
use super::TaskHandlerDescriptor;
// Stores one handler and the identity of its registration source.
mod internal;
use internal::RegisteredHandler;

/// Resolves task handlers by their exact task type and version.
///
/// Duplicate descriptors are rejected so resolution always identifies at most
/// one handler.
///
/// # Examples
///
/// ```
/// use qubit_task::handler::TaskHandlerRegistry;
///
/// let registry = TaskHandlerRegistry::new();
/// assert!(registry.resolve("resize", "2").is_none());
/// ```
#[derive(Default)]
pub struct TaskHandlerRegistry {
    /// Handlers indexed by exact task type and version.
    handlers: HashMap<TaskHandlerDescriptor, RegisteredHandler>,
}

impl TaskHandlerRegistry {
    /// Creates an empty registry.
    ///
    /// # Returns
    ///
    /// An empty registry with no resolvable handler descriptors.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers one handler and rejects duplicate type/version declarations.
    ///
    /// # Parameters
    ///
    /// * `handler` - Handler registered under its descriptor.
    ///
    /// # Returns
    ///
    /// Success when the handler is registered.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError::InvalidDescriptor`] for empty keys or
    /// [`RegistryError::Duplicate`] when the exact key is already registered.
    pub fn register(&mut self, handler: Arc<dyn TaskHandler>) -> Result<(), RegistryError> {
        self.register_with_source(handler, "direct registration")
    }

    /// Registers one handler and retains its provider identity for conflict
    /// diagnostics.
    ///
    /// # Parameters
    ///
    /// * `handler` - Handler registered under its descriptor.
    /// * `source` - Provider or application identity used in duplicate errors.
    ///
    /// # Returns
    ///
    /// Success when the handler is registered.
    ///
    /// # Errors
    ///
    /// Returns an error for empty descriptor fields or an existing exact key.
    pub fn register_with_source(
        &mut self,
        handler: Arc<dyn TaskHandler>,
        source: impl Into<String>,
    ) -> Result<(), RegistryError> {
        let key = handler.descriptor();
        if key.task_type.is_empty() || key.version.is_empty() {
            return Err(RegistryError::InvalidDescriptor);
        }
        let source = source.into();
        if let Some(existing) = self.handlers.get(&key) {
            return Err(RegistryError::Duplicate {
                task_type: key.task_type,
                version: key.version,
                first_source: existing.source.clone(),
                second_source: source,
            });
        }
        self.handlers.insert(key, RegisteredHandler { handler, source });
        Ok(())
    }

    /// Finds only an exact type and version match.
    ///
    /// # Parameters
    ///
    /// * `task_type` - Stable business task family.
    /// * `version` - Exact payload version required by the request.
    ///
    /// # Returns
    ///
    /// A shared handler when the complete key matches, or `None` otherwise.
    #[must_use]
    pub fn resolve(&self, task_type: &str, version: &str) -> Option<Arc<dyn TaskHandler>> {
        self.handlers
            .get(&TaskHandlerDescriptor {
                task_type: task_type.to_owned(),
                version: version.to_owned(),
            })
            .map(|registered| registered.handler.clone())
    }
}
