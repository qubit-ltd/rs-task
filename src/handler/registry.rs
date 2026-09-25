// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::collections::HashMap;
use std::sync::Arc;

use serde::Deserialize;
use serde::Serialize;

/// Stable task family and exact handler version key.
///
/// Both components participate in exact matching; changing either creates a
/// distinct handler registration.
///
/// # Examples
///
/// ```
/// use qubit_task::handler::TaskHandlerDescriptor;
///
/// let descriptor = TaskHandlerDescriptor { task_type: "resize".into(), version: "2".into() };
/// assert_eq!(descriptor.version, "2");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TaskHandlerDescriptor {
    /// Stable business task family.
    pub task_type: String,
    /// Exact payload interpretation version.
    pub version: String,
}

/// Explicit outcome of one handler execution attempt.
///
/// Cancellation is acknowledged only after the handler has stopped its work.
///
/// # Examples
///
/// ```
/// use qubit_task::handler::TaskRunOutcome;
/// use qubit_task::model::TaskOutput;
///
/// let outcome = TaskRunOutcome::Succeeded(TaskOutput::default());
/// assert!(matches!(outcome, TaskRunOutcome::Succeeded(_)));
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskRunOutcome {
    /// Execution completed successfully with a small persisted summary.
    Succeeded(crate::model::TaskOutput),
    /// The handler acknowledged cancellation and stopped work.
    Cancelled,
}

/// Handler result: an explicit outcome or classified failure.
pub type TaskRunResult = Result<TaskRunOutcome, crate::model::TaskRunError>;

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
    handlers: HashMap<TaskHandlerDescriptor, RegisteredHandler>,
}

struct RegisteredHandler {
    handler: Arc<dyn super::TaskHandler>,
    source: String,
}

impl TaskHandlerRegistry {
    /// Creates an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers one handler and rejects duplicate type/version declarations.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError::InvalidDescriptor`] for empty keys or
    /// [`RegistryError::Duplicate`] when the exact key is already registered.
    pub fn register(&mut self, handler: Arc<dyn super::TaskHandler>) -> Result<(), RegistryError> {
        self.register_with_source(handler, "direct registration")
    }

    /// Registers one handler and retains its provider identity for conflict
    /// diagnostics.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError::InvalidDescriptor`] for empty keys or
    /// [`RegistryError::Duplicate`] when another source owns the exact key.
    pub fn register_with_source(
        &mut self,
        handler: Arc<dyn super::TaskHandler>,
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
    pub fn resolve(&self, task_type: &str, version: &str) -> Option<Arc<dyn super::TaskHandler>> {
        self.handlers
            .get(&TaskHandlerDescriptor {
                task_type: task_type.to_owned(),
                version: version.to_owned(),
            })
            .map(|registered| registered.handler.clone())
    }
}

/// Describes a handler registry conflict.
///
/// # Examples
///
/// ```
/// use qubit_task::handler::RegistryError;
///
/// let error = RegistryError::InvalidDescriptor;
/// assert!(error.to_string().contains("must not be empty"));
/// ```
#[derive(Debug, thiserror::Error)]
pub enum RegistryError {
    /// Another provider already registered the same task type and version.
    #[error("duplicate handler for `{task_type}` version `{version}` from `{first_source}` and `{second_source}`")]
    Duplicate {
        /// Task family claimed by both handlers.
        task_type: String,
        /// Payload version claimed by both handlers.
        version: String,
        /// Source that registered the handler first.
        first_source: String,
        /// Source that attempted the conflicting registration.
        second_source: String,
    },
    /// A handler declared an empty task type or version.
    #[error("handler task type and version must not be empty")]
    InvalidDescriptor,
}
