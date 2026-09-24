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
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TaskHandlerDescriptor {
    /// Stable business task family.
    pub task_type: String,
    /// Exact payload interpretation version.
    pub version: String,
}

/// Handler result: a small persisted output or classified failure.
pub type TaskRunResult = Result<crate::model::TaskOutput, crate::model::TaskRunError>;

/// Resolves task handlers by their exact task type and version.
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
    pub fn register(&mut self, handler: Arc<dyn super::TaskHandler>) -> Result<(), RegistryError> {
        self.register_with_source(handler, "direct registration")
    }

    /// Registers one handler and retains its provider identity for conflict
    /// diagnostics.
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
#[derive(Debug, thiserror::Error)]
pub enum RegistryError {
    /// Another provider already registered the same task type and version.
    #[error("duplicate handler for `{task_type}` version `{version}` from `{first_source}` and `{second_source}`")]
    Duplicate {
        task_type: String,
        version: String,
        first_source: String,
        second_source: String,
    },
    /// A handler declared an empty task type or version.
    #[error("handler task type and version must not be empty")]
    InvalidDescriptor,
}
