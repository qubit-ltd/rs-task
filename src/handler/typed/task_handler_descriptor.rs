// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::collections::HashSet;

use qubit_model_id::ModelIdBuf;

use super::cancellation_mode::CancellationMode;
use super::handler_registration_error::HandlerRegistrationError;

/// Declares the routing, payload schema, and cancellation contract for a typed
/// handler.
///
/// A descriptor is valid when its routing key is non-empty, it accepts at least
/// one schema version, and each accepted version appears only once. Registries
/// use the descriptor to select handlers before decoding task payloads.
///
/// # Examples
///
/// ```
/// use qubit_task::CancellationMode;
/// use qubit_task::TaskHandlerDescriptor;
/// use qubit_model_id::ModelIdBuf;
///
/// let descriptor = TaskHandlerDescriptor {
///     kind_id: "email.send".to_owned(),
///     payload_type_id: ModelIdBuf::parse("example.SendEmail").expect("valid model ID"),
///     accepted_schema_versions: vec![1, 2],
///     cancellation_mode: CancellationMode::Cooperative,
/// };
/// assert_eq!(descriptor.accepted_schema_versions, vec![1, 2]);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskHandlerDescriptor {
    /// Stable handler family used for routing.
    pub kind_id: String,
    /// The sole payload model accepted by this handler.
    pub payload_type_id: ModelIdBuf,
    /// Explicit schema versions accepted by this handler.
    pub accepted_schema_versions: Vec<u32>,
    /// In-flight cancellation behavior supported by this handler.
    pub cancellation_mode: CancellationMode,
}

impl TaskHandlerDescriptor {
    /// Checks the fields required for safe handler registration.
    ///
    /// Returns an error when the routing key is blank, no schema versions are
    /// accepted, or a schema version is listed more than once. This check does
    /// not validate registry-wide conflicts or cancellation-hook
    /// configuration.
    pub(crate) fn validate(&self) -> Result<(), HandlerRegistrationError> {
        if self.kind_id.trim().is_empty() {
            return Err(HandlerRegistrationError::EmptyKindId);
        }
        if self.accepted_schema_versions.is_empty() {
            return Err(HandlerRegistrationError::NoAcceptedSchemaVersions);
        }
        let mut versions = HashSet::with_capacity(self.accepted_schema_versions.len());
        for version in &self.accepted_schema_versions {
            if !versions.insert(*version) {
                return Err(HandlerRegistrationError::DuplicateSchemaVersion(*version));
            }
        }
        Ok(())
    }
}
