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

/// Stable routing and payload contract for one typed task handler.
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
