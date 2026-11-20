// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
// =============================================================================
use std::collections::HashSet;
use std::fmt::Display;

use qubit_model_id::HasModelId;
use qubit_model_id::ModelIdBuf;

use super::handler_registration_error::HandlerRegistrationError;

/// Declares the routing identity and accepted payload schema versions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskHandlerDescriptor {
    kind_id: String,
    payload_type_id: ModelIdBuf,
    accepted_schema_versions: Vec<u32>,
}

impl TaskHandlerDescriptor {
    /// Creates a descriptor whose payload model comes from `T`.
    #[must_use]
    pub fn for_payload<T: HasModelId>(kind: impl Into<String>, schema: u32) -> Self {
        Self::new(kind.into(), T::MODEL_ID.into(), vec![schema])
    }

    /// Creates a descriptor from an explicitly configured payload model ID.
    pub fn try_new<M: TryInto<ModelIdBuf>>(
        kind: impl Into<String>,
        model: M,
        schema: u32,
    ) -> Result<Self, HandlerRegistrationError>
    where
        M::Error: Display,
    {
        let model = model
            .try_into()
            .map_err(|error| HandlerRegistrationError::InvalidPayloadModel(error.to_string()))?;
        Ok(Self::new(kind.into(), model, vec![schema]))
    }

    fn new(
        kind_id: String,
        payload_type_id: ModelIdBuf,
        accepted_schema_versions: Vec<u32>,
    ) -> Self {
        Self {
            kind_id,
            payload_type_id,
            accepted_schema_versions,
        }
    }

    /// Returns this descriptor's stable handler kind.
    #[must_use]
    pub fn kind_id(&self) -> &str {
        &self.kind_id
    }

    /// Returns the model identity accepted by this handler.
    #[must_use]
    pub fn payload_type_id(&self) -> &ModelIdBuf {
        &self.payload_type_id
    }

    /// Returns the accepted payload schema versions.
    #[must_use]
    pub fn accepted_schema_versions(&self) -> &[u32] {
        &self.accepted_schema_versions
    }

    /// Replaces the accepted schema versions.
    #[must_use]
    pub fn with_accepted_schema_versions(
        mut self,
        versions: impl IntoIterator<Item = u32>,
    ) -> Self {
        self.accepted_schema_versions = versions.into_iter().collect();
        self
    }

    pub(crate) fn validate(&self) -> Result<(), HandlerRegistrationError> {
        if self.kind_id.trim().is_empty() {
            return Err(HandlerRegistrationError::EmptyKindId);
        }
        if self.accepted_schema_versions.is_empty() {
            return Err(HandlerRegistrationError::NoAcceptedSchemaVersions);
        }
        let mut versions = HashSet::with_capacity(self.accepted_schema_versions.len());
        for version in &self.accepted_schema_versions {
            if *version == 0 {
                return Err(HandlerRegistrationError::ZeroSchemaVersion);
            }
            if !versions.insert(*version) {
                return Err(HandlerRegistrationError::DuplicateSchemaVersion(*version));
            }
        }
        Ok(())
    }
}
