// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::collections::HashSet;
use std::fmt::Display;

use qubit_model_id::HasModelId;
use qubit_model_id::ModelIdBuf;

use super::handler_registration_error::HandlerRegistrationError;

/// Declares the routing identity and accepted payload schema versions.
///
/// The descriptor binds one stable handler kind to a payload model and one or
/// more nonzero schema versions. Registries validate it before registration.
///
/// # Examples
///
/// ```
/// use qubit_model_id::HasModelId;
/// use qubit_model_id::ModelId;
/// use qubit_task::TaskHandlerDescriptor;
///
/// struct Email;
/// impl HasModelId for Email {
///     const MODEL_ID: ModelId = ModelId::new("example.Email");
/// }
///
/// let descriptor = TaskHandlerDescriptor::for_payload::<Email>("email.send", 1);
/// assert_eq!(descriptor.kind_id(), "email.send");
/// assert_eq!(descriptor.accepted_schema_versions(), [1]);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskHandlerDescriptor {
    kind_id: String,
    payload_type_id: ModelIdBuf,
    accepted_schema_versions: Vec<u32>,
}

impl TaskHandlerDescriptor {
    /// Creates a descriptor whose payload model comes from `T`.
    ///
    /// # Type Parameters
    ///
    /// * `T` supplies the stable payload model ID.
    ///
    /// # Parameters
    ///
    /// * `kind` is the stable routing identity.
    /// * `schema` is the accepted payload schema version.
    ///
    /// # Returns
    ///
    /// A descriptor accepting the supplied kind and schema for `T`.
    #[must_use]
    pub fn for_payload<T: HasModelId>(kind: impl Into<String>, schema: u32) -> Self {
        Self::new(kind.into(), T::MODEL_ID.into(), vec![schema])
    }

    /// Creates a descriptor from an explicitly configured payload model ID.
    ///
    /// # Type Parameters
    ///
    /// * `M` is a value convertible to a model ID buffer.
    ///
    /// # Parameters
    ///
    /// * `kind` is the stable routing identity.
    /// * `model` identifies the payload model accepted by the handler.
    /// * `schema` is the accepted payload schema version.
    ///
    /// # Returns
    ///
    /// A descriptor containing the supplied identity and schema.
    ///
    /// # Errors
    ///
    /// Returns an error when `model` cannot be converted to a model ID.
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

    /// Builds a descriptor from already validated identity components.
    fn new(kind_id: String, payload_type_id: ModelIdBuf, accepted_schema_versions: Vec<u32>) -> Self {
        Self {
            kind_id,
            payload_type_id,
            accepted_schema_versions,
        }
    }

    /// Returns this descriptor's stable handler kind.
    ///
    /// # Returns
    ///
    /// The handler kind identifier borrowed from this descriptor.
    #[must_use]
    pub fn kind_id(&self) -> &str {
        &self.kind_id
    }

    /// Returns the model identity accepted by this handler.
    ///
    /// # Returns
    ///
    /// The payload model ID borrowed from this descriptor.
    #[must_use]
    pub fn payload_type_id(&self) -> &ModelIdBuf {
        &self.payload_type_id
    }

    /// Returns the accepted payload schema versions.
    ///
    /// # Returns
    ///
    /// The configured, nonzero schema versions in registration order.
    #[must_use]
    pub fn accepted_schema_versions(&self) -> &[u32] {
        &self.accepted_schema_versions
    }

    /// Replaces the accepted schema versions.
    ///
    /// # Parameters
    ///
    /// * `versions` supplies the versions accepted by the handler.
    ///
    /// # Returns
    ///
    /// The updated descriptor; registration later rejects an empty list,
    /// duplicates, or version zero.
    #[must_use]
    pub fn with_accepted_schema_versions(mut self, versions: impl IntoIterator<Item = u32>) -> Self {
        self.accepted_schema_versions = versions.into_iter().collect();
        self
    }

    /// Checks the descriptor invariants required by registry insertion.
    ///
    /// # Returns
    ///
    /// `Ok(())` when the kind and schema list are valid.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty kind, an empty schema list, a duplicate
    /// schema, or reserved version zero.
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
