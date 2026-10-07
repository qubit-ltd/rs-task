// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use qubit_metadata::Metadata;
use qubit_model_id::HasModelId;

use super::Payload;
use super::ResourceRequest;
use super::StoredTaskRequest;
use super::TaskRequestEncodeError;

/// Maximum metadata entry count allowed on one task request.
pub const MAX_TASK_METADATA_ENTRIES: usize = 32;
/// Maximum serialized metadata size allowed on one task request.
pub const MAX_TASK_METADATA_BYTES: usize = 16 * 1024;

/// Typed task submission before encoding and durable acceptance.
#[derive(Debug, Clone, PartialEq)]
pub struct TaskRequest<T> {
    /// Handler routing identity.
    pub kind_id: String,
    /// User-visible query category, independent from handler routing.
    pub category: Option<String>,
    /// Typed and versioned task payload.
    pub payload: Payload<T>,
    /// Structured metadata governed by `rs-metadata` wire quotas.
    pub metadata: Metadata,
    /// Scheduling quota requested by the task.
    pub resource_limit: ResourceRequest,
    /// Optional caller-defined correlation key.
    pub correlation_key: Option<String>,
    /// Optional idempotency key.
    pub idempotency_key: Option<String>,
}

impl<T: HasModelId> TaskRequest<T> {
    /// Creates a typed request with the supplied routing and payload identity.
    /// The payload model identity always comes from `T::MODEL_ID`.
    ///
    /// Supplying an independent model ID is no longer supported:
    ///
    /// ```compile_fail
    /// use qubit_codec::ValueCodecId;
    /// use qubit_model_id::{HasModelId, ModelId};
    /// use qubit_task::TaskRequest;
    /// struct Input;
    /// impl HasModelId for Input {
    ///     const MODEL_ID: ModelId = ModelId::new("example.Input");
    /// }
    /// let _ = TaskRequest::new(
    ///     "example.kind", ModelId::new("some.other.Model"), 1,
    ///     ValueCodecId::new("example.codec"), Input,
    /// );
    /// ```
    pub fn new(
        kind_id: impl Into<String>,
        schema_version: u32,
        codec_id: qubit_codec::ValueCodecId,
        data: T,
    ) -> Self {
        Self {
            kind_id: kind_id.into(),
            category: None,
            payload: Payload::new(schema_version, codec_id, data),
            metadata: Metadata::new(),
            resource_limit: ResourceRequest::default(),
            correlation_key: None,
            idempotency_key: None,
        }
    }
}

impl<T: HasModelId + 'static> TaskRequest<T> {
    /// Encodes a typed request for durable storage after validating metadata
    /// budgets.
    ///
    /// # Parameters
    ///
    /// * `self` - The typed request being prepared for persistence.
    /// * `registry` - The immutable bytes codec registry used for the payload.
    ///
    /// # Returns
    ///
    /// A type-erased stored request with its payload bytes and metadata intact.
    ///
    /// # Errors
    ///
    /// Returns an error when metadata exceeds the task entry/byte limits, its
    /// JSON encoding violates `rs-metadata` wire budgets, or payload codec
    /// lookup/encoding fails.
    pub fn encode(
        self,
        registry: &qubit_codec::ValueBytesCodecRegistry,
    ) -> Result<StoredTaskRequest, TaskRequestEncodeError> {
        if self.kind_id.trim().is_empty() {
            return Err(TaskRequestEncodeError::EmptyKindId);
        }
        self.resource_limit
            .validate_limits()
            .map_err(|error| TaskRequestEncodeError::Resource(error.to_string()))?;
        if self.metadata.len() > MAX_TASK_METADATA_ENTRIES {
            return Err(TaskRequestEncodeError::TooManyMetadataEntries(
                self.metadata.len(),
            ));
        }
        let metadata_bytes = self
            .metadata
            .to_json_vec()
            .map_err(TaskRequestEncodeError::Metadata)?;
        if metadata_bytes.len() > MAX_TASK_METADATA_BYTES {
            return Err(TaskRequestEncodeError::MetadataTooLarge(
                metadata_bytes.len(),
            ));
        }
        let payload = self.payload.encode(registry)?.into_stored();
        if payload.bytes.len() > crate::model::MAX_TASK_PAYLOAD_BYTES {
            return Err(TaskRequestEncodeError::PayloadTooLarge(payload.bytes.len()));
        }
        Ok(StoredTaskRequest {
            kind_id: self.kind_id,
            category: self.category,
            payload,
            metadata: self.metadata,
            resource_limit: self.resource_limit,
            correlation_key: self.correlation_key,
            idempotency_key: self.idempotency_key,
        })
    }
}
