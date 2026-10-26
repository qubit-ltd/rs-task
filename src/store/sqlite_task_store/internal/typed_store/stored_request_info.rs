// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use qubit_metadata::Metadata;
use serde::Deserialize;
use serde::Serialize;

use super::super::super::failure;
use crate::model::typed::ResourceRequest;
use crate::model::typed::StoredPayload;
use crate::model::typed::StoredTaskRequest;
use crate::store::StoreError;

/// Immutable typed request fields stored separately from payload bytes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(super) struct StoredRequestInfo {
    /// Stable task kind identifier used to dispatch the request.
    pub(super) kind_id: String,
    /// Optional user-facing category associated with the task kind.
    pub(super) category: Option<String>,
    /// Registry identifier for the serialized payload type.
    pub(super) payload_type_id: String,
    /// Version of the payload schema expected by the decoder.
    pub(super) payload_schema_version: u32,
    /// Identifier for the codec that produced the stored payload bytes.
    pub(super) payload_codec_id: String,
    /// Request metadata persisted independently of the payload bytes.
    pub(super) metadata: Metadata,
    /// Resource limits requested for task execution.
    pub(super) resource_limit: ResourceRequest,
    /// Optional key used to correlate this task with a caller operation.
    pub(super) correlation_key: Option<String>,
    /// Optional key used to make request acceptance idempotent.
    pub(super) idempotency_key: Option<String>,
}

impl From<&StoredTaskRequest> for StoredRequestInfo {
    /// Copies immutable request attributes while leaving payload bytes in their
    /// own column.
    fn from(request: &StoredTaskRequest) -> Self {
        Self {
            kind_id: request.kind_id.clone(),
            category: request.category.clone(),
            payload_type_id: request.payload.type_id.as_str().to_owned(),
            payload_schema_version: request.payload.schema_version,
            payload_codec_id: request.payload.codec_id.clone(),
            metadata: request.metadata.clone(),
            resource_limit: request.resource_limit.clone(),
            correlation_key: request.correlation_key.clone(),
            idempotency_key: request.idempotency_key.clone(),
        }
    }
}

impl StoredRequestInfo {
    /// Reconstructs a typed request by pairing these attributes with stored
    /// payload bytes.
    pub(super) fn into_request(self, bytes: Vec<u8>) -> Result<StoredTaskRequest, StoreError> {
        let type_id = self.payload_type_id.as_str().try_into().map_err(failure)?;
        Ok(StoredTaskRequest {
            kind_id: self.kind_id,
            category: self.category,
            payload: StoredPayload {
                type_id,
                schema_version: self.payload_schema_version,
                codec_id: self.payload_codec_id,
                bytes,
            },
            metadata: self.metadata,
            resource_limit: self.resource_limit,
            correlation_key: self.correlation_key,
            idempotency_key: self.idempotency_key,
        })
    }
}
