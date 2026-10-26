// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use qubit_metadata::Metadata;

use super::ResourceRequest;
use super::StoredPayload;

/// Request data stored by a task store without retaining the handler's concrete
/// request type.
///
/// The routing identity and payload preserve enough information for a store to
/// identify and later decode the request. The remaining fields carry query
/// classification, scheduling, metadata, and caller-provided keys alongside
/// that payload.
///
/// # Examples
///
/// A task store receives this value after a typed
/// [`TaskRequest`](crate::model::typed::TaskRequest) has been encoded with
/// [`TaskRequest::encode`](crate::model::typed::TaskRequest::encode).
/// Applications can also inspect or construct the public stored
/// representation directly:
///
/// ```
/// use qubit_metadata::Metadata;
/// use qubit_model_id::ModelIdBuf;
/// use qubit_task::ResourceRequest;
/// use qubit_task::StoredPayload;
/// use qubit_task::StoredTaskRequest;
///
/// let request = StoredTaskRequest {
///     kind_id: String::from("example.kind"),
///     category: None,
///     payload: StoredPayload {
///         type_id: ModelIdBuf::parse("example.Input").expect("model ID is valid"),
///         schema_version: 1,
///         codec_id: String::from("json"),
///         bytes: vec![1, 2, 3],
///     },
///     metadata: Metadata::new(),
///     resource_limit: ResourceRequest::default(),
///     correlation_key: None,
///     idempotency_key: None,
/// };
///
/// assert_eq!(request.kind_id, "example.kind");
/// assert_eq!(request.payload.bytes, [1, 2, 3]);
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct StoredTaskRequest {
    /// Stable identity of the handler that can process this request.
    pub kind_id: String,
    /// Optional user-visible category used to classify the query.
    pub category: Option<String>,
    /// Encoded request bytes together with the schema and codec identities
    /// needed to interpret them.
    pub payload: StoredPayload,
    /// Structured metadata associated with the request independently of its
    /// encoded payload.
    pub metadata: Metadata,
    /// Resource quota requested for scheduling this task.
    pub resource_limit: ResourceRequest,
    /// Optional caller-defined key for correlating this request with related
    /// work.
    pub correlation_key: Option<String>,
    /// Optional key that identifies retries of the same operation for
    /// idempotent handling.
    pub idempotency_key: Option<String>,
}
