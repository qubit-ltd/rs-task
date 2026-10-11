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

/// Request data stored by a task store without retaining the handler's concrete request type.
///
/// The routing identity and payload preserve enough information for a store to identify and
/// later decode the request. The remaining fields carry query classification, scheduling,
/// metadata, and caller-provided keys alongside that payload.
#[derive(Debug, Clone, PartialEq)]
pub struct StoredTaskRequest {
    /// Stable identity of the handler that can process this request.
    pub kind_id: String,
    /// Optional user-visible category used to classify the query.
    pub category: Option<String>,
    /// Encoded request bytes together with the schema and codec identities needed to interpret them.
    pub payload: StoredPayload,
    /// Structured metadata associated with the request independently of its encoded payload.
    pub metadata: Metadata,
    /// Resource quota requested for scheduling this task.
    pub resource_limit: ResourceRequest,
    /// Optional caller-defined key for correlating this request with related work.
    pub correlation_key: Option<String>,
    /// Optional key that identifies retries of the same operation for idempotent handling.
    pub idempotency_key: Option<String>,
}
