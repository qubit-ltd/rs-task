// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Serialized request fields needed to construct a task summary.

use serde::Deserialize;
use serde::Serialize;

use crate::model::typed::ResourceRequest;

/// Request metadata persisted separately from the encoded payload bytes.
#[derive(Deserialize, Serialize)]
pub(super) struct RequestInfo {
    /// Registered handler route for this task.
    pub(super) kind_id: String,
    /// Optional application-defined query category.
    pub(super) category: Option<String>,
    /// Stable model identity of the payload type.
    pub(super) payload_type_id: String,
    /// Schema version associated with the encoded payload.
    pub(super) payload_schema_version: u32,
    /// Identifier of the codec used for the payload bytes.
    pub(super) payload_codec_id: String,
    /// Application metadata attached to the request.
    pub(super) metadata: qubit_metadata::Metadata,
    /// Resource quota requested by this task.
    pub(super) resource_limit: ResourceRequest,
    /// Optional application correlation key.
    pub(super) correlation_key: Option<String>,
    /// Optional key used to deduplicate accepted requests.
    pub(super) idempotency_key: Option<String>,
}
