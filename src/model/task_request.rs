// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::collections::BTreeMap;

use serde::Deserialize;
use serde::Serialize;

use super::ResourceRequest;

/// Maximum number of bytes accepted in a reconstructable task payload.
pub const MAX_TASK_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;

/// Maximum number of bytes retained for a task output summary.
pub const MAX_TASK_OUTPUT_SUMMARY_BYTES: usize = 64 * 1024;

/// Reconstructible description accepted by a task handler.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskRequest {
    /// Stable task family understood by registered handlers.
    pub task_type: String,
    /// Exact handler version required to decode the payload.
    pub handler_version: String,
    /// Opaque bounded payload interpreted by the handler.
    pub payload: Vec<u8>,
    /// Resource budget required during execution.
    pub resources: ResourceRequest,
    /// Optional caller-defined value used to find related tasks.
    pub correlation_key: Option<String>,
    /// Optional key used to deduplicate identical submissions.
    pub idempotency_key: Option<String>,
    /// Small values attached to the task for filtering and diagnostics.
    pub metadata: BTreeMap<String, String>,
}

impl TaskRequest {
    /// Creates a versioned request with one CPU slot and no optional metadata.
    #[must_use]
    pub fn new(task_type: impl Into<String>, handler_version: impl Into<String>, payload: Vec<u8>) -> Self {
        Self {
            task_type: task_type.into(),
            handler_version: handler_version.into(),
            payload,
            resources: ResourceRequest {
                cpu_slots: 1,
                ..ResourceRequest::default()
            },
            correlation_key: None,
            idempotency_key: None,
            metadata: BTreeMap::new(),
        }
    }
}

/// Small result summary saved with the task record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct TaskOutput {
    /// Bounded opaque summary or external result reference.
    pub summary: Vec<u8>,
}

/// Classified failure reported by a handler.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskRunError {
    /// Stable error category suitable for business logic.
    pub category: String,
    /// Human-readable diagnostic summary.
    pub message: String,
    /// Whether the service may retry this attempt.
    pub retryable: bool,
}
