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

use super::legacy::ResourceRequest;
use super::legacy::TaskRequest;

/// Payload-free immutable fields used in task history and lifecycle reads.
///
/// The request payload is intentionally omitted so callers can inspect task
/// metadata without loading potentially large or sensitive input bytes.
///
/// # Examples
///
/// ```
/// use qubit_task::model::TaskRequest;
/// use qubit_task::model::TaskRequestInfo;
///
/// let request = TaskRequest::new("report", "1", b"input".to_vec());
/// let info = TaskRequestInfo::from(&request);
/// assert_eq!(info.task_type, "report");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskRequestInfo {
    /// Stable task family understood by registered handlers.
    pub task_type: String,
    /// Exact handler version required to decode the payload.
    pub handler_version: String,
    /// Resource budget required during execution.
    pub resources: ResourceRequest,
    /// Optional caller-defined value used to find related tasks.
    pub correlation_key: Option<String>,
    /// Optional key used to deduplicate identical submissions.
    pub idempotency_key: Option<String>,
    /// Small values attached to the task for filtering and diagnostics.
    pub metadata: BTreeMap<String, String>,
}

impl From<&TaskRequest> for TaskRequestInfo {
    /// Copies immutable request metadata without cloning the payload.
    ///
    /// # Parameters
    ///
    /// * `request` - Request whose payload-free metadata is copied.
    ///
    /// # Returns
    ///
    /// A metadata snapshot that excludes the request payload.
    fn from(request: &TaskRequest) -> Self {
        Self {
            task_type: request.task_type.clone(),
            handler_version: request.handler_version.clone(),
            resources: request.resources.clone(),
            correlation_key: request.correlation_key.clone(),
            idempotency_key: request.idempotency_key.clone(),
            metadata: request.metadata.clone(),
        }
    }
}
