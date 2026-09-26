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
/// Maximum byte lengths for stable task request identifiers.
pub const MAX_TASK_TYPE_BYTES: usize = 128;
/// Maximum byte lengths for handler versions.
pub const MAX_HANDLER_VERSION_BYTES: usize = 64;
/// Maximum byte lengths for caller correlation keys.
pub const MAX_CORRELATION_KEY_BYTES: usize = 256;
/// Maximum byte lengths for idempotency keys.
pub const MAX_IDEMPOTENCY_KEY_BYTES: usize = 256;
/// Maximum number of metadata entries per task.
pub const MAX_TASK_METADATA_ENTRIES: usize = 32;
/// Maximum byte length of each metadata key.
pub const MAX_TASK_METADATA_KEY_BYTES: usize = 128;
/// Maximum byte length of each metadata value.
pub const MAX_TASK_METADATA_VALUE_BYTES: usize = 4 * 1024;
/// Maximum combined UTF-8 bytes used by task metadata.
pub const MAX_TASK_METADATA_BYTES: usize = 16 * 1024;
/// Maximum byte length of a persisted diagnostic category.
pub const MAX_TASK_DIAGNOSTIC_CATEGORY_BYTES: usize = 128;
/// Maximum byte length of a persisted diagnostic message.
pub const MAX_TASK_DIAGNOSTIC_MESSAGE_BYTES: usize = 4 * 1024;

/// Reconstructible description accepted by a task handler.
///
/// The payload is interpreted by the exact `(task_type, handler_version)`
/// handler registered at service startup. Text limits are measured in UTF-8
/// bytes, and the payload is limited to [`MAX_TASK_PAYLOAD_BYTES`].
///
/// # Examples
///
/// ```
/// use qubit_task::model::TaskRequest;
///
/// let request = TaskRequest::new("image.resize", "2", b"input-key".to_vec());
/// assert_eq!(request.resources.cpu_slots, 1);
/// assert!(request.correlation_key.is_none());
/// ```
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

/// Payload-free immutable fields used in task history and lifecycle reads.
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

impl TaskRequest {
    /// Checks the size limits used by both the service and task stores.
    ///
    /// # Returns
    ///
    /// `Ok(())` when all fields fit their documented byte and entry limits;
    /// otherwise returns a static diagnostic suitable for validation errors.
    pub(crate) fn validate_limits(&self) -> Result<(), &'static str> {
        if self.task_type.is_empty() || self.handler_version.is_empty() {
            return Err("task type and handler version must not be empty");
        }
        if self.task_type.len() > MAX_TASK_TYPE_BYTES {
            return Err("task type exceeds the 128-byte limit");
        }
        if self.handler_version.len() > MAX_HANDLER_VERSION_BYTES {
            return Err("handler version exceeds the 64-byte limit");
        }
        if self.payload.len() > MAX_TASK_PAYLOAD_BYTES {
            return Err("payload exceeds the 16 MiB limit");
        }
        if self
            .correlation_key
            .as_ref()
            .is_some_and(|value| value.len() > MAX_CORRELATION_KEY_BYTES)
        {
            return Err("correlation key exceeds the 256-byte limit");
        }
        if self
            .idempotency_key
            .as_ref()
            .is_some_and(|value| value.len() > MAX_IDEMPOTENCY_KEY_BYTES)
        {
            return Err("idempotency key exceeds the 256-byte limit");
        }
        if self.metadata.len() > MAX_TASK_METADATA_ENTRIES {
            return Err("metadata exceeds the 32-entry limit");
        }
        let mut metadata_bytes = 0_usize;
        for (key, value) in &self.metadata {
            if key.len() > MAX_TASK_METADATA_KEY_BYTES {
                return Err("metadata key exceeds the 128-byte limit");
            }
            if value.len() > MAX_TASK_METADATA_VALUE_BYTES {
                return Err("metadata value exceeds the 4096-byte limit");
            }
            metadata_bytes = metadata_bytes.saturating_add(key.len()).saturating_add(value.len());
        }
        if metadata_bytes > MAX_TASK_METADATA_BYTES {
            return Err("metadata exceeds the 16384-byte limit");
        }
        Ok(())
    }

    /// Creates a versioned request with one CPU slot and no optional metadata.
    ///
    /// # Parameters
    ///
    /// * `task_type` - Stable handler family name.
    /// * `handler_version` - Exact payload interpretation version.
    /// * `payload` - Opaque bytes passed to the selected handler.
    ///
    /// # Returns
    ///
    /// A request with one CPU slot and empty correlation, idempotency, and
    /// metadata fields.
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

    /// Sets the caller-generated key used to recover the same accepted task.
    ///
    /// Create and durably retain this key before calling
    /// [`TaskExecutionService::submit`](crate::service::TaskExecutionService::submit).
    /// Reuse it only with the identical request.
    #[must_use]
    pub fn with_idempotency_key(mut self, key: impl Into<String>) -> Self {
        self.idempotency_key = Some(key.into());
        self
    }
}

/// Small result summary saved with the task record.
///
/// # Examples
///
/// ```
/// use qubit_task::model::TaskOutput;
///
/// let output = TaskOutput { summary: b"stored result key".to_vec() };
/// assert_eq!(output.summary, b"stored result key");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct TaskOutput {
    /// Bounded opaque summary or external result reference.
    pub summary: Vec<u8>,
}

/// Classified failure reported by a handler.
///
/// Set `retryable` only when repeating the operation is safe under the
/// application's idempotency and side-effect rules. Persisted category and
/// message strings are byte-bounded by the service.
///
/// # Examples
///
/// ```
/// use qubit_task::model::TaskRunError;
///
/// let error = TaskRunError {
///     category: "remote_unavailable".into(),
///     message: "try again later".into(),
///     retryable: true,
/// };
/// assert!(error.retryable);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskRunError {
    /// Stable error category suitable for business logic.
    pub category: String,
    /// Human-readable diagnostic summary.
    pub message: String,
    /// Whether the service may retry this attempt.
    pub retryable: bool,
}
