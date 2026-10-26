// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use serde::Deserialize;
use serde::Serialize;

use crate::model::TaskState;
use crate::model::typed::TaskId;
use crate::model::typed::TaskSummary;

/// Versioned lifecycle snapshot emitted after a task state is persisted.
///
/// # Examples
///
/// ```
/// use qubit_metadata::Metadata;
/// use qubit_task::event::TaskEvent;
/// use qubit_task::model::TaskState;
/// use qubit_task::model::typed::ResourceRequest;
/// use qubit_task::model::typed::TaskId;
/// use qubit_task::model::typed::TaskSummary;
///
/// let summary = TaskSummary {
///     id: TaskId::from_id(qubit_id::Id::new(42)),
///     kind_id: "invoice.generate".into(),
///     category: None,
///     payload_type_id: "example.Invoice".into(),
///     payload_schema_version: 1,
///     payload_codec_id: "example.json".into(),
///     metadata: Metadata::default(),
///     resource_limit: ResourceRequest::default(),
///     correlation_key: Some("batch-a".into()),
///     idempotency_key: None,
///     state: TaskState::Succeeded,
///     cancel_requested: false,
///     cancel_error: None,
///     state_version: 3,
///     attempt: 1,
///     retry_not_before_ms: None,
///     accepted_at_ms: 0,
///     started_at_ms: None,
///     finished_at_ms: Some(1),
///     progress: None,
///     output: None,
/// };
/// let event = TaskEvent::from_typed_summary(&summary);
/// assert_eq!(event.state_version, 3);
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
#[must_use = "the task event should be published or otherwise handled"]
pub struct TaskEvent {
    /// Event schema version.
    pub schema_version: u32,
    /// Stable typed task identifier.
    pub task_id: TaskId,
    /// Monotonic lifecycle revision for duplicate and stale event handling.
    pub state_version: u64,
    /// Persisted lifecycle state.
    pub state: TaskState,
    /// Business correlation key, when present.
    pub correlation_key: Option<String>,
}

impl From<&TaskSummary> for TaskEvent {
    /// Converts a persisted typed summary into its event snapshot.
    fn from(summary: &TaskSummary) -> Self {
        Self::from_typed_summary(summary)
    }
}

impl TaskEvent {
    /// Captures the identity, revision, state and correlation of a committed
    /// typed summary.
    ///
    /// # Parameters
    ///
    /// - `summary`: the persisted typed task summary to snapshot.
    ///
    /// # Returns
    ///
    /// A version 1 event containing the summary's identity, state revision,
    /// lifecycle state, and correlation key.
    pub fn from_typed_summary(summary: &TaskSummary) -> Self {
        Self {
            schema_version: 1,
            task_id: summary.id,
            state_version: summary.state_version,
            state: summary.state.clone(),
            correlation_key: summary.correlation_key.clone(),
        }
    }
}
