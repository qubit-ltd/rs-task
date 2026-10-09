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

/// Versioned lifecycle snapshot emitted after a task state is persisted.
#[derive(Debug, Clone, Serialize, Deserialize)]
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

impl From<&crate::model::typed::TaskSummary> for TaskEvent {
    fn from(summary: &crate::model::typed::TaskSummary) -> Self {
        Self::from_typed_summary(summary)
    }
}

impl TaskEvent {
    /// Captures the identity, revision, state and correlation of a committed
    /// typed summary.
    #[must_use]
    pub fn from_typed_summary(summary: &crate::model::typed::TaskSummary) -> Self {
        Self {
            schema_version: 1,
            task_id: summary.id,
            state_version: summary.state_version,
            state: summary.state.clone(),
            correlation_key: summary.correlation_key.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::event::TaskEvent;
    use crate::model::TaskState;
    use crate::model::typed::TaskId;

    #[test]
    fn typed_task_event_round_trips_with_schema_version() {
        let event = TaskEvent {
            schema_version: 1,
            task_id: TaskId::from_id(qubit_id::Id::new(42)),
            state_version: 3,
            state: TaskState::Succeeded,
            correlation_key: Some("batch-a".into()),
        };
        let encoded = serde_json::to_vec(&event).unwrap();
        let decoded: TaskEvent = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(decoded.schema_version, 1);
        assert_eq!(decoded.task_id, event.task_id);
        assert_eq!(decoded.state_version, 3);
        assert_eq!(decoded.correlation_key.as_deref(), Some("batch-a"));
    }

    #[test]
    fn typed_task_event_captures_committed_summary_fields() {
        let summary = crate::model::typed::TaskSummary {
            id: TaskId::from_id(qubit_id::Id::new(43)),
            kind_id: "invoice.generate".into(),
            category: Some("billing".into()),
            payload_type_id: "example.Invoice".into(),
            payload_schema_version: 2,
            payload_codec_id: "example.json".into(),
            metadata: qubit_metadata::Metadata::default(),
            resource_limit: crate::model::typed::ResourceRequest::default(),
            correlation_key: Some("trace-2".into()),
            idempotency_key: None,
            state: TaskState::Queued,
            cancel_requested: false,
            cancel_error: None,
            state_version: 5,
            attempt: 1,
            retry_not_before_ms: None,
            accepted_at_ms: 10,
            started_at_ms: None,
            finished_at_ms: None,
            progress: None,
            output: None,
        };

        let event = TaskEvent::from(&summary);

        assert_eq!(event.schema_version, 1);
        assert_eq!(event.task_id, summary.id);
        assert_eq!(event.state_version, 5);
        assert_eq!(event.state, TaskState::Queued);
        assert_eq!(event.correlation_key.as_deref(), Some("trace-2"));
    }
}
