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
use crate::model::next::TaskId;

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

impl From<&crate::model::next::TaskSummary> for TaskEvent {
    fn from(summary: &crate::model::next::TaskSummary) -> Self {
        Self::from_typed_summary(summary)
    }
}

impl TaskEvent {
    /// Captures the identity, revision, state and correlation of a committed
    /// typed summary.
    #[must_use]
    pub fn from_typed_summary(summary: &crate::model::next::TaskSummary) -> Self {
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
    use crate::model::next::TaskId;

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
}
