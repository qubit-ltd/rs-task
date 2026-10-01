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

/// Immutable status-change event suitable for duplicate-aware consumers.
///
/// # Examples
///
/// ```
/// use qubit_task::event::TaskEvent;
/// use qubit_task::model::TaskId;
/// use qubit_task::model::TaskState;
///
/// let event = TaskEvent {
///     task_id: "42".into(),
///     state_version: 1,
///     state: TaskState::Running,
///     correlation_key: None,
/// };
/// assert_eq!(event.state_version, 1);
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskEvent {
    /// Stable task identifier.
    pub task_id: String,
    /// Monotonic status revision; consumers may discard older revisions.
    pub state_version: u64,
    /// Current lifecycle state.
    pub state: TaskState,
    /// Business correlation key, when present.
    pub correlation_key: Option<String>,
}

#[cfg(test)]
impl From<&crate::model::TaskSummary> for TaskEvent {
    /// Copies the task identity, revision, state, and correlation key.
    ///
    /// # Parameters
    ///
    /// * `record` - Task summary to represent as an event.
    ///
    /// # Returns
    ///
    /// An immutable event snapshot without request payload data.
    fn from(record: &crate::model::TaskSummary) -> Self {
        Self {
            task_id: record.id.to_string(),
            state_version: record.state_version,
            state: record.state.clone(),
            correlation_key: record.request.correlation_key.clone(),
        }
    }
}

#[cfg(not(test))]
impl From<&crate::model::TaskSummary> for TaskEvent {
    /// Copies the typed task identity, revision, state, and correlation key.
    fn from(record: &crate::model::TaskSummary) -> Self {
        Self {
            task_id: record.id.to_string(),
            state_version: record.state_version,
            state: record.state.clone(),
            correlation_key: record.correlation_key.clone(),
        }
    }
}
