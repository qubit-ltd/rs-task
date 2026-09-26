// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Event payloads published directly through `qubit-event-bus`.

use serde::Deserialize;
use serde::Serialize;

use crate::model::TaskId;
use crate::model::TaskState;
use crate::model::TaskSummary;

/// Immutable status-change event suitable for duplicate-aware consumers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskEvent {
    /// Stable task identifier.
    pub task_id: TaskId,
    /// Monotonic status revision; consumers may discard older revisions.
    pub state_version: u64,
    /// Current lifecycle state.
    pub state: TaskState,
    /// Business correlation key, when present.
    pub correlation_key: Option<String>,
}

impl From<&TaskSummary> for TaskEvent {
    fn from(record: &TaskSummary) -> Self {
        Self {
            task_id: record.id,
            state_version: record.state_version,
            state: record.state.clone(),
            correlation_key: record.request.correlation_key.clone(),
        }
    }
}
