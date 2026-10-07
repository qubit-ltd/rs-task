// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use serde::Deserialize;
use serde::Serialize;

use super::TaskId;
use super::TaskSummary;

/// Exclusive position in typed task history.
///
/// History is ordered by acceptance time ascending and then by the numeric
/// value of [`TaskId`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TaskCursor {
    /// Acceptance timestamp in Unix epoch milliseconds.
    pub accepted_at_ms: u64,
    /// Numeric task identity used to break timestamp ties.
    pub id: TaskId,
}

impl TaskCursor {
    /// Creates a cursor for the supplied history position.
    #[must_use]
    pub const fn new(accepted_at_ms: u64, id: TaskId) -> Self {
        Self { accepted_at_ms, id }
    }
}

impl From<&TaskSummary> for TaskCursor {
    fn from(summary: &TaskSummary) -> Self {
        Self::new(summary.accepted_at_ms, summary.id)
    }
}

#[cfg(test)]
mod tests {
    use super::TaskCursor;
    use crate::model::TaskState;
    use crate::model::next::ResourceRequest;
    use crate::model::next::TaskId;
    use crate::model::next::TaskSummary;

    #[test]
    fn cursor_from_summary_preserves_acceptance_position() {
        let id = TaskId::from_id(qubit_id::Id::new(91));
        let summary = TaskSummary {
            id,
            kind_id: "counter.add".into(),
            category: None,
            payload_type_id: "qubit_task.tests.Counter".into(),
            payload_schema_version: 1,
            payload_codec_id: "qubit_task.tests.u32_le".into(),
            metadata: qubit_metadata::Metadata::default(),
            resource_limit: ResourceRequest::default(),
            correlation_key: None,
            idempotency_key: None,
            state: TaskState::Queued,
            cancel_requested: false,
            cancel_error: None,
            state_version: 0,
            attempt: 0,
            retry_not_before_ms: None,
            accepted_at_ms: 123,
            started_at_ms: None,
            finished_at_ms: None,
            progress: None,
            output: None,
        };

        let cursor = TaskCursor::from(&summary);

        assert_eq!(cursor.accepted_at_ms, summary.accepted_at_ms);
        assert_eq!(cursor.id, id);
    }
}
