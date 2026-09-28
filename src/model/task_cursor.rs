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
use super::TaskRecord;
use super::TaskSummary;

/// Stable cursor into task history ordered by acceptance time and task ID.
///
/// Both fields are required because multiple tasks can be accepted during the
/// same millisecond. The ID provides a deterministic tie-breaker.
///
/// # Examples
///
/// ```
/// use qubit_task::model::TaskCursor;
/// use qubit_task::model::TaskId;
///
/// let cursor = TaskCursor::new(42, TaskId::generate());
/// assert_eq!(cursor.accepted_at_ms, 42);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TaskCursor {
    /// Acceptance timestamp in Unix epoch milliseconds.
    pub accepted_at_ms: u64,
    /// Task ID used to order tasks with the same acceptance timestamp.
    pub id: TaskId,
}

impl TaskCursor {
    /// Creates a cursor for the supplied history position.
    ///
    /// # Parameters
    ///
    /// * `accepted_at_ms` - Acceptance timestamp stored in the task record.
    /// * `id` - Task ID that breaks ties at the same timestamp.
    ///
    /// # Returns
    ///
    /// A cursor suitable for `TaskQuery::after`.
    #[must_use]
    pub fn new(accepted_at_ms: u64, id: TaskId) -> Self {
        Self { accepted_at_ms, id }
    }
}

impl From<&TaskRecord> for TaskCursor {
    /// Creates a cursor at the supplied record's history position.
    ///
    /// # Parameters
    ///
    /// * `record` - Record whose acceptance position is used.
    ///
    /// # Returns
    ///
    /// A cursor with the record's acceptance timestamp and ID.
    fn from(record: &TaskRecord) -> Self {
        Self::new(record.accepted_at_ms, record.id)
    }
}

impl From<&TaskSummary> for TaskCursor {
    /// Creates a cursor at the supplied summary's history position.
    ///
    /// # Parameters
    ///
    /// * `record` - Summary whose acceptance position is used.
    ///
    /// # Returns
    ///
    /// A cursor with the summary's acceptance timestamp and ID.
    fn from(record: &TaskSummary) -> Self {
        Self::new(record.accepted_at_ms, record.id)
    }
}
