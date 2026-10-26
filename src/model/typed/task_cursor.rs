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
///
/// # Examples
///
/// ```
/// use qubit_task::model::typed::TaskCursor;
/// use qubit_task::model::typed::TaskId;
///
/// let cursor = TaskCursor::new(1_700_000_000_000, TaskId::from_id(qubit_id::Id::new(42)));
/// assert_eq!(cursor.accepted_at_ms, 1_700_000_000_000);
/// ```
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TaskCursor {
    /// Acceptance timestamp in Unix epoch milliseconds.
    pub accepted_at_ms: u64,
    /// Numeric task identity used to break timestamp ties.
    pub id: TaskId,
}

impl TaskCursor {
    /// Creates a cursor for the supplied history position.
    ///
    /// # Parameters
    ///
    /// - `accepted_at_ms`: acceptance timestamp in Unix epoch milliseconds.
    /// - `id`: task identity used to break ties between equal timestamps.
    ///
    /// # Returns
    ///
    /// A cursor identifying the exclusive history position.
    #[inline]
    pub const fn new(accepted_at_ms: u64, id: TaskId) -> Self {
        Self { accepted_at_ms, id }
    }
}

impl From<&TaskSummary> for TaskCursor {
    /// Copies the acceptance position from a task summary into a cursor.
    ///
    /// # Returns
    ///
    /// A cursor containing the summary's acceptance timestamp and task ID.
    #[inline]
    fn from(summary: &TaskSummary) -> Self {
        Self::new(summary.accepted_at_ms, summary.id)
    }
}
