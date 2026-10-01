// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use super::legacy::TaskCursor;
use super::legacy::TaskSummary;

/// One bounded page of task history.
///
/// Records are ordered by `(accepted_at_ms, id)`. `next` is present only when
/// another page may exist; pass it as the next query's exclusive cursor.
///
/// # Examples
///
/// ```
/// use qubit_task::model::TaskPage;
///
/// let page = TaskPage::default();
/// assert!(page.records.is_empty());
/// ```
#[derive(Debug, Clone, Default)]
pub struct TaskPage {
    /// Records selected by the query.
    pub records: Vec<TaskSummary>,
    /// Cursor for the next page, when more data may exist.
    pub next: Option<TaskCursor>,
}
