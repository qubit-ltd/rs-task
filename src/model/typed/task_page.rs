// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use super::TaskCursor;
use super::TaskSummary;

/// One bounded page of typed task history.
///
/// Rows are ordered by `accepted_at_ms` ascending, then by the numeric value
/// of `id` ascending. `next` is the exclusive key of the last returned row
/// when a lookahead row exists. Each query observes its own storage snapshot;
/// inserts after the cursor may appear on a later page, while inserts at or
/// before it cannot. Changes to a row's state or category between page calls
/// may affect whether that row matches the next query.
///
/// # Examples
///
/// ```
/// use qubit_task::model::typed::TaskPage;
///
/// let page = TaskPage::default();
/// assert!(page.records.is_empty());
/// assert!(page.next.is_none());
/// ```
#[derive(Debug, Clone, Default)]
pub struct TaskPage {
    /// Task summaries returned for this page, in history order.
    pub records: Vec<TaskSummary>,
    /// Exclusive cursor after the last returned record when a lookahead row
    /// confirms that another page may exist; `None` means no continuation was
    /// found.
    pub next: Option<TaskCursor>,
}
