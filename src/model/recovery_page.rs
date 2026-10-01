// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use serde::Deserialize;
use serde::Serialize;

use super::TaskCursor;
use super::TaskSummary;

/// Bounded payload-free projection of unfinished work used during recovery.
///
/// # Examples
///
/// ```
/// use qubit_task::model::RecoveryPage;
///
/// let page = RecoveryPage::default();
/// assert!(page.tasks.is_empty());
/// assert!(page.next.is_none());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[cfg_attr(test, allow(dead_code))]
pub struct RecoveryPage {
    /// At most 256 Queued/Running summaries, strictly ordered by
    /// `(accepted_at_ms, id)` after the requested exclusive lower bound.
    pub tasks: Vec<TaskSummary>,
    /// Cursor of the last returned summary when another page is available.
    ///
    /// Terminal pages use `None`, including full 256-row terminal pages.
    /// Empty pages never advertise a next cursor.
    pub next: Option<TaskCursor>,
}
