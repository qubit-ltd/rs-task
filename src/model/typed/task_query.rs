// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use super::TaskCursor;
use crate::model::TaskStateKind;

/// Maximum number of typed task summaries returned by one history query.
pub const MAX_TASK_QUERY_LIMIT: usize = 256;

/// Filters typed task history and limits the number of returned summaries.
///
/// An empty `states` list includes every lifecycle state. A zero `limit` is
/// treated as one, and values above [`MAX_TASK_QUERY_LIMIT`] are rejected by
/// the store's query validation.
///
/// # Examples
///
/// ```
/// use qubit_task::model::TaskStateKind;
/// use qubit_task::TaskQuery;
///
/// let query = TaskQuery {
///     states: vec![TaskStateKind::Queued, TaskStateKind::Running],
///     limit: 20,
///     ..TaskQuery::default()
/// };
///
/// assert_eq!(query.states.len(), 2);
/// assert_eq!(query.limit, 20);
/// ```
#[derive(Debug, Clone, Default)]
pub struct TaskQuery {
    /// Lifecycle states to include; an empty list matches every state.
    pub states: Vec<TaskStateKind>,
    /// Optional exact business category.
    pub category: Option<String>,
    /// Optional exact caller-defined correlation key.
    pub correlation_key: Option<String>,
    /// Exclusive history position after which matching rows are returned.
    pub after: Option<TaskCursor>,
    /// Maximum number of summaries to return. Zero is normalized to one.
    pub limit: usize,
}

impl TaskQuery {
    /// Validates and normalizes the requested page size before querying.
    ///
    /// # Returns
    ///
    /// The requested limit, with zero normalized to one.
    ///
    /// # Errors
    ///
    /// Returns [`crate::store::StoreError::InvalidRequest`] when the requested
    /// limit exceeds [`MAX_TASK_QUERY_LIMIT`].
    pub(crate) fn checked_page_size(&self) -> Result<usize, crate::store::StoreError> {
        if self.limit > MAX_TASK_QUERY_LIMIT {
            return Err(crate::store::StoreError::InvalidRequest(
                "task history page limit exceeds 256",
            ));
        }
        Ok(self.limit.max(1))
    }
}
