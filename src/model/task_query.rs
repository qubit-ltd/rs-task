// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use super::TaskStateKind;
use super::legacy::TaskCursor;
use crate::store::StoreError;

/// Maximum number of records returned by one task history query.
pub const MAX_TASK_QUERY_LIMIT: usize = 256;

/// Validates a history page size and normalizes zero to one.
///
/// # Parameters
///
/// * `limit` - Requested maximum number of records.
///
/// # Returns
///
/// A limit in the inclusive range `1..=MAX_TASK_QUERY_LIMIT`, or
/// [`StoreError::InvalidRequest`] when the request exceeds the maximum.
///
/// # Errors
///
/// Returns [`StoreError::InvalidRequest`] when `limit` exceeds the maximum.
pub(crate) fn checked_page_size(limit: usize) -> Result<usize, StoreError> {
    if limit > MAX_TASK_QUERY_LIMIT {
        return Err(StoreError::InvalidRequest(
            "task history page limit exceeds 256",
        ));
    }
    Ok(limit.max(1))
}

/// Filters and bounds a task history query.
///
/// Empty `states` matches every state. A zero `limit` is treated as one
/// record, and `after` is an exclusive `(accepted_at_ms, id)` cursor.
///
/// # Examples
///
/// ```
/// use qubit_task::model::TaskQuery;
/// use qubit_task::model::TaskStateKind;
///
/// let query = TaskQuery { states: vec![TaskStateKind::Queued], limit: 20, ..TaskQuery::default() };
/// assert_eq!(query.limit, 20);
/// ```
#[derive(Debug, Clone, Default)]
pub struct TaskQuery {
    /// Optional set of lifecycle states to include.
    pub states: Vec<TaskStateKind>,
    /// Maximum number of records to return.
    pub limit: usize,
    /// Exclusive cursor after which history records are returned.
    pub after: Option<TaskCursor>,
    /// Optional exact business correlation key.
    pub correlation_key: Option<String>,
}
