// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use thiserror::Error;

use super::MAX_TASK_QUERY_LIMIT;

/// Describes why a task history query cannot be executed.
///
/// A rejected page size is reported before any store is queried.
///
/// # Examples
///
/// ```
/// use qubit_task::TaskQuery;
/// use qubit_task::TaskQueryError;
///
/// let error = TaskQuery::default().page_size(0).unwrap_err();
/// assert!(matches!(error, TaskQueryError::InvalidPageSize { requested: 0 }));
/// ```
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum TaskQueryError {
    /// The requested page size is below one or above the supported maximum.
    #[error("task query page size {requested} is outside 1..={MAX_TASK_QUERY_LIMIT}")]
    InvalidPageSize {
        /// Number of matching summaries per page requested by the caller.
        requested: usize,
    },
}
