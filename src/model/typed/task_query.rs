// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use thiserror::Error;

use super::TaskCursor;
use crate::model::TaskStateKind;

/// Maximum number of task summaries returned by one history query.
pub const MAX_TASK_QUERY_LIMIT: usize = 256;

/// Error returned when a task query requests an unsupported page size.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum TaskQueryError {
    /// The page size is outside the inclusive range 1 through 256.
    #[error("task query page size {requested} is outside 1..={MAX_TASK_QUERY_LIMIT}")]
    InvalidPageSize { requested: usize },
}

/// Filters task history and limits the number of returned summaries.
///
/// An empty state list includes every lifecycle state. The default page size
/// is 50.
#[derive(Debug, Clone)]
pub struct TaskQuery {
    states: Vec<TaskStateKind>,
    category: Option<String>,
    correlation_key: Option<String>,
    after: Option<TaskCursor>,
    limit: usize,
}

impl Default for TaskQuery {
    fn default() -> Self {
        Self {
            states: Vec::new(),
            category: None,
            correlation_key: None,
            after: None,
            limit: 50,
        }
    }
}

impl TaskQuery {
    /// Creates a query matching one lifecycle state.
    #[must_use]
    pub fn with_state(state: TaskStateKind) -> Self {
        Self::default().with_states([state])
    }

    /// Restricts the query to the supplied lifecycle states.
    #[must_use]
    pub fn with_states(mut self, states: impl IntoIterator<Item = TaskStateKind>) -> Self {
        self.states = states.into_iter().collect();
        self
    }

    /// Restricts the query to one exact business category.
    #[must_use]
    pub fn with_category(mut self, category: impl Into<String>) -> Self {
        self.category = Some(category.into());
        self
    }

    /// Restricts the query to one exact caller-defined correlation key.
    #[must_use]
    pub fn with_correlation_key(mut self, key: impl Into<String>) -> Self {
        self.correlation_key = Some(key.into());
        self
    }

    /// Continues the query exclusively after a previously observed cursor.
    #[must_use]
    pub fn after(mut self, cursor: TaskCursor) -> Self {
        self.after = Some(cursor);
        self
    }

    /// Sets the page size, which must be between 1 and 256 inclusive.
    pub fn page_size(mut self, requested: usize) -> Result<Self, TaskQueryError> {
        if !(1..=MAX_TASK_QUERY_LIMIT).contains(&requested) {
            return Err(TaskQueryError::InvalidPageSize { requested });
        }
        self.limit = requested;
        Ok(self)
    }

    /// Lifecycle states included in this query; empty means all states.
    pub fn states(&self) -> &[TaskStateKind] {
        &self.states
    }
    /// Exact category filter, if present.
    pub fn category(&self) -> Option<&str> {
        self.category.as_deref()
    }
    /// Exact correlation-key filter, if present.
    pub fn correlation_key(&self) -> Option<&str> {
        self.correlation_key.as_deref()
    }
    /// Exclusive history cursor, if present.
    pub fn cursor(&self) -> Option<TaskCursor> {
        self.after
    }
    /// Validated page size.
    pub fn limit(&self) -> usize {
        self.limit
    }
}

#[cfg(test)]
mod tests {
    use super::TaskQuery;
    use super::TaskQueryError;

    #[test]
    fn page_size_error_preserves_requested_value() {
        assert!(matches!(
            TaskQuery::default().page_size(0),
            Err(TaskQueryError::InvalidPageSize { requested: 0 })
        ));
        assert!(matches!(
            TaskQuery::default().page_size(257),
            Err(TaskQueryError::InvalidPageSize { requested: 257 })
        ));
    }
}
