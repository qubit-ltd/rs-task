// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use super::TaskCursor;
use super::TaskQueryError;
use crate::model::TaskStateKind;

/// Maximum number of task summaries returned by one history query.
pub const MAX_TASK_QUERY_LIMIT: usize = 256;

/// Filters task history and limits the number of returned summaries.
///
/// An empty state list includes every lifecycle state. The default page size
/// is 50.
///
/// # Examples
///
/// ```
/// use qubit_task::TaskQuery;
/// use qubit_task::TaskStateKind;
///
/// let query = TaskQuery::with_state(TaskStateKind::Succeeded)
///     .with_category("billing")
///     .page_size(25)
///     .expect("page size is supported");
/// assert_eq!(query.limit(), 25);
/// assert_eq!(query.category(), Some("billing"));
/// ```
#[derive(Debug, Clone)]
pub struct TaskQuery {
    /// Lifecycle states to include; an empty vector selects every state.
    states: Vec<TaskStateKind>,
    /// Exact user-visible business category filter, when configured.
    category: Option<String>,
    /// Exact caller-defined correlation key filter, when configured.
    correlation_key: Option<String>,
    /// Exclusive continuation point for paginated history.
    after: Option<TaskCursor>,
    /// Maximum number of matching summaries requested for one page.
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
    ///
    /// # Parameters
    ///
    /// * `state` is the lifecycle state to include.
    ///
    /// # Returns
    ///
    /// A query restricted to `state`.
    #[must_use]
    pub fn with_state(state: TaskStateKind) -> Self {
        Self::default().with_states([state])
    }

    /// Restricts the query to the supplied lifecycle states.
    ///
    /// # Parameters
    ///
    /// * `states` supplies the lifecycle states to include.
    ///
    /// # Returns
    ///
    /// The updated query. An empty iterator means all lifecycle states.
    #[must_use]
    pub fn with_states(mut self, states: impl IntoIterator<Item = TaskStateKind>) -> Self {
        self.states = states.into_iter().collect();
        self
    }

    /// Restricts the query to one exact business category.
    ///
    /// # Parameters
    ///
    /// * `category` is the exact category value to match.
    ///
    /// # Returns
    ///
    /// The updated query.
    #[must_use]
    pub fn with_category(mut self, category: impl Into<String>) -> Self {
        self.category = Some(category.into());
        self
    }

    /// Restricts the query to one exact caller-defined correlation key.
    ///
    /// # Parameters
    ///
    /// * `key` is the exact correlation key to match.
    ///
    /// # Returns
    ///
    /// The updated query.
    #[must_use]
    pub fn with_correlation_key(mut self, key: impl Into<String>) -> Self {
        self.correlation_key = Some(key.into());
        self
    }

    /// Continues the query exclusively after a previously observed cursor.
    ///
    /// # Parameters
    ///
    /// * `cursor` marks the last observed history item; it is excluded from
    ///   the next page.
    ///
    /// # Returns
    ///
    /// The updated query.
    #[must_use]
    pub fn after(mut self, cursor: TaskCursor) -> Self {
        self.after = Some(cursor);
        self
    }

    /// Sets the page size, which must be between 1 and 256 inclusive.
    ///
    /// # Parameters
    ///
    /// * `requested` is the maximum number of matching summaries to return.
    ///
    /// # Returns
    ///
    /// The updated query when the size is valid.
    ///
    /// # Errors
    ///
    /// Returns [`TaskQueryError::InvalidPageSize`](crate::model::typed::TaskQueryError::InvalidPageSize)
    /// when the value is outside the supported inclusive range.
    pub fn page_size(mut self, requested: usize) -> Result<Self, TaskQueryError> {
        if !(1..=MAX_TASK_QUERY_LIMIT).contains(&requested) {
            return Err(TaskQueryError::InvalidPageSize { requested });
        }
        self.limit = requested;
        Ok(self)
    }

    /// Returns lifecycle states included in this query; empty means all states.
    ///
    /// # Returns
    ///
    /// The selected states, or an empty slice when every lifecycle state is
    /// included.
    #[must_use]
    pub fn states(&self) -> &[TaskStateKind] {
        &self.states
    }
    /// Returns the exact business category filter, if present.
    ///
    /// # Returns
    ///
    /// `Some` contains the configured category; `None` includes every
    /// category.
    #[must_use]
    pub fn category(&self) -> Option<&str> {
        self.category.as_deref()
    }
    /// Returns the exact correlation-key filter, if present.
    ///
    /// # Returns
    ///
    /// `Some` contains the configured key; `None` includes every key.
    #[must_use]
    pub fn correlation_key(&self) -> Option<&str> {
        self.correlation_key.as_deref()
    }
    /// Returns the exclusive history cursor, if present.
    ///
    /// # Returns
    ///
    /// `Some` is the cursor after which results continue; `None` starts at the
    /// beginning of the matching history.
    #[must_use]
    pub fn cursor(&self) -> Option<TaskCursor> {
        self.after
    }
    /// Returns the validated page size.
    ///
    /// # Returns
    ///
    /// A value in the inclusive range 1 through [`MAX_TASK_QUERY_LIMIT`].
    #[must_use]
    pub fn limit(&self) -> usize {
        self.limit
    }
}

#[cfg(test)]
mod tests {
    use super::TaskQuery;
    use super::TaskQueryError;

    #[test]
    fn test_page_size_error_preserves_requested_value() {
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
