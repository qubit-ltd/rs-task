use super::TaskCursor;
use crate::model::TaskStateKind;

/// Maximum number of typed task summaries returned by one history query.
pub const MAX_TASK_QUERY_LIMIT: usize = 256;

/// Bounded filters for typed task history.
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
    /// Validates and normalizes the requested page size.
    pub(crate) fn checked_page_size(&self) -> Result<usize, crate::store::StoreError> {
        if self.limit > MAX_TASK_QUERY_LIMIT {
            return Err(crate::store::StoreError::InvalidRequest(
                "task history page limit exceeds 256",
            ));
        }
        Ok(self.limit.max(1))
    }
}
