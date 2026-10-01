use serde::Deserialize;
use serde::Serialize;

use super::TaskId;
use super::TaskSummary;

/// Exclusive position in typed task history.
///
/// History is ordered by acceptance time ascending and then by the numeric
/// value of [`TaskId`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TaskCursor {
    /// Acceptance timestamp in Unix epoch milliseconds.
    pub accepted_at_ms: u64,
    /// Numeric task identity used to break timestamp ties.
    pub id: TaskId,
}

impl TaskCursor {
    /// Creates a cursor for the supplied history position.
    #[must_use]
    pub const fn new(accepted_at_ms: u64, id: TaskId) -> Self {
        Self { accepted_at_ms, id }
    }
}

impl From<&TaskSummary> for TaskCursor {
    fn from(summary: &TaskSummary) -> Self {
        Self::new(summary.accepted_at_ms, summary.id)
    }
}
