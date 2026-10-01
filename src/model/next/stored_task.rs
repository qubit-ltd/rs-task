use super::StoredTaskRequest;
use super::TaskSummary;

/// Encoded task request and payload-free lifecycle view loaded from a store.
#[derive(Debug, Clone, PartialEq)]
pub struct StoredTask {
    /// Encoded request required to recover handler execution.
    pub request: StoredTaskRequest,
    /// Current queryable lifecycle summary.
    pub summary: TaskSummary,
}
