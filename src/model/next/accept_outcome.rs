use super::TaskSummary;

/// Result of accepting a new encoded request or finding an idempotent match.
#[derive(Debug, Clone, PartialEq)]
pub struct AcceptOutcome {
    /// Summary of the retained task.
    pub summary: TaskSummary,
    /// Whether this call created a new task record.
    pub created: bool,
}
