use super::TaskId;

/// Compare-and-set command that starts a queued typed task attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StartCommand {
    /// Task to start.
    pub id: TaskId,
    /// Expected lifecycle version.
    pub expected_state_version: u64,
    /// Unix epoch milliseconds when execution starts.
    pub started_at_ms: u64,
}
