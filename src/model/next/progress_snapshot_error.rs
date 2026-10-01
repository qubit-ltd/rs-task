use super::task_progress_snapshot::MAX_TASK_PROGRESS_METRICS;
use super::task_progress_snapshot::MAX_TASK_PROGRESS_SNAPSHOT_BYTES;

/// Snapshot validation failures returned before a store transaction begins.
#[derive(Debug, thiserror::Error)]
pub enum ProgressSnapshotError {
    /// The supplied metric vector exceeds the documented count limit.
    #[error("task progress has {0} metrics; the maximum is {MAX_TASK_PROGRESS_METRICS}")]
    TooManyMetrics(usize),
    /// Stage identifier exceeds the task snapshot protocol limit.
    #[error("task progress stage ID has {0} bytes; the maximum is 128")]
    StageIdTooLarge(usize),
    /// Stage name exceeds the task snapshot protocol limit.
    #[error("task progress stage name has {0} bytes; the maximum is 256")]
    StageNameTooLarge(usize),
    /// JSON serialization of the snapshot failed.
    #[error("failed to encode progress snapshot: {0}")]
    Serialize(#[source] serde_json::Error),
    /// The encoded snapshot exceeds the documented byte limit.
    #[error("task progress snapshot has {0} bytes; the maximum is {MAX_TASK_PROGRESS_SNAPSHOT_BYTES}")]
    TooLarge(usize),
}
