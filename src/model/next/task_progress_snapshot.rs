use serde::Deserialize;
use serde::Serialize;

use super::ProgressCommand;
use super::ProgressMetricSnapshot;
use super::ProgressSnapshotError;
use super::ProgressStageSnapshot;

/// Maximum metrics retained in one task progress snapshot.
pub const MAX_TASK_PROGRESS_METRICS: usize = 32;
/// Maximum encoded JSON bytes retained for one task progress snapshot.
pub const MAX_TASK_PROGRESS_SNAPSHOT_BYTES: usize = 16 * 1024;

/// Bounded progress snapshot persisted independently of task lifecycle state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskProgressSnapshot {
    /// Attempt that produced this snapshot.
    pub attempt: u32,
    /// Monotonic version within the attempt.
    pub progress_version: u64,
    /// Current execution stage.
    pub stage: Option<ProgressStageSnapshot>,
    /// Current metric snapshots.
    pub metrics: Vec<ProgressMetricSnapshot>,
    /// Unix epoch milliseconds when this snapshot was produced.
    pub updated_at_ms: u64,
}

impl TaskProgressSnapshot {
    /// Builds and validates a persistable snapshot from an rs-progress event.
    ///
    /// # Parameters
    ///
    /// * `command` - Progress command containing the version and event fields.
    ///
    /// # Returns
    ///
    /// A serializable snapshot retaining the supplied attempt and version.
    ///
    /// # Errors
    ///
    /// Returns an error when there are too many metrics or the serialized
    /// snapshot exceeds 16 KiB.
    pub fn from_command(command: ProgressCommand) -> Result<Self, ProgressSnapshotError> {
        if command.metrics.len() > MAX_TASK_PROGRESS_METRICS {
            return Err(ProgressSnapshotError::TooManyMetrics(command.metrics.len()));
        }
        if let Some(stage) = command.stage.as_ref() {
            if stage.id().len() > 128 {
                return Err(ProgressSnapshotError::StageIdTooLarge(stage.id().len()));
            }
            if stage.name().len() > 256 {
                return Err(ProgressSnapshotError::StageNameTooLarge(stage.name().len()));
            }
        }
        let stage = command.stage.map(|stage| ProgressStageSnapshot {
            id: stage.id().to_owned(),
            name: stage.name().to_owned(),
            position: stage.position_value(),
            total: stage.total(),
        });
        let metrics = command
            .metrics
            .into_iter()
            .map(|metric| ProgressMetricSnapshot {
                id: metric.id().to_owned(),
                name: metric.name().to_owned(),
                total: metric.total(),
                completed: metric.completed(),
                active: metric.active(),
                succeeded: metric.succeeded(),
                failed: metric.failed(),
                cancelled: metric.cancelled(),
            })
            .collect();
        let snapshot = Self {
            attempt: command.expected_attempt,
            progress_version: command.progress_version,
            stage,
            metrics,
            updated_at_ms: command.updated_at_ms,
        };
        let encoded = serde_json::to_vec(&snapshot).map_err(ProgressSnapshotError::Serialize)?;
        if encoded.len() > MAX_TASK_PROGRESS_SNAPSHOT_BYTES {
            return Err(ProgressSnapshotError::TooLarge(encoded.len()));
        }
        Ok(snapshot)
    }
}
