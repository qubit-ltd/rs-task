use serde::Deserialize;
use serde::Serialize;

/// Serializable metric projection from `rs-progress`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProgressMetricSnapshot {
    /// Machine-readable metric identifier.
    pub id: String,
    /// Human-readable metric name.
    pub name: String,
    /// Configured total, when known.
    pub total: Option<u64>,
    /// Completed work count.
    pub completed: u64,
    /// Active work count.
    pub active: u64,
    /// Explicitly successful work count.
    pub succeeded: u64,
    /// Explicitly failed work count.
    pub failed: u64,
    /// Explicitly cancelled work count.
    pub cancelled: u64,
}
