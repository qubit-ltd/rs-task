use qubit_metadata::Metadata;
use serde::Deserialize;
use serde::Serialize;

use super::ResourceRequest;
use super::TaskId;
use super::TaskProgressSnapshot;
use crate::model::TaskOutput;
use crate::model::TaskState;

/// Payload-free task view including current execution progress.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskSummary {
    /// Stable task identity.
    pub id: TaskId,
    /// Handler routing key.
    pub kind_id: String,
    /// Optional query category.
    pub category: Option<String>,
    /// Stable model identity of the payload.
    pub payload_type_id: String,
    /// Payload schema version.
    pub payload_schema_version: u32,
    /// Stable bytes codec identity.
    pub payload_codec_id: String,
    /// Structured application metadata.
    pub metadata: Metadata,
    /// Requested scheduling quota.
    pub resource_limit: ResourceRequest,
    /// Optional correlation key.
    pub correlation_key: Option<String>,
    /// Optional idempotency key.
    pub idempotency_key: Option<String>,
    /// Current task lifecycle state.
    pub state: TaskState,
    /// Whether cancellation has been requested but not yet acknowledged.
    pub cancel_requested: bool,
    /// Error from an external cancellation hook, if the hook failed.
    pub cancel_error: Option<String>,
    /// Revision of the task lifecycle state.
    pub state_version: u64,
    /// Number of execution attempts started.
    pub attempt: u32,
    /// Earliest Unix epoch millisecond when a queued retry may start.
    pub retry_not_before_ms: Option<u64>,
    /// Unix epoch milliseconds when accepted.
    pub accepted_at_ms: u64,
    /// Unix epoch milliseconds when execution last started.
    pub started_at_ms: Option<u64>,
    /// Unix epoch milliseconds when the task finished.
    pub finished_at_ms: Option<u64>,
    /// Current persisted progress snapshot, if the task has reported progress.
    pub progress: Option<TaskProgressSnapshot>,
    /// Bounded result summary written when the task succeeds.
    pub output: Option<TaskOutput>,
}
