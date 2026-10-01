//! New typed task request and persistence models used by the refactored API.

mod accept_outcome;
mod encoded_payload;
mod payload;
mod payload_encode_error;
mod progress_command;
mod progress_metric_snapshot;
mod progress_snapshot_error;
mod progress_stage_snapshot;
mod resource_request;
mod start_command;
mod stored_payload;
mod stored_task;
mod stored_task_request;
mod task_cursor;
mod task_id;
mod task_page;
mod task_progress_snapshot;
mod task_query;
mod task_request;
mod task_request_encode_error;
mod task_summary;
mod transition_command;

pub use accept_outcome::AcceptOutcome;
pub use encoded_payload::EncodedPayload;
pub use payload::Payload;
pub use payload_encode_error::PayloadEncodeError;
pub use progress_command::ProgressCommand;
pub use progress_metric_snapshot::ProgressMetricSnapshot;
pub use progress_snapshot_error::ProgressSnapshotError;
pub use progress_stage_snapshot::ProgressStageSnapshot;
pub use resource_request::ResourceRequest;
pub use start_command::StartCommand;
pub use stored_payload::StoredPayload;
pub use stored_task::StoredTask;
pub use stored_task_request::StoredTaskRequest;
pub use task_cursor::TaskCursor;
pub use task_id::TaskId;
pub use task_page::TaskPage;
#[cfg(not(test))]
pub use task_progress_snapshot::MAX_TASK_PROGRESS_METRICS;
#[cfg(not(test))]
pub use task_progress_snapshot::MAX_TASK_PROGRESS_SNAPSHOT_BYTES;
pub use task_progress_snapshot::TaskProgressSnapshot;
#[cfg(test)]
pub(crate) use task_query::MAX_TASK_QUERY_LIMIT;
pub use task_query::TaskQuery;
pub use task_request::MAX_TASK_METADATA_BYTES;
pub use task_request::MAX_TASK_METADATA_ENTRIES;
pub use task_request::TaskRequest;
pub use task_request_encode_error::TaskRequestEncodeError;
pub use task_summary::TaskSummary;
pub use transition_command::TransitionCommand;
