// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use qubit_metadata::Metadata;
use serde::Deserialize;
use serde::Serialize;

use super::ResourceRequest;
use super::TaskId;
use super::TaskProgressSnapshot;
use crate::model::TaskOutput;
use crate::model::TaskState;

/// Payload-free task view including current execution progress.
///
/// A summary is returned by public task-store and execution-service operations,
/// such as `TaskExecutionService::get`; callers can inspect its state and
/// progress without decoding the task payload. Its public fields also allow
/// callers to construct summaries directly when needed.
///
/// # Examples
///
/// ```
/// use qubit_task::model::TaskState;
/// use qubit_task::model::typed::ResourceRequest;
/// use qubit_task::model::typed::TaskId;
/// use qubit_task::model::typed::TaskSummary;
///
/// let id = TaskId::from_id(qubit_id::Id::new(42));
/// let summary = TaskSummary {
///     id,
///     kind_id: String::from("counter.add"),
///     category: None,
///     payload_type_id: String::from("example.Counter"),
///     payload_schema_version: 1,
///     payload_codec_id: String::from("example.u32_le"),
///     metadata: qubit_metadata::Metadata::default(),
///     resource_limit: ResourceRequest::default(),
///     correlation_key: None,
///     idempotency_key: None,
///     state: TaskState::Queued,
///     cancel_requested: false,
///     cancel_error: None,
///     state_version: 0,
///     attempt: 0,
///     retry_not_before_ms: None,
///     accepted_at_ms: 1_700_000_000_000,
///     started_at_ms: None,
///     finished_at_ms: None,
///     progress: None,
///     output: None,
/// };
///
/// assert_eq!(summary.id, id);
/// assert_eq!(summary.state, TaskState::Queued);
/// ```
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
