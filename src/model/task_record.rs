// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use serde::Deserialize;
use serde::Serialize;

use super::ResourceRequest;
use super::TaskId;
use super::TaskOutput;
use super::TaskRequest;
use super::TaskRequestInfo;
use super::TaskState;
use super::TaskSummary;

/// Queryable task lifecycle snapshot.
///
/// The state version increases after every successful lifecycle transition.
/// Timestamps are Unix epoch milliseconds, and `attempt` counts starts rather
/// than submissions.
///
/// # Examples
///
/// ```
/// use qubit_task::TaskExecutionService;
/// use qubit_task::model::TaskRequest;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let service = TaskExecutionService::in_memory().await?;
///     let request = TaskRequest::new("report", "1", vec![])
///         .with_idempotency_key("report-2026-09-26");
///     let record = service.submit(request).await?;
///     assert_eq!(record.attempt, 0);
///     service.shutdown().await?;
///     Ok(())
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskRecord {
    /// Stable service-generated identity.
    pub id: TaskId,
    /// Reconstructible work description.
    pub request: TaskRequest,
    /// Current lifecycle state.
    pub state: TaskState,
    /// Monotonically increasing state revision.
    pub state_version: u64,
    /// Number of execution attempts started.
    pub attempt: u32,
    /// Earliest Unix epoch millisecond when a queued retry may start.
    #[serde(default)]
    pub retry_not_before_ms: Option<u64>,
    /// Milliseconds since Unix epoch when accepted.
    pub accepted_at_ms: u64,
    /// Milliseconds since Unix epoch when execution last started.
    pub started_at_ms: Option<u64>,
    /// Milliseconds since Unix epoch when execution became terminal.
    pub finished_at_ms: Option<u64>,
    /// Actual resources assigned to the current or last attempt.
    pub assigned_resources: Vec<String>,
    /// Small output summary for successful work.
    pub output: Option<TaskOutput>,
    /// True after cooperative cancellation has been requested.
    pub cancel_requested: bool,
}

/// Provides access to the resource request without opening the original
/// request.
impl TaskRecord {
    /// Returns the resource demand used to validate and schedule this task.
    ///
    /// # Returns
    ///
    /// A borrow of the resource request stored inside this record.
    #[must_use]
    #[inline]
    pub fn resource_request(&self) -> &ResourceRequest {
        &self.request.resources
    }

    /// Copies lifecycle and immutable request metadata without its payload.
    ///
    /// # Returns
    ///
    /// Lifecycle data and immutable request metadata, excluding the payload.
    #[must_use]
    pub fn summary(&self) -> TaskSummary {
        TaskSummary {
            id: self.id,
            request: TaskRequestInfo::from(&self.request),
            state: self.state.clone(),
            state_version: self.state_version,
            attempt: self.attempt,
            retry_not_before_ms: self.retry_not_before_ms,
            accepted_at_ms: self.accepted_at_ms,
            started_at_ms: self.started_at_ms,
            finished_at_ms: self.finished_at_ms,
            assigned_resources: self.assigned_resources.clone(),
            output: self.output.clone(),
            cancel_requested: self.cancel_requested,
        }
    }
}
