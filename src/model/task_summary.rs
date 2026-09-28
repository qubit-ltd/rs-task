// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use serde::Deserialize;
use serde::Serialize;

use super::TaskId;
use super::TaskOutput;
use super::TaskRecord;
use super::TaskRequestInfo;
use super::TaskState;

/// Payload-free lifecycle snapshot for listing and waiting on tasks.
///
/// # Examples
///
/// ```
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     use qubit_task::TaskExecutionService;
///     use qubit_task::model::TaskRequest;
///
///     let service = TaskExecutionService::in_memory().await?;
///     let request = TaskRequest::new("report", "1", vec![])
///         .with_idempotency_key("report-summary-example");
///     let accepted = service.submit(request).await?;
///     let summary = service.get_summary(accepted.id).await?.expect("accepted task is retained");
///     assert_eq!(summary.id, accepted.id);
///     service.shutdown().await?;
///     Ok(())
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskSummary {
    /// Stable service-generated identity.
    pub id: TaskId,
    /// Immutable request fields without the execution payload.
    pub request: TaskRequestInfo,
    /// Current lifecycle state.
    pub state: TaskState,
    /// Monotonically increasing state revision.
    pub state_version: u64,
    /// Number of execution attempts started.
    pub attempt: u32,
    /// Earliest Unix epoch millisecond when a queued retry may start.
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

impl TaskRecord {
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
