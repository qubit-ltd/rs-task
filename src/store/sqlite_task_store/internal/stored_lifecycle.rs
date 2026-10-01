// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use crate::model::TaskId;
use crate::model::TaskOutput;
use crate::model::TaskRecord;
use crate::model::TaskRequest;
use crate::model::TaskRequestInfo;
use crate::model::TaskState;
use crate::model::TaskSummary;

/// Immutable task request fields are stored separately from this lifecycle.
#[derive(serde::Serialize, serde::Deserialize)]
pub(in crate::store::sqlite_task_store) struct StoredLifecycle {
    /// Stable task identity copied from the indexed row.
    pub(in crate::store::sqlite_task_store) id: TaskId,
    /// Mutable lifecycle state.
    pub(in crate::store::sqlite_task_store) state: TaskState,
    /// Monotonic lifecycle revision.
    pub(in crate::store::sqlite_task_store) state_version: u64,
    /// Number of attempts started.
    pub(in crate::store::sqlite_task_store) attempt: u32,
    /// Earliest eligible retry timestamp, if delayed.
    pub(in crate::store::sqlite_task_store) retry_not_before_ms: Option<u64>,
    /// Acceptance timestamp in Unix epoch milliseconds.
    pub(in crate::store::sqlite_task_store) accepted_at_ms: u64,
    /// Timestamp of the most recent execution start.
    pub(in crate::store::sqlite_task_store) started_at_ms: Option<u64>,
    /// Timestamp when the task became terminal.
    pub(in crate::store::sqlite_task_store) finished_at_ms: Option<u64>,
    /// Resources assigned to the current or last attempt.
    pub(in crate::store::sqlite_task_store) assigned_resources: Vec<String>,
    /// Bounded output summary from successful work.
    pub(in crate::store::sqlite_task_store) output: Option<TaskOutput>,
    /// Whether cooperative cancellation has been requested.
    pub(in crate::store::sqlite_task_store) cancel_requested: bool,
}

impl StoredLifecycle {
    /// Copies mutable and lifecycle fields from a complete record.
    ///
    /// # Parameters
    ///
    /// * `record` - Complete task record being persisted.
    ///
    /// # Returns
    ///
    /// Lifecycle fields copied without request data.
    pub(in crate::store::sqlite_task_store) fn from_record(record: &TaskRecord) -> Self {
        Self {
            id: record.id,
            state: record.state.clone(),
            state_version: record.state_version,
            attempt: record.attempt,
            retry_not_before_ms: record.retry_not_before_ms,
            accepted_at_ms: record.accepted_at_ms,
            started_at_ms: record.started_at_ms,
            finished_at_ms: record.finished_at_ms,
            assigned_resources: record.assigned_resources.clone(),
            output: record.output.clone(),
            cancel_requested: record.cancel_requested,
        }
    }

    /// Copies mutable lifecycle fields from a payload-free task summary.
    ///
    /// # Parameters
    ///
    /// * `record` - Task summary being persisted.
    ///
    /// # Returns
    ///
    /// Lifecycle fields copied without request data.
    #[cfg_attr(test, allow(dead_code))]
    pub(in crate::store::sqlite_task_store) fn from_summary(record: &TaskSummary) -> Self {
        Self {
            id: record.id,
            state: record.state.clone(),
            state_version: record.state_version,
            attempt: record.attempt,
            retry_not_before_ms: record.retry_not_before_ms,
            accepted_at_ms: record.accepted_at_ms,
            started_at_ms: record.started_at_ms,
            finished_at_ms: record.finished_at_ms,
            assigned_resources: record.assigned_resources.clone(),
            output: record.output.clone(),
            cancel_requested: record.cancel_requested,
        }
    }

    /// Reconstructs a complete record by attaching its immutable request.
    ///
    /// # Parameters
    ///
    /// * `request` - Immutable request associated with this lifecycle.
    ///
    /// # Returns
    ///
    /// A complete record reconstructed from persisted lifecycle values.
    pub(in crate::store::sqlite_task_store) fn into_record(self, request: TaskRequest) -> TaskRecord {
        TaskRecord {
            id: self.id,
            request,
            state: self.state,
            state_version: self.state_version,
            attempt: self.attempt,
            retry_not_before_ms: self.retry_not_before_ms,
            accepted_at_ms: self.accepted_at_ms,
            started_at_ms: self.started_at_ms,
            finished_at_ms: self.finished_at_ms,
            assigned_resources: self.assigned_resources,
            output: self.output,
            cancel_requested: self.cancel_requested,
        }
    }

    /// Reconstructs a payload-free summary from lifecycle and request fields.
    ///
    /// # Parameters
    ///
    /// * `request` - Immutable request metadata associated with this lifecycle.
    ///
    /// # Returns
    ///
    /// A summary reconstructed without loading the request payload.
    pub(in crate::store::sqlite_task_store) fn into_summary(self, request: TaskRequestInfo) -> TaskSummary {
        TaskSummary {
            id: self.id,
            request,
            state: self.state,
            state_version: self.state_version,
            attempt: self.attempt,
            retry_not_before_ms: self.retry_not_before_ms,
            accepted_at_ms: self.accepted_at_ms,
            started_at_ms: self.started_at_ms,
            finished_at_ms: self.finished_at_ms,
            assigned_resources: self.assigned_resources,
            output: self.output,
            cancel_requested: self.cancel_requested,
        }
    }
}
