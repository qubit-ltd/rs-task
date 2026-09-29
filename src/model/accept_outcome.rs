// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use super::TaskRecord;

/// Result of atomically accepting a task request.
///
/// `Existing` is returned for an identical request with the same idempotency
/// key, so callers should use the returned record in either case.
///
/// # Examples
///
/// ```
/// use qubit_task::model::AcceptOutcome;
/// use qubit_task::model::TaskId;
/// use qubit_task::model::TaskRecord;
/// use qubit_task::model::TaskRequest;
/// use qubit_task::model::TaskState;
///
/// let outcome = AcceptOutcome::Accepted(TaskRecord {
///     id: TaskId::generate(),
///     request: TaskRequest::new("report", "1", vec![]),
///     state: TaskState::Queued,
///     state_version: 0,
///     attempt: 0,
///     retry_not_before_ms: None,
///     accepted_at_ms: 0,
///     started_at_ms: None,
///     finished_at_ms: None,
///     assigned_resources: Vec::new(),
///     output: None,
///     cancel_requested: false,
/// });
/// let record = match outcome {
///     AcceptOutcome::Accepted(record) | AcceptOutcome::Existing(record) => record,
/// };
/// assert_eq!(record.state, TaskState::Queued);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub enum AcceptOutcome {
    /// A new task was accepted.
    Accepted(
        /// Newly retained task record.
        TaskRecord,
    ),
    /// An identical idempotent request already exists.
    Existing(
        /// Previously retained record associated with the idempotency key.
        TaskRecord,
    ),
}
