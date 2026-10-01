// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use crate::model::TaskStateKind;
use crate::store::StoreError;

/// Failure reported by a service operation.
///
/// # Examples
///
/// ```
/// use qubit_task::service::TaskServiceError;
///
/// let error = TaskServiceError::QueueFull;
/// assert_eq!(error.to_string(), "task queue is full");
/// ```
#[derive(Debug, thiserror::Error)]
#[must_use]
pub enum TaskServiceError {
    /// ID generation failed before task acceptance.
    #[error("task ID generation failed: {0}")]
    IdGeneration(#[from] qubit_id::IdGenerationError),
    /// Encoding or validating a typed task request failed.
    #[error("typed task request failed: {0}")]
    TypedRequest(String),
    /// The selected store failed an operation.
    #[error(transparent)]
    Store(
        /// Underlying storage failure.
        #[from]
        StoreError,
    ),
    /// The configured queue has no remaining waiting capacity.
    #[error("task queue is full")]
    QueueFull,
    /// The request payload would exceed the configured in-flight byte budget.
    #[error(
        "in-flight task payload budget exceeded: requested {requested_bytes} bytes, {available_bytes} bytes available"
    )]
    PayloadBudgetExceeded {
        /// Payload bytes in the rejected submission.
        requested_bytes: usize,
        /// Payload bytes remaining when the submission was checked.
        available_bytes: usize,
    },
    /// The configured number of detached admission workers is already in
    /// flight.
    #[error("in-flight task operation limit reached ({limit})")]
    OperationLimitExceeded {
        /// Maximum number of concurrent external write operations.
        limit: usize,
    },
    /// The request exceeds available configured capacity.
    #[error("task request cannot be satisfied by configured resources")]
    Unsatisfiable,
    /// The request contains invalid metadata or an oversized payload.
    #[error("invalid task request: {0}")]
    InvalidRequest(
        /// Validation diagnostic describing the rejected request field.
        String,
    ),
    /// The requested task is blocked pending intervention.
    #[error("task is blocked and requires intervention")]
    Blocked,
    /// The expected record revision exists, but its lifecycle is not blocked.
    #[error("task is not blocked (current state: {actual:?})")]
    NotBlocked {
        /// Lifecycle state observed when the operation was rejected.
        actual: TaskStateKind,
    },
    /// The task used all configured execution attempts and cannot be requeued.
    #[error("task exhausted its execution attempt budget ({attempts}/{limit})")]
    AttemptsExhausted {
        /// Number of attempts already started.
        attempts: u32,
        /// Maximum attempts configured for the service.
        limit: u32,
    },
    /// New task submissions have been stopped.
    #[error("task execution service is shutting down")]
    ShuttingDown,
    /// The caller's shutdown deadline expired while accepted work was draining.
    #[error("task execution service did not shut down before the deadline")]
    ShutdownTimedOut,
    /// A persistence failure suspended task acceptance and scheduling.
    #[error("task execution service is paused after a task store failure: {0}")]
    StoreUnavailable(
        /// First store failure retained by the service.
        String,
    ),
    /// The scheduler or execution engine cannot accept or start more work.
    #[error("task execution scheduler is unavailable: {0}")]
    SchedulerUnavailable(
        /// Scheduler or engine failure retained by the service.
        String,
    ),
    /// The task notification publisher failed while draining during shutdown.
    #[error("task notification publisher failed to close: {0}")]
    NotificationClose(
        /// Notification publisher close or worker failure diagnostic.
        String,
    ),
    /// No handler matches the submitted type and exact version.
    #[error("no handler registered for `{task_type}` version `{version}`")]
    MissingHandler {
        /// Task type requested by the submitted record.
        task_type: String,
        /// Exact version requested by the submitted record.
        version: String,
    },
    /// Reconstructable storage cannot accept a local closure.
    #[error("local closure submission is unavailable with a restart-recoverable store")]
    UnsupportedCapability,
    /// The running typed handler does not support cancellation.
    #[error("the running task handler does not support cancellation")]
    CancellationUnsupported,
}
