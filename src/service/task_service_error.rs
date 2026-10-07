// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use crate::model::TaskStateKind;
use crate::model::next::TaskId;
use crate::store::StoreError;

/// Failure reported by a service operation.
///
/// # Examples
///
/// ```
/// use qubit_task::service::TaskServiceError;
///
/// let error = TaskServiceError::ShuttingDown;
/// assert_eq!(error.to_string(), "task execution service is shutting down");
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
    /// The submission exceeds the available payload capacity.
    #[error(
        "task submission capacity exceeded: requested {requested_bytes} bytes, {available_bytes} bytes available"
    )]
    SubmissionCapacityExceeded {
        /// Bytes requested by the rejected submission.
        requested_bytes: usize,
        /// Bytes available when the submission was checked.
        available_bytes: usize,
    },
    /// The store already contains the maximum number of unfinished tasks.
    #[error("unfinished task limit exceeded ({limit})")]
    UnfinishedTaskLimitExceeded {
        /// Maximum number of unfinished tasks allowed by the store.
        limit: usize,
    },
    /// The idempotency key is already associated with different request data.
    #[error("idempotency key conflicts with an existing task")]
    IdempotencyConflict,
    /// The submitted task identifier already exists.
    #[error("task identifier already exists")]
    DuplicateTaskId,
    /// The request contains invalid metadata or an oversized payload.
    #[error("invalid task request: {0}")]
    InvalidRequest(
        /// Validation diagnostic describing the rejected request field.
        String,
    ),
    /// The expected record revision exists, but its lifecycle is not blocked.
    #[error("task is not blocked (current state: {actual:?})")]
    NotBlocked {
        /// Lifecycle state observed when the operation was rejected.
        actual: TaskStateKind,
    },
    /// Cancellation must be resolved before a blocked task can be resumed.
    #[error("task has a pending cancellation request")]
    CancellationPending,
    /// New task submissions have been stopped.
    #[error("task execution service is shutting down")]
    ShuttingDown,
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
    /// The notification bus cannot retain messages without active subscribers.
    #[error(
        "notification provider '{provider_id}' is not durable; task notifications require a durable provider"
    )]
    NotificationProviderNotDurable {
        /// Identifier of the rejected event-bus provider.
        provider_id: String,
    },
    /// The task notification topic requires a codec that is not registered.
    #[cfg(feature = "event-bus")]
    #[error(
        "notification codec unavailable for topic '{topic}' on provider '{provider_id}': {source}"
    )]
    NotificationCodecUnavailable {
        /// Identifier of the event-bus provider whose configuration failed.
        provider_id: String,
        /// Topic that requires an encoded payload.
        topic: String,
        /// Codec readiness diagnostic returned by the event bus.
        #[source]
        source: qubit_event_bus::CapabilityError,
    },
    /// The task notification publisher failed while draining during shutdown.
    #[error("task notification publisher failed to close: {0}")]
    NotificationClose(
        /// Notification publisher close or worker failure diagnostic.
        String,
    ),
    /// An external cancellation hook failed for a running task attempt.
    #[error("external cancellation failed for task {task_id} attempt {attempt}: {message}")]
    ExternalCancellationFailed {
        /// Identifier of the task whose cancellation hook failed.
        task_id: TaskId,
        /// Running attempt for which the hook was invoked.
        attempt: u32,
        /// Failure diagnostic returned by the cancellation hook.
        message: String,
    },
    /// The running typed handler does not support cancellation.
    #[error("the running task handler does not support cancellation")]
    CancellationUnsupported,
}
