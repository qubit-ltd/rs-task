// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use qubit_executor::service::SubmissionError;
use qubit_id::Id;
use qubit_thread_pool::PoolJobSubmissionError;
use thiserror::Error;

/// Error returned when [`TaskExecutionService`](super::TaskExecutionService)
/// cannot accept a task.
///
/// This error is about the service submission path. The accepted task's own
/// result is still reported through [`TaskHandle`](super::TaskHandle).
///
/// # Examples
///
/// ```
/// use qubit_id::Id;
/// use qubit_task::service::TaskExecutionServiceError;
///
/// let error = TaskExecutionServiceError::DuplicateTask(Id::new(7));
/// assert_eq!(error.to_string(), "task 7 already exists");
/// ```
#[derive(Debug, Error)]
pub enum TaskExecutionServiceError {
    /// Another active task or pending submission already uses this task ID.
    #[error("task {0} already exists")]
    DuplicateTask(Id),

    /// The service is suspended and temporarily refuses new tasks.
    #[error("task execution service is suspended")]
    Suspended,

    /// The underlying thread pool rejected the task.
    #[error(transparent)]
    Rejected(#[from] SubmissionError),

    /// The pool acceptance callback panicked before the task was published.
    #[error("thread pool task acceptance callback panicked")]
    AcceptancePanicked,
}

impl From<PoolJobSubmissionError> for TaskExecutionServiceError {
    /// Converts a thread-pool submission error into a service submission error.
    fn from(error: PoolJobSubmissionError) -> Self {
        match error {
            PoolJobSubmissionError::Rejected(error) => Self::Rejected(error),
            PoolJobSubmissionError::AcceptancePanicked => Self::AcceptancePanicked,
        }
    }
}
