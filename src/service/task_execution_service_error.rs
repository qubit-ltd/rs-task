// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use qubit_executor::service::SubmissionError;
use thiserror::Error;

use super::task_id::TaskId;

/// Error returned when [`TaskExecutionService`](super::TaskExecutionService)
/// cannot accept a task.
///
/// This error is about the service submission path. The accepted task's own
/// result is still reported through [`TaskHandle`](qubit_executor::TaskHandle).
#[derive(Debug, Error)]
pub enum TaskExecutionServiceError {
    /// Another retained task record already uses the same task ID.
    #[error("task {0} already exists")]
    DuplicateTask(TaskId),

    /// The service is suspended and temporarily refuses new tasks.
    #[error("task execution service is suspended")]
    Suspended,

    /// The underlying thread pool rejected the task.
    #[error(transparent)]
    Rejected(#[from] SubmissionError),
}
