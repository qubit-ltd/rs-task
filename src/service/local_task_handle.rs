// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::fmt::Display;

use tokio::sync::oneshot;

use crate::model::TaskId;
use crate::model::TaskOutput;
use crate::model::TaskState;

/// Outcome returned by a process-local task closure.
pub enum LocalTaskOutcome<R, E> {
    /// Provides a process-local value and a bounded persisted summary.
    Succeeded {
        /// Full value delivered only through the local handle.
        value: R,
        /// Small summary retained in the task record.
        summary: TaskOutput,
    },
    /// Provides the original application error type.
    Failed(E),
    /// Acknowledges cancellation without a result value.
    Cancelled,
}

/// Failure to obtain a typed local result after a task was accepted.
#[derive(Debug, thiserror::Error)]
pub enum LocalTaskResultError {
    /// Execution was cancelled before or during the handler.
    #[error("local task was cancelled")]
    Cancelled,
    /// Execution panicked.
    #[error("local task panicked: {0}")]
    Panicked(String),
    /// Execution cannot currently continue.
    #[error("local task is blocked: {0}")]
    Blocked(String),
    /// The engine failed without a typed application error.
    #[error("local task infrastructure failed: {0}")]
    Infrastructure(String),
    /// A task store failure prevented authoritative finalization.
    #[error("local task store is unavailable: {0}")]
    StoreUnavailable(String),
    /// The typed result channel closed unexpectedly.
    #[error("local task result channel closed")]
    ResultChannelClosed,
    /// The authoritative finalization channel closed unexpectedly.
    #[error("local task finalization channel closed")]
    FinalizationChannelClosed,
}

/// Process-local typed result of one accepted task.
pub struct LocalTaskHandle<R, E> {
    id: TaskId,
    typed_result: oneshot::Receiver<Result<R, E>>,
    final_state: oneshot::Receiver<Result<TaskState, LocalTaskResultError>>,
}

impl<R, E> LocalTaskHandle<R, E> {
    pub(crate) fn new(
        id: TaskId,
        typed_result: oneshot::Receiver<Result<R, E>>,
        final_state: oneshot::Receiver<Result<TaskState, LocalTaskResultError>>,
    ) -> Self {
        Self {
            id,
            typed_result,
            final_state,
        }
    }

    /// Returns the stable identity of this accepted task.
    #[must_use]
    pub fn task_id(&self) -> TaskId {
        self.id
    }

    /// Waits for the authoritative state before delivering a typed result.
    pub async fn result(self) -> Result<Result<R, E>, LocalTaskResultError> {
        let state = self
            .final_state
            .await
            .map_err(|_| LocalTaskResultError::FinalizationChannelClosed)??;
        match state {
            TaskState::Succeeded => self
                .typed_result
                .await
                .map_err(|_| LocalTaskResultError::ResultChannelClosed),
            TaskState::Failed { category, message } => match self.typed_result.await {
                Ok(Err(error)) => Ok(Err(error)),
                Ok(Ok(_)) => Err(LocalTaskResultError::Infrastructure(message)),
                Err(_) if category == "engine" => Err(LocalTaskResultError::Infrastructure(message)),
                Err(_) => Err(LocalTaskResultError::ResultChannelClosed),
            },
            TaskState::Cancelled => Err(LocalTaskResultError::Cancelled),
            TaskState::Panicked { message } => Err(LocalTaskResultError::Panicked(message)),
            TaskState::Blocked { reason } => Err(LocalTaskResultError::Blocked(reason)),
            TaskState::Queued | TaskState::Running => Err(LocalTaskResultError::Infrastructure(
                "finalization received a non-final task state".into(),
            )),
        }
    }
}

impl<R, E> std::fmt::Debug for LocalTaskHandle<R, E> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("LocalTaskHandle").field("id", &self.id).finish()
    }
}

pub(crate) fn adapt_local_outcome<F, R, E>(
    task: F,
    sender: oneshot::Sender<Result<R, E>>,
) -> impl FnOnce(crate::handler::TaskContext) -> crate::handler::TaskRunResult + Send + 'static
where
    F: FnOnce(crate::handler::TaskContext) -> LocalTaskOutcome<R, E> + Send + 'static,
    R: Send + 'static,
    E: Display + Send + 'static,
{
    move |context| match task(context) {
        LocalTaskOutcome::Succeeded { value, summary } => {
            let _ = sender.send(Ok(value));
            Ok(crate::handler::TaskRunOutcome::Succeeded(summary))
        }
        LocalTaskOutcome::Failed(error) => {
            let message = error.to_string();
            let _ = sender.send(Err(error));
            Err(crate::model::TaskRunError {
                category: "local".into(),
                message,
                retryable: false,
            })
        }
        LocalTaskOutcome::Cancelled => Ok(crate::handler::TaskRunOutcome::Cancelled),
    }
}
