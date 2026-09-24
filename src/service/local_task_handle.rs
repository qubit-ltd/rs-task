// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use tokio::sync::oneshot;

use super::local_task_result_error::LocalTaskResultError;
use crate::model::TaskId;
use crate::model::TaskState;

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
