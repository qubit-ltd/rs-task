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
///
/// The handle owns one-shot result channels. Consuming it with
/// [`result`](Self::result) waits for persisted finalization before exposing
/// the process-local value.
///
/// # Type Parameters
///
/// * `R` - Value returned by the local closure on success.
/// * `E` - Application error returned by the local closure on failure.
///
/// # Examples
///
/// ```
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use qubit_task::TaskExecutionService;
/// use qubit_task::model::TaskOutput;
/// use qubit_task::service::LocalTaskOutcome;
///
/// let service = TaskExecutionService::in_memory().await?;
/// let handle = service.submit_local(|_| {
///     LocalTaskOutcome::<u32, std::io::Error>::Succeeded {
///         value: 7,
///         summary: TaskOutput { summary: b"seven".to_vec() },
///     }
/// }).await?;
/// assert_eq!(handle.result().await??, 7);
/// service.shutdown().await?;
/// # Ok(())
/// # }
/// ```
pub struct LocalTaskHandle<R, E> {
    /// Stable identity assigned by the task service.
    id: TaskId,
    /// One-shot channel carrying the closure's typed result.
    typed_result: oneshot::Receiver<Result<R, E>>,
    /// One-shot channel carrying the persisted final lifecycle state.
    final_state: oneshot::Receiver<Result<TaskState, LocalTaskResultError>>,
}

impl<R, E> LocalTaskHandle<R, E> {
    /// Creates a handle from the result and authoritative-finalization
    /// channels.
    ///
    /// # Parameters
    ///
    /// * `id` - Stable accepted task identity.
    /// * `typed_result` - Closure result receiver.
    /// * `final_state` - Persisted final-state receiver.
    ///
    /// # Returns
    ///
    /// A handle that waits for finalization before yielding the typed result.
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
    ///
    /// # Returns
    ///
    /// The task identifier.
    #[inline]
    #[must_use]
    pub fn task_id(&self) -> TaskId {
        self.id
    }

    /// Waits for the authoritative state before delivering a typed result.
    ///
    /// # Returns
    ///
    /// The original closure result for successful or failed application work;
    /// infrastructure, cancellation, blocked, and channel failures are
    /// reported as [`LocalTaskResultError`].
    ///
    /// # Errors
    ///
    /// Returns a finalization error for cancellation, panic, blocking,
    /// infrastructure failure, or a closed result channel.
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
    /// Formats the handle using its task identity without requiring `R` or `E`
    /// to implement `Debug`.
    ///
    /// # Parameters
    ///
    /// * `formatter` - Destination formatter.
    ///
    /// # Returns
    ///
    /// The formatter result, including any write failure.
    ///
    /// # Errors
    ///
    /// Returns [`std::fmt::Error`] if writing the debug representation fails.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("LocalTaskHandle").field("id", &self.id).finish()
    }
}
