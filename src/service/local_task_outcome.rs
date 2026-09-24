// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::fmt::Display;

use tokio::sync::oneshot;

use crate::handler::TaskContext;
use crate::handler::TaskRunOutcome;
use crate::handler::TaskRunResult;
use crate::model::TaskOutput;
use crate::model::TaskRunError;

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

/// Converts a typed closure result into the handler outcome persisted by the service.
pub(crate) fn adapt_local_outcome<F, R, E>(
    task: F,
    sender: oneshot::Sender<Result<R, E>>,
) -> impl FnOnce(TaskContext) -> TaskRunResult + Send + 'static
where
    F: FnOnce(TaskContext) -> LocalTaskOutcome<R, E> + Send + 'static,
    R: Send + 'static,
    E: Display + Send + 'static,
{
    move |context| match task(context) {
        LocalTaskOutcome::Succeeded { value, summary } => {
            let _ = sender.send(Ok(value));
            Ok(TaskRunOutcome::Succeeded(summary))
        }
        LocalTaskOutcome::Failed(error) => {
            let message = error.to_string();
            let _ = sender.send(Err(error));
            Err(TaskRunError {
                category: "local".into(),
                message,
                retryable: false,
            })
        }
        LocalTaskOutcome::Cancelled => Ok(TaskRunOutcome::Cancelled),
    }
}
