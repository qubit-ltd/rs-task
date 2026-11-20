// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Errors returned while waiting for a task to reach a terminal state.

use crate::TaskId;
use crate::store::StoreError;

/// Explains why a terminal task checkpoint could not be obtained.
///
/// # Examples
///
/// ```
/// use qubit_task::TaskId;
/// use qubit_task::service::WaitTaskError;
///
/// let error = WaitTaskError::Timeout(TaskId::new(7));
/// assert!(matches!(error, WaitTaskError::Timeout(_)));
/// ```
#[derive(Debug, thiserror::Error)]
#[must_use]
pub enum WaitTaskError {
    /// No task with this ID is available in the store.
    #[error("task {0} is unavailable")]
    Unavailable(
        /// Identifier of the missing task.
        TaskId,
    ),
    /// The task did not reach a terminal state before the deadline.
    #[error("timed out waiting for task {0}")]
    Timeout(
        /// Identifier of the task that did not finish in time.
        TaskId,
    ),
    /// The store failed while reading task state.
    #[error("failed to read task state: {0}")]
    Read(
        /// Store failure encountered while reading the task.
        #[source]
        StoreError,
    ),
}
