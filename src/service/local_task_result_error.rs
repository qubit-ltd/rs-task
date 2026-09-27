// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
/// Failure to obtain a typed local result after a task was accepted.
///
/// These errors distinguish task outcomes from failures to persist or deliver
/// the process-local value.
///
/// # Examples
///
/// ```
/// use qubit_task::service::LocalTaskResultError;
///
/// let error = LocalTaskResultError::Cancelled;
/// assert_eq!(error.to_string(), "local task was cancelled");
/// ```
#[derive(Debug, thiserror::Error)]
#[must_use]
pub enum LocalTaskResultError {
    /// Execution was cancelled before or during the handler.
    #[error("local task was cancelled")]
    Cancelled,
    /// Execution panicked.
    #[error("local task panicked: {0}")]
    Panicked(
        /// Panic diagnostic reported by the execution engine.
        String,
    ),
    /// Execution cannot currently continue.
    #[error("local task is blocked: {0}")]
    Blocked(
        /// Reason the task requires intervention before it can continue.
        String,
    ),
    /// The engine failed without a typed application error.
    #[error("local task infrastructure failed: {0}")]
    Infrastructure(
        /// Diagnostic from the execution engine or service infrastructure.
        String,
    ),
    /// A task store failure prevented authoritative finalization.
    #[error("local task store is unavailable: {0}")]
    StoreUnavailable(
        /// Store diagnostic that prevented authoritative finalization.
        String,
    ),
    /// The typed result channel closed unexpectedly.
    #[error("local task result channel closed")]
    ResultChannelClosed,
    /// The authoritative finalization channel closed unexpectedly.
    #[error("local task finalization channel closed")]
    FinalizationChannelClosed,
}
