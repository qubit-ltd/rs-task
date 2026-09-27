// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use serde::Deserialize;
use serde::Serialize;

/// Maximum byte length of a persisted diagnostic category.
pub const MAX_TASK_DIAGNOSTIC_CATEGORY_BYTES: usize = 128;
/// Maximum byte length of a persisted diagnostic message.
pub const MAX_TASK_DIAGNOSTIC_MESSAGE_BYTES: usize = 4 * 1024;

/// Classified failure reported by a handler.
///
/// Set `retryable` only when repeating the operation is safe under the
/// application's idempotency and side-effect rules. Persisted category and
/// message strings are byte-bounded by the service.
///
/// # Examples
///
/// ```
/// use qubit_task::model::TaskRunError;
///
/// let error = TaskRunError {
///     category: "remote_unavailable".into(),
///     message: "try again later".into(),
///     retryable: true,
/// };
/// assert!(error.retryable);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[must_use]
pub struct TaskRunError {
    /// Stable error category suitable for business logic.
    pub category: String,
    /// Human-readable diagnostic summary.
    pub message: String,
    /// Whether the service may retry this attempt.
    pub retryable: bool,
}
