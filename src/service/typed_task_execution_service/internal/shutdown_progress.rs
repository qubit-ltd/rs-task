// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use super::ShutdownOutcome;

/// Tracks shutdown attempts and results shared among concurrent callers.
#[derive(Default)]
pub(in crate::service::typed_task_execution_service) struct ShutdownProgress {
    /// Index of the shutdown result currently being produced.
    pub(in crate::service::typed_task_execution_service) attempt: usize,
    /// Results retained for callers that joined earlier attempts.
    pub(in crate::service::typed_task_execution_service) results: Vec<ShutdownOutcome>,
    /// Whether a failed owner release can be retried.
    pub(in crate::service::typed_task_execution_service) retryable_release: bool,
    /// Whether a caller requested another owner release attempt.
    pub(in crate::service::typed_task_execution_service) retry_requested: bool,
}
