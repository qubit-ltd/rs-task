// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use tokio::sync::watch;

use super::ExternalCancelResult;

/// Publishes one external cancellation result to concurrent waiters.
pub(in crate::service::typed_task_execution_service) struct ExternalCancelFlight {
    /// Result channel observed by all callers waiting on this hook.
    pub(in crate::service::typed_task_execution_service) result: watch::Sender<Option<ExternalCancelResult>>,
}
