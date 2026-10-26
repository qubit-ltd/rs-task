// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;

use super::ExternalCancelFlight;

/// Tracks an external cancellation hook for one task attempt.
pub(in crate::service::typed_task_execution_service) enum ExternalCancelEntry {
    /// An external cancellation attempt shared by concurrent callers.
    InFlight(Arc<ExternalCancelFlight>),
    /// The cancellation hook succeeded for this attempt.
    Succeeded,
}
