// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
/// Copyable shutdown failure retained for every caller awaiting close.
#[derive(Clone)]
#[must_use]
pub(in crate::service::admission_gate) enum CloseFailure {
    /// A shutdown failure without a dedicated service error category.
    Other(
        /// Display message retained for later shutdown callers.
        String,
    ),
    /// A task store failure that must be returned to every shutdown caller.
    Store(
        /// Store diagnostic retained for later shutdown callers.
        String,
    ),
    /// A scheduler worker failure that must be returned to every shutdown
    /// caller.
    Scheduler(
        /// Scheduler diagnostic retained for later shutdown callers.
        String,
    ),
    /// An event notification worker failed while closing.
    NotificationClose(
        /// Notification worker diagnostic retained for later callers.
        String,
    ),
}
