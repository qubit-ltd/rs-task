// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
/// Outcome of a cancellation request.
///
/// # Examples
///
/// ```
/// use qubit_task::service::CancelOutcome;
///
/// let outcome = CancelOutcome::AlreadyTerminal;
/// assert!(matches!(outcome, CancelOutcome::AlreadyTerminal));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub enum CancelOutcome {
    /// The task was cancelled while it was queued.
    CancelledBeforeStart,
    /// Cooperative cancellation was signalled to a running handler.
    CancellationRequested,
    /// The running handler has no cooperative or external cancellation path.
    CancellationUnsupported,
    /// The task had already reached a terminal state.
    AlreadyTerminal,
}
