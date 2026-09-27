// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use tokio::sync::oneshot;

use super::ExecutionOutcome;

/// Completion notification returned when an attempt has started.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use std::sync::atomic::Ordering;
///
/// use tokio::sync::oneshot;
///
/// use qubit_task::engine::ExecutionHandle;
/// use qubit_task::engine::ExecutionOutcome;
///
/// let (_sender, receiver) = oneshot::channel::<ExecutionOutcome>();
/// let handle = ExecutionHandle::new(receiver, Arc::default());
/// assert!(!handle.cancellation_signal().load(Ordering::Relaxed));
/// ```
#[must_use]
pub struct ExecutionHandle {
    /// Completion receiver for the started attempt.
    pub(crate) receiver: oneshot::Receiver<ExecutionOutcome>,
    /// Shared cancellation signal for this attempt.
    pub(crate) cancelled: Arc<AtomicBool>,
}

impl ExecutionHandle {
    /// Creates a handle for a custom engine implementation.
    ///
    /// # Parameters
    ///
    /// * `receiver` - Completion channel for the started attempt.
    /// * `cancelled` - Shared cancellation flag set by the task service.
    ///
    /// # Returns
    ///
    /// A handle that exposes cancellation and receives the attempt outcome.
    pub fn new(receiver: oneshot::Receiver<ExecutionOutcome>, cancelled: Arc<AtomicBool>) -> Self {
        Self { receiver, cancelled }
    }

    /// Returns the signal that the task service sets when cancellation is
    /// requested.
    ///
    /// # Returns
    ///
    /// A shared flag set when the task service requests cancellation.
    #[must_use]
    #[inline]
    pub fn cancellation_signal(&self) -> Arc<AtomicBool> {
        self.cancelled.clone()
    }
}
