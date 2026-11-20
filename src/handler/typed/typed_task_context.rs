// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use qubit_progress::AsyncProgress;
use qubit_progress::AsyncProgressBuilder;
use qubit_progress::AsyncReporter;

#[cfg(feature = "operation")]
use super::attempt_cancellation::AttemptCancellation;
use crate::model::typed::TaskId;

/// Per-attempt identity and cooperative cancellation state for typed handlers.
///
/// The task execution service supplies this context when it invokes a typed
/// handler, so handlers can inspect task identity, cancellation state, and
/// report progress without constructing the context themselves.
///
/// # Examples
///
/// ```
/// use qubit_task::TaskContext;
///
/// fn should_continue(context: &TaskContext) -> bool {
///     !context.is_cancelled()
/// }
/// ```
#[derive(Clone)]
pub struct TypedTaskContext {
    /// Stable identifier shared by every attempt of the task.
    task_id: TaskId,
    /// One-based number of this execution attempt.
    attempt: u32,
    /// Shared flag observed by this handler and its child work.
    cancelled: Arc<AtomicBool>,
    /// Notification channel shared with the service's attempt cancellation state.
    #[cfg(feature = "operation")]
    cancellation: Arc<AttemptCancellation>,
    /// Reporter that persists progress events for this attempt.
    progress_reporter: Arc<dyn AsyncReporter>,
}

impl TypedTaskContext {
    /// Creates the context owned by the typed service for one attempt.
    ///
    /// # Parameters
    ///
    /// * `task_id` identifies the task across its attempts.
    /// * `attempt` is the one-based number of this attempt.
    /// * `cancelled` is the shared cancellation flag for this attempt.
    /// * `progress_reporter` persists progress reports for this task attempt.
    ///
    /// # Returns
    ///
    /// A context that shares the supplied cancellation flag and progress
    /// reporter.
    pub(crate) fn new(
        task_id: TaskId,
        attempt: u32,
        cancelled: Arc<AtomicBool>,
        #[cfg(feature = "operation")] cancellation: Arc<AttemptCancellation>,
        progress_reporter: Arc<dyn AsyncReporter>,
    ) -> Self {
        Self {
            task_id,
            attempt,
            cancelled,
            #[cfg(feature = "operation")]
            cancellation,
            progress_reporter,
        }
    }

    /// Returns the stable task identifier.
    ///
    /// # Returns
    ///
    /// The identifier shared by all attempts of this task.
    #[must_use]
    #[inline]
    pub const fn task_id(&self) -> TaskId {
        self.task_id
    }

    /// Returns the one-based attempt number.
    ///
    /// # Returns
    ///
    /// The number of this attempt, starting at one.
    #[must_use]
    #[inline]
    pub const fn attempt(&self) -> u32 {
        self.attempt
    }

    /// Reports whether cancellation was requested for this attempt.
    ///
    /// # Returns
    ///
    /// `true` if the shared cancellation flag is set, or `false` otherwise.
    #[must_use]
    #[inline]
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    /// Returns the signal that this handler may share with its child work.
    ///
    /// Cloning the `Arc` does not create a new cancellation state; updates to
    /// the returned flag remain visible to this context and its other holders.
    ///
    /// # Returns
    ///
    /// A shared handle to this attempt's cancellation flag.
    #[must_use]
    #[inline]
    pub fn cancellation_signal(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.cancelled)
    }

    /// Waits for a persisted cooperative cancellation request from the task service.
    ///
    /// This notification can be awaited repeatedly. Writes made directly to
    /// [`cancellation_signal`](Self::cancellation_signal) remain visible through
    /// [`is_cancelled`](Self::is_cancelled), but do not publish this notification.
    /// Under cancellation modes other than `Cooperative`, this future may remain
    /// pending indefinitely.
    #[cfg(feature = "operation")]
    pub async fn wait_cancel_requested(&self) {
        self.cancellation.wait_requested().await;
    }

    /// Creates a progress builder whose events are persisted for this task
    /// attempt before each report call completes.
    ///
    /// The handler chooses its stable metrics and initial stage before calling
    /// [`AsyncProgressBuilder::start_async`]. Later stage or metric changes
    /// become visible to task status queries after `report_async` completes.
    ///
    /// # Returns
    ///
    /// A builder that reports progress through this context's reporter.
    #[inline]
    pub fn progress_builder(&self) -> AsyncProgressBuilder<'static> {
        AsyncProgress::builder_arc(Arc::clone(&self.progress_reporter))
    }
}
