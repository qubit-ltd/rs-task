// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;

use qubit_progress::AsyncProgress;
use qubit_progress::AsyncProgressBuilder;
use qubit_progress::AsyncReporter;

use super::attempt_cancellation::TaskCancellation;
use crate::model::TaskAttempt;

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
    attempt: TaskAttempt,
    /// Read-only cancellation view for this attempt.
    cancellation: TaskCancellation,
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
        attempt: TaskAttempt,
        cancellation: TaskCancellation,
        progress_reporter: Arc<dyn AsyncReporter>,
    ) -> Self {
        Self {
            attempt,
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
    pub const fn task_id(&self) -> crate::model::typed::TaskId {
        self.attempt.task_id()
    }

    /// Returns the one-based attempt number.
    ///
    /// # Returns
    ///
    /// The number of this attempt, starting at one.
    #[must_use]
    #[inline]
    pub const fn attempt(&self) -> u32 {
        self.attempt.attempt()
    }

    /// Reports whether cancellation was requested for this attempt.
    ///
    /// For external cancellation, the request becomes visible after its
    /// durable intent is recorded and before the service invokes the hook.
    ///
    /// # Returns
    ///
    /// `true` if the shared cancellation flag is set, or `false` otherwise.
    #[must_use]
    #[inline]
    pub fn is_cancelled(&self) -> bool {
        self.cancellation.is_requested()
    }

    /// Returns the attempt identity shared with external cancellation hooks.
    #[must_use]
    pub const fn task_attempt(&self) -> TaskAttempt {
        self.attempt
    }

    /// Returns a read-only cloneable view of the attempt cancellation state.
    #[must_use]
    pub fn cancellation(&self) -> TaskCancellation {
        self.cancellation.clone()
    }

    /// Waits for a persisted cooperative cancellation request from the task
    /// service.
    ///
    /// This notification can be awaited repeatedly. The read-only handle
    /// returned by [`cancellation`](Self::cancellation) observes every durable
    /// cancellation request, while this future completes only for cooperative
    /// requests. An external cancellation request is visible through that
    /// handle but does not complete this future.
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
