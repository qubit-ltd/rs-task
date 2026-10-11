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

use crate::model::typed::TaskId;

/// Per-attempt identity and cooperative cancellation state for typed handlers.
#[derive(Clone)]
pub struct TypedTaskContext {
    task_id: TaskId,
    attempt: u32,
    cancelled: Arc<AtomicBool>,
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
        progress_reporter: Arc<dyn AsyncReporter>,
    ) -> Self {
        Self {
            task_id,
            attempt,
            cancelled,
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
    #[must_use]
    #[inline]
    pub fn progress_builder(&self) -> AsyncProgressBuilder<'static> {
        AsyncProgress::builder_arc(Arc::clone(&self.progress_reporter))
    }
}
