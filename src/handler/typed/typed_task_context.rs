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

use crate::model::next::TaskId;

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
    #[must_use]
    pub const fn task_id(&self) -> TaskId {
        self.task_id
    }

    /// Returns the one-based attempt number.
    #[must_use]
    pub const fn attempt(&self) -> u32 {
        self.attempt
    }

    /// Reports whether cancellation was requested for this attempt.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    /// Returns the signal that this handler may share with its child work.
    #[must_use]
    pub fn cancellation_signal(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.cancelled)
    }

    /// Creates a progress builder whose events are persisted for this task
    /// attempt before each report call completes.
    ///
    /// The handler chooses its stable metrics and initial stage before calling
    /// [`AsyncProgressBuilder::start_async`]. Later stage or metric changes
    /// become visible to task status queries after `report_async` completes.
    #[must_use]
    pub fn progress_builder(&self) -> AsyncProgressBuilder<'static> {
        AsyncProgress::builder_arc(Arc::clone(&self.progress_reporter))
    }
}
