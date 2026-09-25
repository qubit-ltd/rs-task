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

use crate::model::TaskId;

/// Per-attempt metadata and cooperative cancellation signal.
///
/// The service creates this context for each started attempt. Handlers can
/// inspect resource assignments and poll or share the cancellation flag.
///
/// # Examples
///
/// ```
/// use qubit_task::handler::TaskHandler;
/// use qubit_task::handler::TaskContext;
///
/// fn observe_attempt(handler: &dyn TaskHandler, payload: &[u8], context: TaskContext) {
///     let _ = handler.run(payload, context);
/// }
/// ```
#[derive(Clone)]
pub struct TaskContext {
    id: TaskId,
    attempt: u32,
    assigned_resources: Arc<[String]>,
    cancelled: Arc<AtomicBool>,
}

impl TaskContext {
    /// Creates context for one service-managed execution attempt.
    ///
    /// The cancellation flag is shared with the service and engine. Resource
    /// assignments are copied into immutable shared storage for the handler.
    pub(crate) fn new(id: TaskId, attempt: u32, assigned_resources: Vec<String>, cancelled: Arc<AtomicBool>) -> Self {
        Self {
            id,
            attempt,
            assigned_resources: assigned_resources.into(),
            cancelled,
        }
    }

    /// Returns the identifier of the accepted task.
    #[must_use]
    #[inline]
    pub fn task_id(&self) -> TaskId {
        self.id
    }

    /// Returns the one-based attempt number.
    #[must_use]
    #[inline]
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    /// Returns the devices and resources allocated to this attempt.
    #[must_use]
    #[inline]
    pub fn assigned_resources(&self) -> &[String] {
        &self.assigned_resources
    }

    /// Reports whether a caller has requested cooperative cancellation.
    #[must_use]
    #[inline]
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    /// Returns the signal shared with the execution engine for cancellation.
    #[must_use]
    pub fn cancellation_signal(&self) -> Arc<AtomicBool> {
        self.cancelled.clone()
    }
}
