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
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use qubit_task::handler::TaskContext;
/// use qubit_task::model::TaskOutput;
/// use qubit_task::service::LocalTaskOutcome;
/// use qubit_task::TaskExecutionService;
///
/// let service = TaskExecutionService::in_memory().await?;
/// let handle = service.submit_local(|context: TaskContext| {
///     assert!(context.attempt() > 0);
///     LocalTaskOutcome::<(), String>::Succeeded { value: (), summary: TaskOutput::default() }
/// }).await?;
/// let _result = handle.result().await?;
/// service.shutdown().await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct TaskContext {
    /// Stable identity of the accepted task.
    id: TaskId,
    /// One-based execution attempt number.
    attempt: u32,
    /// Immutable resource assignments shared with the handler.
    assigned_resources: Arc<[String]>,
    /// Shared cooperative cancellation state.
    cancelled: Arc<AtomicBool>,
}

impl TaskContext {
    /// Creates context for one service-managed execution attempt.
    ///
    /// The cancellation flag is shared with the service and engine. Resource
    /// assignments are copied into immutable shared storage for the handler.
    ///
    /// # Parameters
    ///
    /// * `id` - Stable task identity.
    /// * `attempt` - One-based execution attempt number.
    /// * `assigned_resources` - Resources reserved for this attempt.
    /// * `cancelled` - Shared cancellation signal.
    ///
    /// # Returns
    ///
    /// Context exposing attempt metadata and cooperative cancellation.
    pub(crate) fn new(id: TaskId, attempt: u32, assigned_resources: Vec<String>, cancelled: Arc<AtomicBool>) -> Self {
        Self {
            id,
            attempt,
            assigned_resources: assigned_resources.into(),
            cancelled,
        }
    }

    /// Returns the identifier of the accepted task.
    ///
    /// # Returns
    ///
    /// The stable task identity.
    #[must_use]
    #[inline]
    pub fn task_id(&self) -> TaskId {
        self.id
    }

    /// Returns the one-based attempt number.
    ///
    /// # Returns
    ///
    /// The attempt counter for this execution.
    #[must_use]
    #[inline]
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    /// Returns the devices and resources allocated to this attempt.
    ///
    /// # Returns
    ///
    /// A borrowed slice of assigned resource identifiers.
    #[must_use]
    #[inline]
    pub fn assigned_resources(&self) -> &[String] {
        &self.assigned_resources
    }

    /// Reports whether a caller has requested cooperative cancellation.
    ///
    /// # Returns
    ///
    /// Whether the shared cancellation flag is currently set.
    #[must_use]
    #[inline]
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    /// Returns the signal shared with the execution engine for cancellation.
    ///
    /// # Returns
    ///
    /// A shared handle to the cancellation flag.
    #[must_use]
    pub fn cancellation_signal(&self) -> Arc<AtomicBool> {
        self.cancelled.clone()
    }
}
