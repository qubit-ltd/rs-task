// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use crate::model::TaskId;

/// Opaque resource reservation prepared before the store commits `Running`.
///
/// # Examples
///
/// ```
/// use qubit_task::engine::PreparedExecution;
/// use qubit_task::TaskId;
///
/// let prepared = PreparedExecution::new(TaskId::generate(), Vec::new(), || {});
/// assert!(prepared.assigned_resources().is_empty());
/// ```
#[must_use]
pub struct PreparedExecution {
    /// Stable task identity for this reserved attempt.
    pub(crate) id: TaskId,
    /// Resource identifiers assigned to the attempt.
    pub(crate) assigned: Vec<String>,
    /// Callback that releases the reservation unless moved to the worker.
    pub(crate) release: Option<Box<dyn FnOnce() + Send>>,
}

impl PreparedExecution {
    /// Creates a prepared execution for a custom engine implementation.
    ///
    /// `release` must return all resources reserved for this task when the
    /// prepared execution is abandoned or the execution attempt finishes.
    ///
    /// # Type Parameters
    ///
    /// * `F` - Callback type that releases the reserved resources.
    ///
    /// # Parameters
    ///
    /// * `id` - Task whose resources were reserved.
    /// * `assigned` - Resource identifiers selected by the engine.
    /// * `release` - Callback that releases the reservation exactly once.
    ///
    /// # Returns
    ///
    /// A prepared reservation that releases itself when dropped unless its
    /// callback is transferred to an execution completion guard.
    pub fn new<F>(id: TaskId, assigned: Vec<String>, release: F) -> Self
    where
        F: FnOnce() + Send + 'static,
    {
        Self {
            id,
            assigned,
            release: Some(Box::new(release)),
        }
    }

    /// Returns the identifier associated with this prepared attempt.
    ///
    /// # Returns
    ///
    /// The stable task identifier.
    #[must_use]
    #[inline]
    pub fn task_id(&self) -> TaskId {
        self.id
    }

    /// Returns resources assigned by the engine when reserving this attempt.
    ///
    /// # Returns
    ///
    /// A borrowed slice of assigned resource identifiers.
    #[must_use]
    #[inline]
    pub fn assigned_resources(&self) -> &[String] {
        &self.assigned
    }

    /// Takes responsibility for releasing this reservation from the value.
    ///
    /// The engine should move the returned callback into its execution
    /// completion guard. If it leaves the callback in the prepared value,
    /// dropping the value releases the reservation immediately.
    ///
    /// # Returns
    ///
    /// The release callback, or `None` if responsibility was already taken.
    #[must_use]
    pub fn take_release(&mut self) -> Option<Box<dyn FnOnce() + Send>> {
        self.release.take()
    }
}

impl Drop for PreparedExecution {
    /// Releases any reservation whose callback was not transferred.
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            release();
        }
    }
}
