// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;

use super::EngineError;
use super::ExecutionHandle;
use super::PreparedExecution;
use crate::handler::TaskContext;
use crate::handler::TaskHandler;
use crate::model::ResourceRequest;
use crate::model::ResourceSnapshot;
use crate::model::TaskId;
use crate::store::TaskFuture;

/// Resource-aware execution engine extension point.
///
/// Implementations reserve all requested resources before changing the task to
/// `Running` and keep the reservation until the execution attempt exits.
///
/// # Examples
///
/// ```
/// use qubit_task::engine::LocalTaskExecutionEngine;
/// use qubit_task::engine::TaskExecutionEngine;
/// use qubit_task::model::ResourceCapacity;
///
/// let engine = LocalTaskExecutionEngine::new(ResourceCapacity::default());
/// assert_eq!(engine.capacity().capacity.cpu_slots, 0);
/// ```
pub trait TaskExecutionEngine: Send + Sync {
    /// Reports configured capacity and current reservations.
    ///
    /// # Returns
    ///
    /// A snapshot of total capacity and currently reserved resources.
    #[must_use]
    fn capacity(&self) -> ResourceSnapshot;

    /// Atomically reserves all task resources without starting handler code.
    ///
    /// # Parameters
    ///
    /// * `id` - Stable identity of the task attempt.
    /// * `request` - Resources required by the task.
    ///
    /// # Returns
    ///
    /// A future resolving to a reservation or a classified engine error.
    ///
    /// # Errors
    ///
    /// Resolves to `TemporarilyUnavailable`, `Unsatisfiable`, or `Closed` when
    /// reservation cannot proceed.
    fn prepare<'a>(
        &'a self,
        id: TaskId,
        request: ResourceRequest,
    ) -> TaskFuture<'a, Result<PreparedExecution, EngineError>>;

    /// Starts a prepared task attempt and releases its reservation on exit.
    ///
    /// # Parameters
    ///
    /// * `prepared` - Reservation created for this attempt.
    /// * `handler` - Handler that runs the task.
    /// * `payload` - Task request payload.
    /// * `context` - Attempt identity, resources, and cancellation signal.
    ///
    /// # Returns
    ///
    /// A future resolving to an execution handle for the started attempt.
    ///
    /// # Errors
    ///
    /// Resolves to an engine error if the prepared attempt cannot be started.
    fn activate<'a>(
        &'a self,
        prepared: PreparedExecution,
        handler: Arc<dyn TaskHandler>,
        payload: Vec<u8>,
        context: TaskContext,
    ) -> TaskFuture<'a, Result<ExecutionHandle, EngineError>>;
}
