// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Pluggable resource reservation and task execution engines.

mod local;

use std::sync::Arc;

pub use local::LocalTaskExecutionEngine;

use crate::handler::TaskContext;
use crate::handler::TaskHandler;
use crate::handler::TaskRunResult;
use crate::model::ResourceCapacity;
use crate::model::ResourceRequest;
use crate::model::ResourceSnapshot;
use crate::model::TaskId;
use crate::store::TaskFuture;

/// Opaque resource reservation prepared before the store commits `Running`.
pub struct PreparedExecution {
    pub(crate) id: TaskId,
    pub(crate) assigned: Vec<String>,
    pub(crate) release: Option<Box<dyn FnOnce() + Send>>,
}

impl PreparedExecution {
    /// Creates a prepared execution for a custom engine implementation.
    ///
    /// `release` must return all resources reserved for this task when the
    /// prepared execution is abandoned or the execution attempt finishes.
    #[must_use]
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
    #[must_use]
    pub fn task_id(&self) -> TaskId {
        self.id
    }

    /// Returns resources assigned by the engine when reserving this attempt.
    #[must_use]
    pub fn assigned_resources(&self) -> &[String] {
        &self.assigned
    }

    /// Takes responsibility for releasing this reservation from the value.
    ///
    /// The engine should move the returned callback into its execution
    /// completion guard. If it leaves the callback in the prepared value,
    /// dropping the value releases the reservation immediately.
    pub fn take_release(&mut self) -> Option<Box<dyn FnOnce() + Send>> {
        self.release.take()
    }
}

impl Drop for PreparedExecution {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            release();
        }
    }
}

/// Outcome reported by an execution backend for one started attempt.
#[derive(Debug)]
pub enum ExecutionOutcome {
    /// The handler returned a result, including a classified application error.
    Returned(TaskRunResult),
    /// The handler panicked while constructing or polling its future.
    Panicked(String),
    /// The execution worker stopped before it could report a handler result.
    WorkerStopped(String),
}

/// Completion notification returned when an attempt has started.
pub struct ExecutionHandle {
    pub(crate) receiver: tokio::sync::oneshot::Receiver<ExecutionOutcome>,
    pub(crate) cancelled: Arc<std::sync::atomic::AtomicBool>,
}

impl ExecutionHandle {
    /// Creates a handle for a custom engine implementation.
    #[must_use]
    pub fn new(
        receiver: tokio::sync::oneshot::Receiver<ExecutionOutcome>,
        cancelled: Arc<std::sync::atomic::AtomicBool>,
    ) -> Self {
        Self { receiver, cancelled }
    }

    /// Returns the signal that the task service sets when cancellation is
    /// requested.
    #[must_use]
    pub fn cancellation_signal(&self) -> Arc<std::sync::atomic::AtomicBool> {
        self.cancelled.clone()
    }
}

/// Engine errors distinguish temporary contention from invalid capacity
/// requests.
#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    /// Resources are valid but currently reserved by other tasks.
    #[error("requested resources are temporarily unavailable")]
    TemporarilyUnavailable,
    /// Request exceeds configured capacity or requires unknown resources.
    #[error("requested resources cannot be satisfied by this engine")]
    Unsatisfiable,
    /// Engine cannot accept new task execution.
    #[error("task execution engine is shut down")]
    Closed,
}

/// Resource-aware execution engine extension point.
///
/// Implementations reserve all requested resources before changing the task to
/// `Running` and keep the reservation until the execution attempt exits.
///
/// # Examples
///
/// ```
/// use qubit_task::engine::{LocalTaskExecutionEngine, TaskExecutionEngine};
/// use qubit_task::model::ResourceCapacity;
///
/// let engine = LocalTaskExecutionEngine::new(ResourceCapacity::default());
/// assert_eq!(engine.capacity().capacity.cpu_slots, 0);
/// ```
pub trait TaskExecutionEngine: Send + Sync {
    /// Reports configured capacity and current reservations.
    fn capacity(&self) -> ResourceSnapshot;
    /// Atomically reserves all task resources without starting handler code.
    fn prepare<'a>(
        &'a self,
        id: TaskId,
        request: ResourceRequest,
    ) -> TaskFuture<'a, Result<PreparedExecution, EngineError>>;
    /// Starts a prepared task attempt and releases its reservation on exit.
    fn activate<'a>(
        &'a self,
        prepared: PreparedExecution,
        handler: Arc<dyn TaskHandler>,
        payload: Vec<u8>,
        context: TaskContext,
    ) -> TaskFuture<'a, Result<ExecutionHandle, EngineError>>;
}

/// Factory contract used by the application or SPI assembly.
pub trait TaskExecutionEngineProvider: Send + Sync {
    /// Creates an engine with the supplied capacity.
    fn create(&self, capacity: ResourceCapacity) -> Result<Arc<dyn TaskExecutionEngine>, String>;
}
