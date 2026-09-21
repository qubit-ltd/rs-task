// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::future::IntoFuture;

use qubit_executor::TaskHandle as ExecutorTaskHandle;
use qubit_executor::TaskResult;
use qubit_executor::TryGet;
use qubit_executor::task::TaskHandleFuture;
use qubit_id::Id;

/// Public handle for a task submitted to [`super::TaskExecutionService`].
///
/// The handle keeps the caller-provided [`Id`] while hiding executor-specific
/// implementation details. It can be consumed for a blocking result, polled
/// without blocking, or awaited as a future.
///
/// # Type Parameters
///
/// * `R` - Successful task result type.
/// * `E` - Task error type.
///
/// # Examples
///
/// ```
/// use qubit_id::Id;
/// use qubit_task::service::TaskExecutionService;
///
/// let service = TaskExecutionService::new().unwrap();
/// let handle = service.submit_callable(Id::new(1), || Ok::<_, ()>(7)).unwrap();
/// assert_eq!(handle.get().unwrap(), 7);
/// service.shutdown();
/// service.wait_termination();
/// ```
#[must_use = "a task handle must be consumed or retained to observe its result"]
pub struct TaskHandle<R, E> {
    /// Caller-provided identifier associated with the task.
    task_id: Id,
    /// Executor handle that owns the typed task result.
    executor: ExecutorTaskHandle<R, E>,
}

impl<R, E> TaskHandle<R, E> {
    /// Wraps an executor handle with its caller-provided task identifier.
    pub(crate) fn new(task_id: Id, executor: ExecutorTaskHandle<R, E>) -> Self {
        Self { task_id, executor }
    }

    /// Returns the caller-provided task identifier.
    ///
    /// The identifier is copied from the submission and does not perform any
    /// registry lookup.
    #[must_use]
    #[inline]
    pub fn task_id(&self) -> Id {
        self.task_id
    }

    /// Waits for and returns the task result.
    ///
    /// # Returns
    ///
    /// The executor's typed success or failure result. This method blocks the
    /// current thread until the task reaches a terminal result.
    #[inline]
    pub fn get(self) -> TaskResult<R, E> {
        self.executor.get()
    }

    /// Attempts to retrieve the result without blocking.
    ///
    /// # Returns
    ///
    /// [`TryGet::Ready`] with the task result when complete, or
    /// [`TryGet::Pending`] with a handle that can be polled or awaited later.
    #[inline]
    pub fn try_get(self) -> TryGet<Self, R, E> {
        match self.executor.try_get() {
            TryGet::Ready(result) => TryGet::Ready(result),
            TryGet::Pending(executor) => TryGet::Pending(Self::new(self.task_id, executor)),
        }
    }

    /// Returns whether the task reached a terminal state.
    ///
    /// # Returns
    ///
    /// `true` after the executor has published a terminal result; otherwise
    /// `false`.
    #[must_use]
    #[inline]
    pub fn is_done(&self) -> bool {
        self.executor.is_done()
    }
}

impl<R, E> IntoFuture for TaskHandle<R, E> {
    type Output = TaskResult<R, E>;
    type IntoFuture = TaskHandleFuture<R, E>;

    /// Converts this handle into the future that resolves to its task result.
    #[inline]
    fn into_future(self) -> Self::IntoFuture {
        self.executor.into_future()
    }
}
