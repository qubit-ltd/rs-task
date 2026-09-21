use std::future::IntoFuture;

use qubit_executor::TaskHandle as ExecutorTaskHandle;
use qubit_executor::TaskResult;
use qubit_executor::TryGet;
use qubit_executor::task::TaskHandleFuture;

use super::Id;

/// Public handle for a task submitted to [`super::TaskExecutionService`].
///
/// The handle keeps the caller-provided [`Id`] while hiding executor-specific
/// implementation details. It can be consumed for a blocking result, polled
/// without blocking, or awaited as a future.
#[must_use = "a task handle must be consumed or retained to observe its result"]
pub struct TaskHandle<R, E> {
    task_id: Id,
    executor: ExecutorTaskHandle<R, E>,
}

impl<R, E> TaskHandle<R, E> {
    pub(crate) fn new(task_id: Id, executor: ExecutorTaskHandle<R, E>) -> Self {
        Self { task_id, executor }
    }

    /// Returns the caller-provided task identifier.
    #[inline]
    pub fn task_id(&self) -> Id {
        self.task_id
    }

    /// Waits for and returns the task result.
    #[inline]
    pub fn get(self) -> TaskResult<R, E> {
        self.executor.get()
    }

    /// Attempts to retrieve the result without blocking.
    #[inline]
    pub fn try_get(self) -> TryGet<Self, R, E> {
        match self.executor.try_get() {
            TryGet::Ready(result) => TryGet::Ready(result),
            TryGet::Pending(executor) => TryGet::Pending(Self::new(self.task_id, executor)),
        }
    }

    /// Returns whether the task reached a terminal state.
    #[inline]
    pub fn is_done(&self) -> bool {
        self.executor.is_done()
    }
}

impl<R, E> IntoFuture for TaskHandle<R, E> {
    type Output = TaskResult<R, E>;
    type IntoFuture = TaskHandleFuture<R, E>;

    #[inline]
    fn into_future(self) -> Self::IntoFuture {
        self.executor.into_future()
    }
}
