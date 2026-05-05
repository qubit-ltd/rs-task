/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
use std::{
    collections::HashMap,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Condvar, Mutex, MutexGuard},
};

use qubit_function::{Callable, Runnable};

use qubit_executor::{
    TaskCompletion,
    TaskCompletionPair,
    TaskExecutionError,
    TaskHandle,
};
use qubit_executor::service::{ExecutorService, ShutdownReport};
use qubit_thread_pool::{PoolJob, ThreadPool, ThreadPoolBuildError};

use super::{
    task_execution_service_builder::TaskExecutionServiceBuilder,
    task_execution_service_error::TaskExecutionServiceError,
    task_execution_stats::TaskExecutionStats, task_id::TaskId, task_status::TaskStatus,
};

/// Managed task execution service built on [`ThreadPool`].
///
/// Assigns a stable business [`TaskId`] per task and tracks service-level status
/// (submitted, running, succeeded, failed, cancelled, panicked). The typed task
/// outcome is still retrieved through [`TaskHandle`].
///
/// # Responsibilities
///
/// - **Registry**: The same [`TaskId`] cannot be submitted again while a record
///   for it exists; a duplicate returns [`TaskExecutionServiceError::DuplicateTask`].
///   Use this when you need lookup by ID, optional pre-start cancellation, or
///   long-lived task bookkeeping.
/// - **Thread pool**: Owns a [`ThreadPool`] for queuing and worker threads; queue
///   internals are not exposed. Configure the pool via [`TaskExecutionServiceBuilder`]
///   or [`Self::builder`].
/// - **Submission semantics**: [`Self::submit`] / [`Self::submit_callable`] returning
///   `Ok(handle)` means only that the **service accepted** the task—not that it
///   started or succeeded. Observe the final result with [`TaskHandle::get`] or by
///   awaiting the handle’s [`Future`](std::future::Future) implementation.
///
/// # Suspend
///
/// [`Self::suspend`] rejects **new** submissions ([`TaskExecutionServiceError::Suspended`]).
/// Tasks already queued or running are unaffected. [`Self::resume`] re-enables submission.
///
/// # Cancel
///
/// [`Self::cancel`] may succeed only **before** the task starts running; once running,
/// cancellation behavior follows [`TaskHandle`] and the internal completion protocol.
///
/// # Shutdown
///
/// [`Self::shutdown`] and [`Self::shutdown_now`] delegate to the backing pool.
/// [`Self::await_termination`] returns a [`Future`](std::future::Future) that must be
/// driven in an async context (`.await`) or on a suitable runtime (for example
/// `tokio::runtime::Handle::current().block_on` from blocking code).
///
/// # Example: submit, inspect status, wait for idle, shutdown
///
/// ```
/// use std::error::Error;
/// use qubit_task::service::{TaskExecutionService, TaskId, TaskStatus};
///
/// fn main() -> Result<(), Box<dyn Error>> {
///     let service = TaskExecutionService::new()?;
///     let id: TaskId = 1001;
///
///     let handle = service.submit(id, || Ok::<(), ()>(()))?;
///     handle.get().unwrap();
///
///     assert_eq!(service.status(id), Some(TaskStatus::Succeeded));
///
///     service.await_idle();
///     service.shutdown();
///     Ok(())
/// }
/// ```
///
pub struct TaskExecutionService {
    pool: ThreadPool,
    state: Arc<TaskExecutionServiceState>,
}

impl TaskExecutionService {
    /// Creates a service using the default [`ThreadPoolBuilder`] settings (worker
    /// counts, queue, and other defaults match [`ThreadPool::builder`]).
    ///
    /// # Example
    ///
    /// ```
    /// use qubit_task::service::{TaskExecutionService, ThreadPoolBuildError};
    ///
    /// fn main() -> Result<(), ThreadPoolBuildError> {
    ///     let _service = TaskExecutionService::new()?;
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Returns
    ///
    /// `Ok(Self)` on success, or [`ThreadPoolBuildError`] if the pool cannot be built.
    pub fn new() -> Result<Self, ThreadPoolBuildError> {
        Self::builder().build()
    }

    /// Returns a [`TaskExecutionServiceBuilder`] so you can tune the backing pool
    /// before [`TaskExecutionServiceBuilder::build`] (for example
    /// [`ThreadPoolBuilder::pool_size`], [`ThreadPoolBuilder::queue_capacity`]).
    ///
    /// # Example
    ///
    /// ```
    /// use qubit_task::service::{
    ///     TaskExecutionService, ThreadPoolBuilder, ThreadPoolBuildError,
    /// };
    ///
    /// fn main() -> Result<(), ThreadPoolBuildError> {
    ///     let _service = TaskExecutionService::builder()
    ///         .thread_pool(ThreadPoolBuilder::default().pool_size(8))
    ///         .build()?;
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Returns
    ///
    /// A builder holding the default [`ThreadPoolBuilder`].
    #[inline]
    pub fn builder() -> TaskExecutionServiceBuilder {
        TaskExecutionServiceBuilder::default()
    }

    /// Builds a service from an already constructed pool.
    pub(crate) fn from_thread_pool(pool: ThreadPool) -> Self {
        Self {
            pool,
            state: Arc::new(TaskExecutionServiceState::default()),
        }
    }

    /// Submits a runnable task with a business task ID.
    ///
    /// # Example
    ///
    /// ```
    /// use std::error::Error;
    /// use qubit_task::service::TaskExecutionService;
    ///
    /// fn main() -> Result<(), Box<dyn Error>> {
    ///     let service = TaskExecutionService::new()?;
    ///     let handle = service.submit(42_u64, || Ok::<(), ()>(()))?;
    ///     handle.get().unwrap();
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Parameters
    ///
    /// * `task_id` - Stable business ID for registry operations.
    /// * `task` - Runnable to execute.
    ///
    /// # Returns
    ///
    /// `Ok(handle)` if the service accepts the task. This only means
    /// acceptance; task success is observed through the handle. Returns
    /// [`TaskExecutionServiceError`] when the ID is duplicated, the service is
    /// suspended, or the backing pool rejects the task.
    #[inline]
    pub fn submit<T, E>(
        &self,
        task_id: TaskId,
        mut task: T,
    ) -> Result<TaskHandle<(), E>, TaskExecutionServiceError>
    where
        T: Runnable<E> + Send + 'static,
        E: Send + 'static,
    {
        self.submit_callable(task_id, move || task.run())
    }

    /// Submits a callable task with a business task ID.
    ///
    /// # Example
    ///
    /// ```
    /// use std::error::Error;
    /// use qubit_task::service::{TaskExecutionService, TaskId};
    ///
    /// fn main() -> Result<(), Box<dyn Error>> {
    ///     let service = TaskExecutionService::new()?;
    ///     let id: TaskId = 7;
    ///     let handle = service.submit_callable(id, || Ok::<i32, ()>(21))?;
    ///     assert_eq!(handle.get().unwrap(), 21);
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Parameters
    ///
    /// * `task_id` - Stable business ID for registry operations.
    /// * `task` - Callable to execute.
    ///
    /// # Returns
    ///
    /// `Ok(handle)` if the service accepts the task. The handle reports the
    /// typed task result while this service records only service-level status.
    pub fn submit_callable<C, R, E>(
        &self,
        task_id: TaskId,
        task: C,
    ) -> Result<TaskHandle<R, E>, TaskExecutionServiceError>
    where
        C: Callable<R, E> + Send + 'static,
        R: Send + 'static,
        E: Send + 'static,
    {
        let (handle, completion) = TaskCompletionPair::new().into_parts();
        let cancel_completion = completion.clone();
        let cancel_state = Arc::clone(&self.state);
        let cancel: Arc<dyn Fn() -> bool + Send + Sync> = Arc::new(move || {
            let cancelled = cancel_completion.cancel();
            if cancelled {
                cancel_state.set_status(task_id, TaskStatus::Cancelled);
            }
            cancelled
        });

        self.state.register(task_id, Arc::clone(&cancel))?;

        let run_state = Arc::clone(&self.state);
        let job = PoolJob::new(
            Box::new(move || run_tracked_task(task_id, task, completion, run_state)),
            Box::new(move || {
                cancel();
            }),
        );

        if let Err(error) = self.pool.submit_job(job) {
            self.state.remove(task_id);
            return Err(error.into());
        }
        Ok(handle)
    }

    /// Attempts to cancel a submitted task by ID.
    ///
    /// Cancellation succeeds only before the task starts running.
    ///
    /// # Example
    ///
    /// ```
    /// use std::error::Error;
    /// use qubit_task::service::{TaskExecutionService, TaskId};
    ///
    /// fn main() -> Result<(), Box<dyn Error>> {
    ///     let service = TaskExecutionService::new()?;
    ///     let id: TaskId = 1;
    ///     let handle = service.submit(id, || Ok::<(), ()>(()))?;
    ///     // `true` only if cancelled before a worker starts the task (race with the pool).
    ///     let _cancelled = service.cancel(id);
    ///     match handle.get() {
    ///         Ok(()) => {}
    ///         Err(e) if e.is_cancelled() => {}
    ///         Err(e) => panic!("unexpected task outcome: {e:?}"),
    ///     }
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Parameters
    ///
    /// * `task_id` - ID of the task to cancel.
    ///
    /// # Returns
    ///
    /// `true` if the task was cancelled before start, or `false` if no active
    /// task with this ID can be cancelled.
    pub fn cancel(&self, task_id: TaskId) -> bool {
        let cancel = self.state.cancel_callback(task_id);
        cancel.is_some_and(|cancel| cancel())
    }

    /// Returns the current status of a task.
    ///
    /// # Example
    ///
    /// ```
    /// use std::error::Error;
    /// use qubit_task::service::{TaskExecutionService, TaskId, TaskStatus};
    ///
    /// fn main() -> Result<(), Box<dyn Error>> {
    ///     let service = TaskExecutionService::new()?;
    ///     let id: TaskId = 10;
    ///     let handle = service.submit(id, || Ok::<(), ()>(()))?;
    ///     handle.get().unwrap();
    ///     assert_eq!(service.status(id), Some(TaskStatus::Succeeded));
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Parameters
    ///
    /// * `task_id` - ID of the task to inspect.
    ///
    /// # Returns
    ///
    /// `Some(status)` if the service retains a record for this ID, or `None`
    /// if the ID is unknown.
    #[inline]
    pub fn status(&self, task_id: TaskId) -> Option<TaskStatus> {
        self.state.status(task_id)
    }

    /// Returns registry-derived task statistics.
    ///
    /// # Example
    ///
    /// ```
    /// use std::error::Error;
    /// use qubit_task::service::TaskExecutionService;
    ///
    /// fn main() -> Result<(), Box<dyn Error>> {
    ///     let service = TaskExecutionService::new()?;
    ///     let handle = service.submit(1_u64, || Ok::<(), ()>(()))?;
    ///     handle.get().unwrap();
    ///     let snapshot = service.stats();
    ///     assert!(snapshot.total >= 1);
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Returns
    ///
    /// A snapshot of retained task records grouped by status.
    #[inline]
    pub fn stats(&self) -> TaskExecutionStats {
        self.state.stats()
    }

    /// Suspends new submissions.
    ///
    /// Existing submitted and running tasks continue normally.
    ///
    /// # Example
    ///
    /// ```
    /// use qubit_task::service::{TaskExecutionService, ThreadPoolBuildError};
    ///
    /// fn main() -> Result<(), ThreadPoolBuildError> {
    ///     let service = TaskExecutionService::new()?;
    ///     service.suspend();
    ///     assert!(service.is_suspended());
    ///     service.resume();
    ///     assert!(!service.is_suspended());
    ///     Ok(())
    /// }
    /// ```
    #[inline]
    pub fn suspend(&self) {
        self.state.set_suspended(true);
    }

    /// Resumes accepting new submissions.
    #[inline]
    pub fn resume(&self) {
        self.state.set_suspended(false);
    }

    /// Returns whether the service is suspended.
    ///
    /// # Returns
    ///
    /// `true` if new submissions are rejected before reaching the pool.
    #[inline]
    pub fn is_suspended(&self) -> bool {
        self.state.is_suspended()
    }

    /// Waits for the active task snapshot observed at call time to finish.
    ///
    /// Tasks submitted after this method starts are not part of the waited
    /// snapshot. This method blocks the current thread.
    ///
    /// # Example
    ///
    /// ```
    /// use std::error::Error;
    /// use qubit_task::service::{TaskExecutionService, TaskId};
    ///
    /// fn main() -> Result<(), Box<dyn Error>> {
    ///     let service = TaskExecutionService::new()?;
    ///     let a: TaskId = 1;
    ///     let b: TaskId = 2;
    ///     let h1 = service.submit(a, || Ok::<(), ()>(()))?;
    ///     let h2 = service.submit(b, || Ok::<(), ()>(()))?;
    ///     service.await_in_flight_tasks_completion();
    ///     h1.get().unwrap();
    ///     h2.get().unwrap();
    ///     Ok(())
    /// }
    /// ```
    pub fn await_in_flight_tasks_completion(&self) {
        self.state.await_in_flight_tasks_completion();
    }

    /// Waits until the service registry has no submitted or running tasks.
    ///
    /// This method blocks the current thread and observes real-time idleness.
    ///
    /// # Example
    ///
    /// ```
    /// use std::error::Error;
    /// use qubit_task::service::{TaskExecutionService, TaskId};
    ///
    /// fn main() -> Result<(), Box<dyn Error>> {
    ///     let service = TaskExecutionService::new()?;
    ///     let id: TaskId = 1;
    ///     let handle = service.submit(id, || Ok::<(), ()>(()))?;
    ///     handle.get().unwrap();
    ///     service.await_idle();
    ///     Ok(())
    /// }
    /// ```
    pub fn await_idle(&self) {
        self.state.await_idle();
    }

    /// Initiates graceful shutdown of the backing pool.
    ///
    /// # Example
    ///
    /// ```
    /// use qubit_task::service::{TaskExecutionService, ThreadPoolBuildError};
    ///
    /// fn main() -> Result<(), ThreadPoolBuildError> {
    ///     let service = TaskExecutionService::new()?;
    ///     service.shutdown();
    ///     assert!(service.is_shutdown());
    ///     Ok(())
    /// }
    /// ```
    #[inline]
    pub fn shutdown(&self) {
        self.pool.shutdown();
    }

    /// Initiates immediate shutdown of the backing pool.
    ///
    /// # Example
    ///
    /// ```
    /// use qubit_task::service::{TaskExecutionService, ThreadPoolBuildError};
    ///
    /// fn main() -> Result<(), ThreadPoolBuildError> {
    ///     let service = TaskExecutionService::new()?;
    ///     let _report = service.shutdown_now();
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Returns
    ///
    /// A count-based report from the backing pool.
    #[inline]
    pub fn shutdown_now(&self) -> ShutdownReport {
        self.pool.shutdown_now()
    }

    /// Returns whether the backing pool has begun shutdown.
    #[inline]
    pub fn is_shutdown(&self) -> bool {
        self.pool.is_shutdown()
    }

    /// Returns whether the backing pool has terminated.
    #[inline]
    pub fn is_terminated(&self) -> bool {
        self.pool.is_terminated()
    }

    /// Waits until the backing pool has terminated.
    ///
    /// # Example
    ///
    /// ```
    /// use std::error::Error;
    /// use qubit_task::service::TaskExecutionService;
    ///
    /// fn main() -> Result<(), Box<dyn Error>> {
    ///     let rt = tokio::runtime::Builder::new_current_thread()
    ///         .enable_all()
    ///         .build()?;
    ///     let service = TaskExecutionService::new()?;
    ///     service.shutdown();
    ///     rt.block_on(async {
    ///         service.await_termination().await;
    ///     });
    ///     assert!(service.is_terminated());
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Returns
    ///
    /// A future that completes after shutdown and worker exit.
    #[inline]
    pub fn await_termination(&self) -> <ThreadPool as ExecutorService>::Termination<'_> {
        self.pool.await_termination()
    }

    /// Returns the backing thread pool.
    ///
    /// # Example
    ///
    /// ```
    /// use qubit_task::service::{TaskExecutionService, ThreadPoolBuildError};
    ///
    /// fn main() -> Result<(), ThreadPoolBuildError> {
    ///     let service = TaskExecutionService::new()?;
    ///     let pool = service.thread_pool();
    ///     assert!(pool.maximum_pool_size() > 0);
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Returns
    ///
    /// A shared reference for low-level inspection such as pool statistics.
    #[inline]
    pub fn thread_pool(&self) -> &ThreadPool {
        &self.pool
    }
}

/// Shared state for [`TaskExecutionService`].
#[derive(Default)]
struct TaskExecutionServiceState {
    inner: Mutex<TaskExecutionServiceInner>,
    idle: Condvar,
}

impl TaskExecutionServiceState {
    /// Acquires service state while tolerating poisoned locks.
    fn lock_inner(&self) -> MutexGuard<'_, TaskExecutionServiceInner> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Registers a submitted task.
    fn register(
        &self,
        task_id: TaskId,
        cancel: Arc<dyn Fn() -> bool + Send + Sync>,
    ) -> Result<(), TaskExecutionServiceError> {
        let mut inner = self.lock_inner();
        if inner.suspended {
            return Err(TaskExecutionServiceError::Suspended);
        }
        if inner.tasks.contains_key(&task_id) {
            return Err(TaskExecutionServiceError::DuplicateTask(task_id));
        }
        inner.tasks.insert(
            task_id,
            TaskRecord {
                status: TaskStatus::Submitted,
                cancel,
            },
        );
        Ok(())
    }

    /// Removes a task record.
    fn remove(&self, task_id: TaskId) {
        let mut inner = self.lock_inner();
        inner.tasks.remove(&task_id);
        self.idle.notify_all();
    }

    /// Gets a task status.
    fn status(&self, task_id: TaskId) -> Option<TaskStatus> {
        self.lock_inner()
            .tasks
            .get(&task_id)
            .map(|record| record.status)
    }

    /// Gets a task cancel callback if the task is active.
    fn cancel_callback(&self, task_id: TaskId) -> Option<Arc<dyn Fn() -> bool + Send + Sync>> {
        let inner = self.lock_inner();
        let record = inner.tasks.get(&task_id)?;
        record
            .status
            .is_active()
            .then(|| Arc::clone(&record.cancel))
    }

    /// Updates a task status.
    fn set_status(&self, task_id: TaskId, status: TaskStatus) {
        let mut inner = self.lock_inner();
        if let Some(record) = inner.tasks.get_mut(&task_id) {
            record.status = status;
        }
        self.idle.notify_all();
    }

    /// Updates suspended flag.
    fn set_suspended(&self, suspended: bool) {
        self.lock_inner().suspended = suspended;
    }

    /// Returns whether new submissions are suspended.
    fn is_suspended(&self) -> bool {
        self.lock_inner().suspended
    }

    /// Returns task statistics.
    fn stats(&self) -> TaskExecutionStats {
        let inner = self.lock_inner();
        let mut stats = TaskExecutionStats::default();
        for record in inner.tasks.values() {
            stats.add_status(record.status);
        }
        stats
    }

    /// Waits for active task IDs observed at call time.
    fn await_in_flight_tasks_completion(&self) {
        let mut inner = self.lock_inner();
        let task_ids = inner
            .tasks
            .iter()
            .filter_map(|(&task_id, record)| record.status.is_active().then_some(task_id))
            .collect::<Vec<_>>();
        while task_ids
            .iter()
            .any(|task_id| inner.task_is_active(*task_id))
        {
            inner = self
                .idle
                .wait(inner)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }

    /// Waits until no retained task record is active.
    fn await_idle(&self) {
        let mut inner = self.lock_inner();
        while inner.has_active_tasks() {
            inner = self
                .idle
                .wait(inner)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }
}

/// Mutable service state protected by a mutex.
#[derive(Default)]
struct TaskExecutionServiceInner {
    suspended: bool,
    tasks: HashMap<TaskId, TaskRecord>,
}

impl TaskExecutionServiceInner {
    /// Returns whether a retained task ID is still active.
    fn task_is_active(&self, task_id: TaskId) -> bool {
        self.tasks
            .get(&task_id)
            .is_some_and(|record| record.status.is_active())
    }

    /// Returns whether any retained task is still active.
    fn has_active_tasks(&self) -> bool {
        self.tasks.values().any(|record| record.status.is_active())
    }
}

/// Registry record for one managed task.
struct TaskRecord {
    status: TaskStatus,
    cancel: Arc<dyn Fn() -> bool + Send + Sync>,
}

/// Runs a task and updates service status around the typed handle result.
fn run_tracked_task<C, R, E>(
    task_id: TaskId,
    mut task: C,
    completion: TaskCompletion<R, E>,
    state: Arc<TaskExecutionServiceState>,
) where
    C: Callable<R, E>,
{
    if !completion.start() {
        state.set_status(task_id, TaskStatus::Cancelled);
        return;
    }
    state.set_status(task_id, TaskStatus::Running);
    match catch_unwind(AssertUnwindSafe(|| task.call())) {
        Ok(Ok(value)) => {
            state.set_status(task_id, TaskStatus::Succeeded);
            completion.complete(Ok(value));
        }
        Ok(Err(error)) => {
            state.set_status(task_id, TaskStatus::Failed);
            completion.complete(Err(TaskExecutionError::Failed(error)));
        }
        Err(_) => {
            state.set_status(task_id, TaskStatus::Panicked);
            completion.complete(Err(TaskExecutionError::Panicked));
        }
    }
}
