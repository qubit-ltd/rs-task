// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
// qubit-style: allow multiple-public-types
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::panic::resume_unwind;
use std::sync::Arc;

use qubit_executor::service::ExecutorService;
use qubit_executor::service::ExecutorServiceBuilderError;
use qubit_executor::service::StopReport;
use qubit_executor::task::spi::TaskEndpointPair;
use qubit_executor::task::spi::TaskSlotCell;
use qubit_function::Callable;
use qubit_function::Runnable;
use qubit_id::Id;
use qubit_thread_pool::PoolJob;
use qubit_thread_pool::ThreadPool;

use super::task_execution_service_builder::TaskExecutionServiceBuilder;
use super::task_execution_service_error::TaskExecutionServiceError;
use super::task_execution_service_state::CancelFn;
use super::task_execution_service_state::SubmissionToken;
use super::task_execution_service_state::TaskExecutionServiceState;
use super::task_execution_stats::TaskExecutionStats;
use super::task_handle::TaskHandle;
use super::task_status::TaskStatus;

/// Managed task execution service built on [`ThreadPool`].
///
/// Accepts a caller-provided business [`Id`] per task and tracks
/// service-level status (submitted, running, succeeded, failed, cancelled,
/// panicked). The typed task outcome is still retrieved through [`TaskHandle`].
///
/// # Responsibilities
///
/// - **Registry**: The same [`Id`] cannot be submitted again while its task is
///   active or being submitted; a duplicate returns
///   [`TaskExecutionServiceError::DuplicateTask`]. Use this when you need
///   lookup by ID or optional pre-start cancellation. Terminal statuses are
///   retained only up to the builder's history capacity (1024 by default). A
///   terminal ID can be reused; a new submission replaces its old status.
/// - **Thread pool**: Owns a [`ThreadPool`] for queuing and worker threads;
///   queue internals are not exposed. Configure the pool via
///   [`TaskExecutionServiceBuilder`] or [`Self::builder`].
/// - **Submission semantics**: [`Self::submit`] / [`Self::submit_callable`]
///   returning `Ok(handle)` means only that the **service accepted** the
///   task—not that it started or succeeded. Observe the final result with
///   [`TaskHandle::get`] or by awaiting the handle’s
///   [`Future`](std::future::Future) implementation.
///
/// # Suspend
///
/// [`Self::suspend`] rejects **new** submissions
/// ([`TaskExecutionServiceError::Suspended`]). Tasks already queued or running
/// are unaffected. [`Self::resume`] re-enables submission.
///
/// # Cancel
///
/// [`Self::cancel`] may succeed only **before** the task starts running; once
/// running, cancellation behavior follows [`TaskHandle`] and the internal
/// completion protocol.
///
/// # Shutdown
///
/// [`Self::shutdown`] and [`Self::stop`] delegate to the backing pool.
/// [`Self::wait_termination`] blocks the current thread until all accepted work
/// has completed, failed, panicked, or been cancelled.
///
/// # Example: submit, inspect status, wait for idle, shutdown
///
/// ```
/// use std::error::Error;
/// use qubit_id::Id;
/// use qubit_task::service::{TaskExecutionService, TaskStatus};
///
/// fn main() -> Result<(), Box<dyn Error>> {
///     let service = TaskExecutionService::new()?;
///     let id: Id = Id::new(1001);
///
///     let handle = service.submit(id, || Ok::<(), ()>(()))?;
///     handle.get().unwrap();
///
///     assert_eq!(service.status(id), Some(TaskStatus::Succeeded));
///
///     service.wait_for_idle();
///     service.shutdown();
///     Ok(())
/// }
/// ```
#[must_use = "a task execution service must be retained to submit and observe tasks"]
pub struct TaskExecutionService {
    /// Backing pool that accepts and runs submitted tasks.
    pool: ThreadPool,
    /// Registry containing task lifecycle state and bounded terminal history.
    state: Arc<TaskExecutionServiceState>,
}

impl TaskExecutionService {
    /// Creates a service using the default
    /// [`qubit_thread_pool::ThreadPoolBuilder`] settings (worker counts,
    /// queue, and other defaults match [`ThreadPool::builder`]).
    ///
    /// # Example
    ///
    /// ```
    /// use qubit_executor::service::ExecutorServiceBuilderError;
    /// use qubit_task::service::TaskExecutionService;
    ///
    /// fn main() -> Result<(), ExecutorServiceBuilderError> {
    ///     let _service = TaskExecutionService::new()?;
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Returns
    ///
    /// `Ok(Self)` on success, or [`ExecutorServiceBuilderError`] if the pool
    /// cannot be built.
    pub fn new() -> Result<Self, ExecutorServiceBuilderError> {
        Self::builder().build()
    }

    /// Returns a [`TaskExecutionServiceBuilder`] so you can tune the backing
    /// pool before [`TaskExecutionServiceBuilder::build`] (for example
    /// [`qubit_thread_pool::ThreadPoolBuilder::pool_size`],
    /// [`qubit_thread_pool::ThreadPoolBuilder::queue_capacity`]).
    ///
    /// # Example
    ///
    /// ```
    /// use qubit_executor::service::ExecutorServiceBuilderError;
    /// use qubit_task::service::TaskExecutionService;
    /// use qubit_thread_pool::ThreadPoolBuilder;
    ///
    /// fn main() -> Result<(), ExecutorServiceBuilderError> {
    ///     let _service = TaskExecutionService::builder()
    ///         .thread_pool(ThreadPoolBuilder::default().pool_size(8))
    ///         .build()?;
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Returns
    ///
    /// A builder holding the default [`qubit_thread_pool::ThreadPoolBuilder`].
    #[must_use]
    #[inline]
    pub fn builder() -> TaskExecutionServiceBuilder {
        TaskExecutionServiceBuilder::default()
    }

    /// Builds a service from an already constructed pool and history policy.
    ///
    /// The pool is assumed to be configured by the caller; this constructor
    /// only creates the service registry around it.
    ///
    /// # Parameters
    ///
    /// * `pool` - Already configured pool owned by the new service.
    /// * `history_capacity` - Maximum number of terminal statuses to retain.
    ///
    /// # Returns
    ///
    /// A service using the supplied pool and history policy.
    pub(crate) fn from_thread_pool(pool: ThreadPool, history_capacity: usize) -> Self {
        Self {
            pool,
            state: Arc::new(TaskExecutionServiceState::new(history_capacity)),
        }
    }

    /// Submits a runnable task with a business task ID.
    ///
    /// # Example
    ///
    /// ```
    /// use std::error::Error;
    /// use qubit_id::Id;
    /// use qubit_task::service::TaskExecutionService;
    ///
    /// fn main() -> Result<(), Box<dyn Error>> {
    ///     let service = TaskExecutionService::new()?;
    ///     let handle = service.submit(Id::new(42), || Ok::<(), ()>(()))?;
    ///     handle.get().unwrap();
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Parameters
    ///
    /// * `task_id` - Caller-provided business ID, unique among active tasks.
    /// * `task` - Runnable to execute.
    ///
    /// # Type Parameters
    ///
    /// * `T` - Runnable task type.
    /// * `E` - Error type returned by the runnable.
    ///
    /// # Returns
    ///
    /// `Ok(handle)` if the service accepts the task. This only means
    /// acceptance; task success is observed through the handle. Returns
    /// [`TaskExecutionServiceError`] when the ID is duplicated, the service is
    /// suspended, or the backing pool rejects the task.
    #[inline]
    pub fn submit<T, E>(&self, task_id: Id, mut task: T) -> Result<TaskHandle<(), E>, TaskExecutionServiceError>
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
    /// use qubit_id::Id;
    /// use qubit_task::service::TaskExecutionService;
    ///
    /// fn main() -> Result<(), Box<dyn Error>> {
    ///     let service = TaskExecutionService::new()?;
    ///     let id: Id = Id::new(7);
    ///     let handle = service.submit_callable(id, || Ok::<i32, ()>(21))?;
    ///     assert_eq!(handle.get().unwrap(), 21);
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Parameters
    ///
    /// * `task_id` - Caller-provided business ID, unique among active tasks.
    /// * `task` - Callable to execute.
    ///
    /// # Returns
    ///
    /// `Ok(handle)` if the service accepts the task. The handle reports the
    /// typed task result while this service records only service-level status.
    ///
    /// # Type Parameters
    ///
    /// * `C` - Callable task type.
    /// * `R` - Successful result type.
    /// * `E` - Error type returned by the callable.
    pub fn submit_callable<C, R, E>(&self, task_id: Id, task: C) -> Result<TaskHandle<R, E>, TaskExecutionServiceError>
    where
        C: Callable<R, E> + Send + 'static,
        R: Send + 'static,
        E: Send + 'static,
    {
        let (handle, slot) = TaskEndpointPair::new().into_parts();
        let slot = Arc::new(TaskSlotCell::new(slot));
        let accept_slot = Arc::clone(&slot);
        let cancel_slot = Arc::clone(&slot);
        let cancel: CancelFn = Arc::new(move || cancel_slot.cancel_unstarted());
        let token = self.state.reserve(task_id, cancel)?;

        let accept_state = Arc::clone(&self.state);
        let accept_token = token.clone();
        let run_slot = Arc::clone(&slot);
        let run_state = Arc::clone(&self.state);
        let run_token = token.clone();
        let stop_slot = Arc::clone(&slot);
        let stop_state = Arc::clone(&self.state);
        let stop_token = token.clone();
        let job = PoolJob::with_accept(
            Box::new(move || {
                accept_slot.accept();
                let _ = accept_state.accept(task_id, &accept_token);
            }),
            Box::new(move || {
                let slot = run_slot.take();
                if let Some(slot) = slot {
                    let task = StatusReportingTask {
                        task_id,
                        task,
                        state: run_state,
                        token: run_token,
                    };
                    let _ran = slot.run(task);
                }
            }),
            Box::new(move || {
                if stop_slot.cancel_unstarted() {
                    let _ = stop_state.finish(task_id, &stop_token, TaskStatus::Cancelled);
                }
            }),
        );

        if let Err(error) = self.pool.submit_job(job) {
            let _ = self.state.discard(task_id, &token);
            return Err(error.into());
        }
        Ok(TaskHandle::new(task_id, handle))
    }

    /// Attempts to cancel a submitted task by ID.
    ///
    /// Cancellation succeeds only before the task starts running.
    ///
    /// # Example
    ///
    /// ```
    /// use std::error::Error;
    /// use qubit_id::Id;
    /// use qubit_task::service::TaskExecutionService;
    ///
    /// fn main() -> Result<(), Box<dyn Error>> {
    ///     let service = TaskExecutionService::new()?;
    ///     let id: Id = Id::new(1);
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
    #[must_use]
    pub fn cancel(&self, task_id: Id) -> bool {
        let Some((token, cancel)) = self.state.cancel_candidate(task_id) else {
            return false;
        };
        if cancel() {
            let _ = self.state.finish(task_id, &token, TaskStatus::Cancelled);
            true
        } else {
            false
        }
    }

    /// Returns the current status of a task.
    ///
    /// # Example
    ///
    /// ```
    /// use std::error::Error;
    /// use qubit_id::Id;
    /// use qubit_task::service::{TaskExecutionService, TaskStatus};
    ///
    /// fn main() -> Result<(), Box<dyn Error>> {
    ///     let service = TaskExecutionService::new()?;
    ///     let id: Id = Id::new(10);
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
    /// `Some(status)` for an accepted active or retained terminal task. Returns
    /// `None` for an unaccepted reservation, unknown ID, or evicted terminal
    /// record. A new task with the same ID replaces the old terminal status.
    /// Cancellation may publish a handle result just before this registry is
    /// updated; handle and service observations are not an atomic pair.
    #[must_use]
    #[inline]
    pub fn status(&self, task_id: Id) -> Option<TaskStatus> {
        self.state.status(task_id)
    }

    /// Returns registry-derived task statistics.
    ///
    /// # Example
    ///
    /// ```
    /// use std::error::Error;
    /// use qubit_id::Id;
    /// use qubit_task::service::TaskExecutionService;
    ///
    /// fn main() -> Result<(), Box<dyn Error>> {
    ///     let service = TaskExecutionService::new()?;
    ///     let handle = service.submit(Id::new(1), || Ok::<(), ()>(()))?;
    ///     handle.get().unwrap();
    ///     let snapshot = service.stats();
    ///     assert!(snapshot.total >= 1);
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Returns
    ///
    /// A snapshot of accepted active and retained terminal tasks grouped by
    /// status. `total` is the sum of these visible records, not a lifetime
    /// submission counter; unaccepted reservations are excluded.
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
    /// use qubit_executor::service::ExecutorServiceBuilderError;
    /// use qubit_task::service::TaskExecutionService;
    ///
    /// fn main() -> Result<(), ExecutorServiceBuilderError> {
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
    #[must_use]
    #[inline]
    pub fn is_suspended(&self) -> bool {
        self.state.is_suspended()
    }

    /// Waits for the submission identity snapshot observed at call time to
    /// leave the active registry.
    ///
    /// A later submission reusing an ID does not extend this snapshot. This
    /// method blocks the current thread and does not guarantee that a task
    /// handle has finished publishing its result.
    ///
    /// # Example
    ///
    /// ```
    /// use std::error::Error;
    /// use qubit_id::Id;
    /// use qubit_task::service::TaskExecutionService;
    ///
    /// fn main() -> Result<(), Box<dyn Error>> {
    ///     let service = TaskExecutionService::new()?;
    ///     let a: Id = Id::new(1);
    ///     let b: Id = Id::new(2);
    ///     let h1 = service.submit(a, || Ok::<(), ()>(()))?;
    ///     let h2 = service.submit(b, || Ok::<(), ()>(()))?;
    ///     service.wait_for_current_tasks();
    ///     h1.get().unwrap();
    ///     h2.get().unwrap();
    ///     Ok(())
    /// }
    /// ```
    pub fn wait_for_current_tasks(&self) {
        self.state.await_in_flight_tasks_completion();
    }

    /// Waits until the service registry has no submitted or running tasks.
    ///
    /// This method blocks until no accepted task or pending reservation is
    /// active. Result publication to a handle may still be in progress.
    ///
    /// # Example
    ///
    /// ```
    /// use std::error::Error;
    /// use qubit_id::Id;
    /// use qubit_task::service::TaskExecutionService;
    ///
    /// fn main() -> Result<(), Box<dyn Error>> {
    ///     let service = TaskExecutionService::new()?;
    ///     let id: Id = Id::new(1);
    ///     let handle = service.submit(id, || Ok::<(), ()>(()))?;
    ///     handle.get().unwrap();
    ///     service.wait_for_idle();
    ///     Ok(())
    /// }
    /// ```
    pub fn wait_for_idle(&self) {
        self.state.await_idle();
    }

    /// Initiates graceful shutdown of the backing pool.
    ///
    /// # Example
    ///
    /// ```
    /// use qubit_executor::service::ExecutorServiceBuilderError;
    /// use qubit_task::service::TaskExecutionService;
    ///
    /// fn main() -> Result<(), ExecutorServiceBuilderError> {
    ///     let service = TaskExecutionService::new()?;
    ///     service.shutdown();
    ///     assert!(service.is_not_running());
    ///     Ok(())
    /// }
    /// ```
    #[inline]
    pub fn shutdown(&self) {
        self.pool.shutdown();
    }

    /// Initiates immediate stop of the backing pool.
    ///
    /// # Example
    ///
    /// ```
    /// use qubit_executor::service::ExecutorServiceBuilderError;
    /// use qubit_task::service::TaskExecutionService;
    ///
    /// fn main() -> Result<(), ExecutorServiceBuilderError> {
    ///     let service = TaskExecutionService::new()?;
    ///     let _report = service.stop();
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Returns
    ///
    /// A count-based report from the backing pool.
    #[must_use]
    #[inline]
    pub fn stop(&self) -> StopReport {
        self.pool.stop()
    }

    /// Returns whether the backing pool no longer accepts new work.
    #[must_use]
    #[inline]
    pub fn is_not_running(&self) -> bool {
        self.pool.is_not_running()
    }

    /// Returns whether the backing pool has terminated.
    #[must_use]
    #[inline]
    pub fn is_terminated(&self) -> bool {
        self.pool.is_terminated()
    }

    /// Blocks until the backing pool has terminated.
    ///
    /// # Example
    ///
    /// ```
    /// use qubit_executor::service::ExecutorServiceBuilderError;
    /// use qubit_task::service::TaskExecutionService;
    ///
    /// fn main() -> Result<(), ExecutorServiceBuilderError> {
    ///     let service = TaskExecutionService::new()?;
    ///     service.shutdown();
    ///     service.wait_termination();
    ///     assert!(service.is_terminated());
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Returns
    ///
    /// Returns after shutdown and worker exit.
    #[inline]
    pub fn wait_termination(&self) {
        self.pool.wait_termination();
    }

    /// Returns the backing thread pool.
    ///
    /// # Example
    ///
    /// ```
    /// use qubit_executor::service::ExecutorServiceBuilderError;
    /// use qubit_task::service::TaskExecutionService;
    ///
    /// fn main() -> Result<(), ExecutorServiceBuilderError> {
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
    #[must_use]
    #[inline]
    pub fn thread_pool(&self) -> &ThreadPool {
        &self.pool
    }
}

/// Callable wrapper that keeps service-level status aligned with task outcome.
///
/// # Type Parameters
///
/// * `C` - User callable whose outcome is reported to the service registry.
struct StatusReportingTask<C> {
    /// Stable business task ID.
    task_id: Id,
    /// User task to execute.
    task: C,
    /// Shared service registry.
    state: Arc<TaskExecutionServiceState>,
    /// Identity of this submission, protecting reused business IDs.
    token: SubmissionToken,
}

impl<C, R, E> Callable<R, E> for StatusReportingTask<C>
where
    C: Callable<R, E>,
{
    /// Runs the user task and records the corresponding service-level status.
    ///
    /// # Returns
    ///
    /// The user's successful value or error. A panic is recorded as
    /// [`TaskStatus::Panicked`] and then resumed on the worker thread.
    fn call(&mut self) -> Result<R, E> {
        let _ = self.state.start(self.task_id, &self.token);
        match catch_unwind(AssertUnwindSafe(|| self.task.call())) {
            Ok(Ok(value)) => {
                let _ = self.state.finish(self.task_id, &self.token, TaskStatus::Succeeded);
                Ok(value)
            }
            Ok(Err(error)) => {
                let _ = self.state.finish(self.task_id, &self.token, TaskStatus::Failed);
                Err(error)
            }
            Err(payload) => {
                let _ = self.state.finish(self.task_id, &self.token, TaskStatus::Panicked);
                resume_unwind(payload);
            }
        }
    }
}
