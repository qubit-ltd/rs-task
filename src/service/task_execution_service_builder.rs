// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use qubit_executor::service::ExecutorServiceBuilderError;
use qubit_thread_pool::ThreadPoolBuilder;

use super::task_execution_service::TaskExecutionService;

/// Builder for [`TaskExecutionService`], used to configure the backing
/// [`qubit_thread_pool::ThreadPool`] before the service is created.
///
/// # Design
///
/// Configuration is delegated to [`ThreadPoolBuilder`] (pool sizes, queue
/// capacity, thread name prefix, and so on). This type exists so future
/// **service-level** options can be added without pushing every thread-pool
/// construction detail onto [`TaskExecutionService`].
///
/// # Relation to [`TaskExecutionService::builder`]
///
/// [`TaskExecutionService::builder`] returns
/// `TaskExecutionServiceBuilder::default()`. For default pool settings you can
/// use [`TaskExecutionService::new`] or `TaskExecutionService::builder().
/// build()`.
///
/// # Example: custom pool, then build the service
///
/// ```
/// use qubit_executor::service::ExecutorServiceBuilderError;
/// use qubit_task::service::TaskExecutionServiceBuilder;
/// use qubit_thread_pool::ThreadPoolBuilder;
///
/// fn main() -> Result<(), ExecutorServiceBuilderError> {
///     let _service = TaskExecutionServiceBuilder::default()
///         .thread_pool(
///             ThreadPoolBuilder::default()
///                 .pool_size(4)
///                 .queue_capacity(256),
///         )
///         .build()?;
///     Ok(())
/// }
/// ```
#[derive(Debug, Clone)]
pub struct TaskExecutionServiceBuilder {
    /// Thread-pool configuration used when the service is built.
    pool_builder: ThreadPoolBuilder,
    /// Maximum number of terminal statuses retained for lookup and stats.
    history_capacity: usize,
}

impl Default for TaskExecutionServiceBuilder {
    /// Uses the default thread pool and retains at most 1024 terminal statuses.
    fn default() -> Self {
        Self {
            pool_builder: ThreadPoolBuilder::default(),
            history_capacity: 1024,
        }
    }
}

impl TaskExecutionServiceBuilder {
    /// Sets the [`ThreadPoolBuilder`] used when [`Self::build`] creates the
    /// pool.
    ///
    /// # Example
    ///
    /// ```
    /// use qubit_executor::service::ExecutorServiceBuilderError;
    /// use qubit_task::service::TaskExecutionServiceBuilder;
    /// use qubit_thread_pool::ThreadPoolBuilder;
    ///
    /// fn main() -> Result<(), ExecutorServiceBuilderError> {
    ///     let _service = TaskExecutionServiceBuilder::default()
    ///         .thread_pool(ThreadPoolBuilder::default().pool_size(2))
    ///         .build()?;
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Parameters
    ///
    /// * `pool_builder` - Builder that produces the backing
    ///   [`qubit_thread_pool::ThreadPool`].
    ///
    /// # Returns
    ///
    /// `self` for fluent configuration.
    #[inline]
    pub fn thread_pool(mut self, pool_builder: ThreadPoolBuilder) -> Self {
        self.pool_builder = pool_builder;
        self
    }

    /// Sets how many recent terminal task statuses the service retains.
    ///
    /// Zero discards each terminal status immediately. This limit does not
    /// affect active tasks or the backing pool's queue capacity.
    ///
    /// # Parameters
    ///
    /// * `capacity` - Maximum number of terminal statuses retained.
    ///
    /// # Returns
    ///
    /// `self` for fluent configuration.
    #[inline]
    pub fn completed_history_capacity(mut self, capacity: usize) -> Self {
        self.history_capacity = capacity;
        self
    }

    /// Builds a [`TaskExecutionService`] from the current configuration.
    ///
    /// # Example
    ///
    /// ```
    /// use qubit_executor::service::ExecutorServiceBuilderError;
    /// use qubit_task::service::TaskExecutionServiceBuilder;
    ///
    /// fn main() -> Result<(), ExecutorServiceBuilderError> {
    ///     let _service = TaskExecutionServiceBuilder::default().build()?;
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Returns
    ///
    /// `Ok(TaskExecutionService)` when [`ThreadPoolBuilder`] settings are valid
    /// and workers start successfully; otherwise
    /// [`ExecutorServiceBuilderError`].
    pub fn build(self) -> Result<TaskExecutionService, ExecutorServiceBuilderError> {
        let pool = self.pool_builder.build()?;
        Ok(TaskExecutionService::from_thread_pool(pool, self.history_capacity))
    }
}
