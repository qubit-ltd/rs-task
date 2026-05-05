/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
use qubit_thread_pool::{
    ThreadPoolBuildError,
    ThreadPoolBuilder,
};

use super::task_execution_service::TaskExecutionService;

/// Builder for [`TaskExecutionService`], used to configure the backing [`super::ThreadPool`]
/// before the service is created.
///
/// # Design
///
/// Configuration is delegated to [`ThreadPoolBuilder`] (pool sizes, queue capacity,
/// thread name prefix, and so on). This type exists so future **service-level** options
/// can be added without pushing every thread-pool construction detail onto
/// [`TaskExecutionService`].
///
/// # Relation to [`TaskExecutionService::builder`]
///
/// [`TaskExecutionService::builder`] returns `TaskExecutionServiceBuilder::default()`.
/// For default pool settings you can use [`TaskExecutionService::new`] or
/// `TaskExecutionService::builder().build()`.
///
/// # Example: custom pool, then build the service
///
/// ```
/// use qubit_task::service::{
///     TaskExecutionServiceBuilder, ThreadPoolBuilder, ThreadPoolBuildError,
/// };
///
/// fn main() -> Result<(), ThreadPoolBuildError> {
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
///
#[derive(Debug, Default, Clone)]
pub struct TaskExecutionServiceBuilder {
    pool_builder: ThreadPoolBuilder,
}

impl TaskExecutionServiceBuilder {
    /// Sets the [`ThreadPoolBuilder`] used when [`Self::build`] creates the pool.
    ///
    /// # Example
    ///
    /// ```
    /// use qubit_task::service::{
    ///     TaskExecutionServiceBuilder, ThreadPoolBuilder, ThreadPoolBuildError,
    /// };
    ///
    /// fn main() -> Result<(), ThreadPoolBuildError> {
    ///     let _service = TaskExecutionServiceBuilder::default()
    ///         .thread_pool(ThreadPoolBuilder::default().pool_size(2))
    ///         .build()?;
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Parameters
    ///
    /// * `pool_builder` - Builder that produces the backing [`super::ThreadPool`].
    ///
    /// # Returns
    ///
    /// `self` for fluent configuration.
    #[inline]
    pub fn thread_pool(mut self, pool_builder: ThreadPoolBuilder) -> Self {
        self.pool_builder = pool_builder;
        self
    }

    /// Builds a [`TaskExecutionService`] from the current configuration.
    ///
    /// # Example
    ///
    /// ```
    /// use qubit_task::service::{TaskExecutionServiceBuilder, ThreadPoolBuildError};
    ///
    /// fn main() -> Result<(), ThreadPoolBuildError> {
    ///     let _service = TaskExecutionServiceBuilder::default().build()?;
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Returns
    ///
    /// `Ok(TaskExecutionService)` when [`ThreadPoolBuilder`] settings are valid and
    /// workers start successfully; otherwise [`ThreadPoolBuildError`].
    pub fn build(self) -> Result<TaskExecutionService, ThreadPoolBuildError> {
        let pool = self.pool_builder.build()?;
        Ok(TaskExecutionService::from_thread_pool(pool))
    }
}
