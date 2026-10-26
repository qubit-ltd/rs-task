// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use super::StoredTaskRequest;
use super::TaskSummary;

/// Encoded task request and payload-free lifecycle view loaded from a store.
///
/// A `StoredTask` value is returned by [`TaskStore::get_encoded_task`] when the
/// store contains the requested task. Its fields provide the encoded request
/// needed for recovery and the lifecycle summary used for queries. The task
/// store supplies this value when an application loads a retained task.
///
/// # Examples
///
/// ```
/// use qubit_task::model::{StoredTask, TaskId};
/// use qubit_task::store::{StoreError, TaskStore};
///
/// async fn load_stored_task(
///     store: &dyn TaskStore,
///     id: TaskId,
/// ) -> Result<Option<StoredTask>, StoreError> {
///     let task = store.get_encoded_task(id).await?;
///     if let Some(task) = &task {
///         println!("{}: {:?}", task.request.kind_id, task.summary.state);
///     }
///     Ok(task)
/// }
/// ```
///
/// [`TaskStore::get_encoded_task`]: crate::store::TaskStore::get_encoded_task
#[derive(Debug, Clone, PartialEq)]
pub struct StoredTask {
    /// Encoded request required to recover handler execution.
    pub request: StoredTaskRequest,
    /// Current queryable lifecycle summary.
    pub summary: TaskSummary,
}
