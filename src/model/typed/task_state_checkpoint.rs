// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use super::TaskId;
use crate::model::TaskState;

/// Authoritative current lifecycle state and revision retained for a task.
///
/// Stores return a checkpoint when a task has an authoritative state, including
/// after its history has been pruned. The revision lets callers distinguish
/// successive lifecycle updates.
///
/// # Examples
///
/// Inspect a checkpoint returned by a store:
///
/// ```
/// use qubit_task::TaskId;
/// use qubit_task::store::TaskStore;
///
/// async fn inspect_task_state(store: &impl TaskStore, id: TaskId) {
///     match store.get_state_checkpoint(id).await {
///         Ok(Some(checkpoint)) => {
///             let is_terminal = checkpoint.state.is_terminal();
///             println!(
///                 "state version: {}, terminal: {is_terminal}",
///                 checkpoint.state_version
///             );
///         }
///         Ok(None) => println!("the store has no checkpoint for this task"),
///         Err(error) => eprintln!("failed to read task checkpoint: {error}"),
///     }
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskStateCheckpoint {
    /// Stable identity of the task whose lifecycle is recorded.
    pub id: TaskId,
    /// Monotonically increasing revision of the task lifecycle state.
    pub state_version: u64,
    /// Latest authoritative lifecycle state retained by the store.
    pub state: TaskState,
}
