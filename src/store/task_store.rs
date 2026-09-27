// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::num::NonZeroUsize;

use super::StoreError;
use super::TaskFuture;
use crate::model::AcceptOutcome;
use crate::model::OwnerEpoch;
use crate::model::StoreCapabilities;
use crate::model::StoredTaskPage;
use crate::model::TaskId;
use crate::model::TaskPage;
use crate::model::TaskQuery;
use crate::model::TaskRecord;
use crate::model::TaskRequest;
use crate::model::TaskStateCounts;
use crate::model::TaskSummary;
use crate::model::TransitionCommand;

/// Persistent task history implementation contract.
///
/// Implementations must make acceptance and version-checked transitions
/// atomic. Recoverable stores additionally serialize service ownership and
/// provide bounded scans of unfinished requests.
///
/// # Examples
///
/// ```
/// use qubit_task::store::MemoryTaskStore;
/// use qubit_task::store::TaskStore;
///
/// let store = MemoryTaskStore::new(100);
/// assert!(!store.capabilities().restart_recovery);
/// ```
pub trait TaskStore: Send + Sync {
    /// Reports whether history and unfinished task descriptions survive
    /// restart.
    ///
    /// # Returns
    ///
    /// The persistence and recovery features supported by this store.
    #[must_use]
    fn capabilities(&self) -> StoreCapabilities;

    /// Atomically accepts a task or returns an existing identical idempotent
    /// task.
    ///
    /// # Parameters
    ///
    /// * `id` - Stable identifier to assign if the request is new.
    /// * `request` - Validated task request to persist.
    ///
    /// # Returns
    ///
    /// A future resolving to either the newly accepted task or an identical
    /// existing task.
    ///
    /// # Errors
    ///
    /// Resolves to an error for invalid requests, duplicate IDs, idempotency
    /// conflicts, capacity limits, or persistence failures.
    fn accept<'a>(&'a self, id: TaskId, request: TaskRequest) -> TaskFuture<'a, Result<AcceptOutcome, StoreError>>;

    /// Finds a retained task by its caller-supplied idempotency key without
    /// consuming queue capacity.
    ///
    /// # Parameters
    ///
    /// * `key` - Idempotency key to look up.
    ///
    /// # Returns
    ///
    /// A future resolving to the matching retained task, if any.
    ///
    /// # Errors
    ///
    /// Resolves to an error if the store cannot complete the lookup.
    fn get_by_idempotency_key<'a>(&'a self, key: &'a str) -> TaskFuture<'a, Result<Option<TaskRecord>, StoreError>>;

    /// Applies a lifecycle transition only when its expected revision matches.
    ///
    /// # Parameters
    ///
    /// * `command` - Desired state and expected task revision.
    ///
    /// # Returns
    ///
    /// A future resolving to the updated task summary.
    ///
    /// # Errors
    ///
    /// Resolves to an error when the task is missing, the revision conflicts,
    /// the transition is invalid, or persistence fails.
    fn transition<'a>(&'a self, command: TransitionCommand) -> TaskFuture<'a, Result<TaskSummary, StoreError>>;

    /// Loads lifecycle metadata without reading or copying the payload.
    ///
    /// # Parameters
    ///
    /// * `id` - Stable task identifier.
    ///
    /// # Returns
    ///
    /// A future resolving to the task summary when retained.
    ///
    /// # Errors
    ///
    /// Resolves to an error if the store cannot complete the read.
    fn get_summary<'a>(&'a self, id: TaskId) -> TaskFuture<'a, Result<Option<TaskSummary>, StoreError>>;

    /// Cancels a blocked record only when its revision still matches.
    ///
    /// # Parameters
    ///
    /// * `id` - Stable identifier of the blocked task.
    /// * `expected_version` - State version observed by the caller.
    ///
    /// # Returns
    ///
    /// A future resolving to the cancelled task summary.
    ///
    /// # Errors
    ///
    /// Resolves to `UnsupportedCapability` by default, or another store error
    /// when an implementation cannot apply the transition.
    fn abandon_blocked<'a>(
        &'a self,
        id: TaskId,
        expected_version: u64,
    ) -> TaskFuture<'a, Result<TaskSummary, StoreError>> {
        let _ = (id, expected_version);
        Box::pin(async { Err(StoreError::UnsupportedCapability) })
    }

    /// Loads one task record by its stable identifier.
    ///
    /// # Parameters
    ///
    /// * `id` - Stable task identifier.
    ///
    /// # Returns
    ///
    /// A future resolving to the retained task, if any.
    ///
    /// # Errors
    ///
    /// Resolves to an error if the store cannot complete the read.
    fn get<'a>(&'a self, id: TaskId) -> TaskFuture<'a, Result<Option<TaskRecord>, StoreError>>;

    /// Lists a bounded page of task history.
    ///
    /// # Parameters
    ///
    /// * `query` - State, correlation, cursor, and page-size filters.
    ///
    /// # Returns
    ///
    /// A future resolving to the requested page and optional continuation
    /// cursor.
    ///
    /// # Errors
    ///
    /// Resolves to an error for an invalid page size or a storage failure.
    fn list<'a>(&'a self, query: TaskQuery) -> TaskFuture<'a, Result<TaskPage, StoreError>>;

    /// Counts every retained lifecycle state in one store snapshot, including
    /// terminal records.
    ///
    /// # Returns
    ///
    /// A future resolving to counts from one consistent store snapshot.
    ///
    /// # Errors
    ///
    /// Resolves to an error when aggregation or storage access fails.
    #[must_use]
    fn count_states<'a>(&'a self) -> TaskFuture<'a, Result<TaskStateCounts, StoreError>>;

    /// Deletes at most `max_rows` terminal records accepted before the supplied
    /// timestamp.
    ///
    /// The default reports `UnsupportedCapability`.
    ///
    /// # Parameters
    ///
    /// * `accepted_before_ms` - Exclusive Unix epoch millisecond cutoff.
    /// * `max_rows` - Maximum number of terminal records to delete.
    ///
    /// # Returns
    ///
    /// A future resolving to the number of deleted records.
    ///
    /// # Errors
    ///
    /// Resolves to `UnsupportedCapability` by default or a store error.
    fn prune_terminal_before<'a>(
        &'a self,
        accepted_before_ms: u64,
        max_rows: NonZeroUsize,
    ) -> TaskFuture<'a, Result<usize, StoreError>> {
        let _ = (accepted_before_ms, max_rows);
        Box::pin(async { Err(StoreError::UnsupportedCapability) })
    }

    /// Acquires exclusive ownership before a recoverable service starts.
    ///
    /// # Returns
    ///
    /// A future resolving to the ownership epoch used by recovery operations.
    ///
    /// # Errors
    ///
    /// Resolves to an error if another owner is active or acquisition fails.
    fn acquire_owner<'a>(&'a self) -> TaskFuture<'a, Result<OwnerEpoch, StoreError>>;

    /// Returns true only when queued or running records strictly exceed
    /// `limit`.
    ///
    /// Implementations must check the result in one store consistency boundary
    /// without loading or decoding request payloads.
    ///
    /// # Parameters
    ///
    /// * `limit` - Maximum accepted number of queued and running records.
    ///
    /// # Returns
    ///
    /// A future resolving to whether the count strictly exceeds `limit`.
    ///
    /// # Errors
    ///
    /// Resolves to an error if the store cannot perform the consistency check.
    fn has_unfinished_over_limit<'a>(&'a self, limit: usize) -> TaskFuture<'a, Result<bool, StoreError>>;

    /// Scans one bounded page of unfinished tasks during recovery.
    ///
    /// # Parameters
    ///
    /// * `cursor` - Exclusive task ID cursor from the previous page, if any.
    ///
    /// # Returns
    ///
    /// A future resolving to one bounded page and its optional next cursor.
    ///
    /// # Errors
    ///
    /// Resolves to an error if recovery scanning is unsupported or fails.
    fn scan_unfinished<'a>(&'a self, cursor: Option<TaskId>) -> TaskFuture<'a, Result<StoredTaskPage, StoreError>>;

    /// Releases ownership after the service has stopped accepting work.
    ///
    /// # Parameters
    ///
    /// * `epoch` - Ownership epoch returned by [`TaskStore::acquire_owner`].
    ///
    /// # Returns
    ///
    /// A future resolving when the ownership lock is released.
    ///
    /// # Errors
    ///
    /// Resolves to an error if the epoch is stale or release fails.
    fn release_owner<'a>(&'a self, epoch: OwnerEpoch) -> TaskFuture<'a, Result<(), StoreError>>;
}
