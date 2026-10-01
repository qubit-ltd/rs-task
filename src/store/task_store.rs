// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
#[cfg(test)]
use std::num::NonZeroUsize;

use super::StoreError;
use super::TaskFuture;
#[cfg(test)]
use crate::model::AcceptOutcome;
use crate::model::OwnerEpoch;
#[cfg(test)]
use crate::model::RecoveryPage;
use crate::model::StoreCapabilities;
#[cfg(test)]
use crate::model::TaskCursor;
#[cfg(test)]
use crate::model::TaskId;
#[cfg(test)]
use crate::model::TaskPage;
#[cfg(test)]
use crate::model::TaskQuery;
#[cfg(test)]
use crate::model::TaskRecord;
#[cfg(test)]
use crate::model::TaskRequest;
#[cfg(test)]
use crate::model::TaskStateCounts;
#[cfg(test)]
use crate::model::TaskSummary;
#[cfg(test)]
use crate::model::TransitionCommand;
use crate::model::next::AcceptOutcome as TypedAcceptOutcome;
use crate::model::next::AcceptOutcome as EncodedAcceptOutcome;
use crate::model::next::ProgressCommand as TypedProgressCommand;
use crate::model::next::ProgressCommand;
use crate::model::next::StartCommand as TypedStartCommand;
use crate::model::next::StartCommand;
use crate::model::next::StoredTask as TypedStoredTask;
use crate::model::next::StoredTask;
use crate::model::next::StoredTaskRequest as TypedStoredTaskRequest;
use crate::model::next::StoredTaskRequest;
use crate::model::next::TaskId as TypedTaskId;
use crate::model::next::TaskPage as TypedTaskPage;
use crate::model::next::TaskPage as EncodedTaskPage;
use crate::model::next::TaskQuery as TypedTaskQuery;
use crate::model::next::TaskQuery as EncodedTaskQuery;
use crate::model::next::TaskSummary as TypedTaskSummary;
use crate::model::next::TransitionCommand as TypedTransitionCommand;
use crate::model::next::TransitionCommand as EncodedTransitionCommand;

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
pub(crate) trait LegacyTaskStore: Send + Sync {
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
    #[cfg(test)]
    fn accept<'a>(&'a self, id: TaskId, request: TaskRequest) -> TaskFuture<'a, Result<AcceptOutcome, StoreError>>;

    /// Accepts a request whose payload has already been encoded for storage.
    ///
    /// Existing stores may keep supporting only the legacy
    /// [`LegacyTaskStore::accept`] path. Such stores return
    /// `UnsupportedCapability` from this default.
    ///
    /// # Parameters
    ///
    /// * `id` - Stable identifier to assign if the request is new.
    /// * `request` - Request containing its type-erased stored payload.
    ///
    /// # Returns
    ///
    /// A future resolving to the newly accepted task or an identical existing
    /// idempotent task.
    ///
    /// # Errors
    ///
    /// Resolves to `UnsupportedCapability` by default, or a store error when
    /// an implementation supports but cannot complete encoded acceptance.
    fn accept_encoded<'a>(
        &'a self,
        id: crate::model::next::TaskId,
        request: StoredTaskRequest,
    ) -> TaskFuture<'a, Result<EncodedAcceptOutcome, StoreError>> {
        let _ = (id, request);
        Box::pin(async { Err(StoreError::UnsupportedCapability) })
    }

    /// Loads a typed request and summary for recovery or status queries.
    ///
    /// # Parameters
    ///
    /// * `id` - Stable typed task identifier to load.
    ///
    /// # Returns
    ///
    /// `Some` when an encoded task is retained, or `None` when it is absent.
    ///
    /// # Errors
    ///
    /// Returns `UnsupportedCapability` when the store has not implemented
    /// encoded task persistence, or a store error when loading fails.
    fn get_encoded_task<'a>(
        &'a self,
        id: crate::model::next::TaskId,
    ) -> TaskFuture<'a, Result<Option<StoredTask>, StoreError>> {
        let _ = id;
        Box::pin(async { Err(StoreError::UnsupportedCapability) })
    }

    /// Starts a queued encoded task attempt with a lifecycle version check.
    ///
    /// # Parameters
    ///
    /// * `command` - Task identity, expected state revision, and start time.
    ///
    /// # Returns
    ///
    /// The updated typed task summary with its attempt count advanced.
    ///
    /// # Errors
    ///
    /// Returns `UnsupportedCapability` by default, `NotFound` when absent, or
    /// `Conflict` when the task is not queued or its state revision changed.
    fn start_encoded<'a>(
        &'a self,
        command: StartCommand,
    ) -> TaskFuture<'a, Result<crate::model::next::TaskSummary, StoreError>> {
        let _ = command;
        Box::pin(async { Err(StoreError::UnsupportedCapability) })
    }

    /// Applies an atomic state transition to a typed task.
    fn transition_encoded<'a>(
        &'a self,
        command: EncodedTransitionCommand,
    ) -> TaskFuture<'a, Result<crate::model::next::TaskSummary, StoreError>> {
        let _ = command;
        Box::pin(async { Err(StoreError::UnsupportedCapability) })
    }

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
    #[cfg(test)]
    fn get_by_idempotency_key<'a>(&'a self, key: &'a str) -> TaskFuture<'a, Result<Option<TaskRecord>, StoreError>>;

    /// Finds lifecycle metadata by idempotency key without loading payload
    /// bytes.
    ///
    /// Implementations must query only the summary projection. This operation
    /// is required so implementations cannot silently fall back to a full
    /// record read.
    ///
    /// # Parameters
    ///
    /// * `key` - Idempotency key to look up.
    ///
    /// # Returns
    ///
    /// A future resolving to the matching retained summary, if any.
    ///
    /// # Errors
    ///
    /// Resolves to an error if the store cannot complete the lookup.
    #[cfg(test)]
    fn get_summary_by_idempotency_key<'a>(
        &'a self,
        key: &'a str,
    ) -> TaskFuture<'a, Result<Option<TaskSummary>, StoreError>>;

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
    #[cfg(test)]
    fn transition<'a>(&'a self, command: TransitionCommand) -> TaskFuture<'a, Result<TaskSummary, StoreError>>;

    /// Writes a progress snapshot using an attempt and progress-version
    /// compare-and-set.
    ///
    /// Implementations must accept a command only while the matching attempt
    /// is running and only when its progress version is newer than the stored
    /// version. Lifecycle `state_version` remains independent of the progress
    /// version. The default reports `UnsupportedCapability`.
    ///
    /// # Parameters
    ///
    /// * `command` - Progress snapshot and its expected attempt/version.
    ///
    /// # Returns
    ///
    /// A future resolving to the lifecycle summary after the progress write.
    ///
    /// # Errors
    ///
    /// Resolves to `UnsupportedCapability` by default, `NotFound` when the
    /// task is not retained, `Conflict` for a stale attempt/version, or a store
    /// error when persistence fails.
    fn update_progress<'a>(
        &'a self,
        command: ProgressCommand,
    ) -> TaskFuture<'a, Result<crate::model::next::TaskSummary, StoreError>> {
        let _ = command;
        Box::pin(async { Err(StoreError::UnsupportedCapability) })
    }

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
    #[cfg(test)]
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
    #[cfg(test)]
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
    #[cfg(test)]
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
    #[cfg(test)]
    fn list<'a>(&'a self, query: TaskQuery) -> TaskFuture<'a, Result<TaskPage, StoreError>>;

    /// Lists typed task summaries in deterministic acceptance order.
    ///
    /// The exclusive cursor is ordered by `(accepted_at_ms, numeric task ID)`.
    /// Implementations that do not support typed task storage return
    /// `UnsupportedCapability` by default.
    fn list_encoded<'a>(&'a self, query: EncodedTaskQuery) -> TaskFuture<'a, Result<EncodedTaskPage, StoreError>> {
        let _ = query;
        Box::pin(async { Err(StoreError::UnsupportedCapability) })
    }

    /// Counts every retained lifecycle state in one store snapshot, including
    /// terminal records.
    ///
    /// The returned counts describe one consistent point in the store's
    /// history. Concurrent transitions may change the counts before this
    /// future returns; callers that wait for a state change must register their
    /// notification before querying and recheck the condition after waking.
    ///
    /// # Returns
    ///
    /// A future resolving to counts from one consistent store snapshot.
    ///
    /// # Errors
    ///
    /// Resolves to an error when aggregation or storage access fails.
    #[must_use]
    #[cfg(test)]
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
    #[cfg(test)]
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
    #[cfg(test)]
    fn has_unfinished_over_limit<'a>(&'a self, limit: usize) -> TaskFuture<'a, Result<bool, StoreError>>;

    /// Scans at most 256 Queued/Running summaries during recovery.
    ///
    /// Rows must be strictly ordered by `(accepted_at_ms, id)` and each key
    /// must exceed `cursor`. Scan without decoding request payloads. Ownership
    /// must remain exclusive while the service scans and restores these rows.
    ///
    /// # Parameters
    ///
    /// * `cursor` - Exclusive acceptance-time/ID lower bound, or `None` for the
    ///   first page.
    ///
    /// # Returns
    ///
    /// A future resolving to one bounded page. A next cursor, when present,
    /// equals the last row key and indicates more rows are available. Terminal
    /// pages have `next = None`, even when exactly 256 rows are returned; an
    /// empty page must never have a next cursor.
    ///
    /// # Errors
    ///
    /// Resolves to an error if recovery scanning is unsupported or fails.
    #[cfg(test)]
    fn scan_unfinished<'a>(&'a self, cursor: Option<TaskCursor>) -> TaskFuture<'a, Result<RecoveryPage, StoreError>>;

    /// Releases ownership after the service has stopped accepting work and
    /// drained writes admitted under this ownership epoch.
    ///
    /// `Ok(())` is a completion barrier: no earlier write under `epoch` may
    /// still be running or commit after this future resolves. Implementations
    /// must keep ownership fenced until those writes finish. A release error
    /// does not authorize another service to assume that draining completed.
    ///
    /// # Parameters
    ///
    /// * `epoch` - Ownership epoch returned by
    ///   [`LegacyTaskStore::acquire_owner`].
    ///
    /// # Returns
    ///
    /// A future resolving after all earlier writes are complete and the
    /// ownership lock is released.
    ///
    /// # Errors
    ///
    /// Resolves to an error if the epoch is stale or release fails.
    fn release_owner<'a>(&'a self, epoch: OwnerEpoch) -> TaskFuture<'a, Result<(), StoreError>>;
}

/// Persists encoded requests and lifecycle summaries for typed tasks.
///
/// IDs use `qubit_id::Id`; payload type and schema identity remain in the
/// stored payload. Implementations must make acceptance and version-checked
/// updates atomic. Recovery ownership methods fence concurrent services.
pub trait TaskStore: Send + Sync {
    /// Reports persistence and recovery support.
    fn capabilities(&self) -> StoreCapabilities;

    /// Atomically accepts a type-erased request that was encoded by the codec
    /// registry.
    fn accept_encoded<'a>(
        &'a self,
        id: TypedTaskId,
        request: TypedStoredTaskRequest,
    ) -> TaskFuture<'a, Result<TypedAcceptOutcome, StoreError>>;

    /// Loads a retained task and its stored request.
    fn get_encoded_task<'a>(&'a self, id: TypedTaskId) -> TaskFuture<'a, Result<Option<TypedStoredTask>, StoreError>>;

    /// Starts one attempt if the task is queued at the supplied state version.
    fn start_encoded<'a>(&'a self, command: TypedStartCommand) -> TaskFuture<'a, Result<TypedTaskSummary, StoreError>>;

    /// Applies a typed task lifecycle transition using optimistic concurrency.
    fn transition_encoded<'a>(
        &'a self,
        command: TypedTransitionCommand,
    ) -> TaskFuture<'a, Result<TypedTaskSummary, StoreError>>;

    /// Persists progress for the matching running attempt and newer progress
    /// version.
    fn update_progress<'a>(
        &'a self,
        command: TypedProgressCommand,
    ) -> TaskFuture<'a, Result<TypedTaskSummary, StoreError>>;

    /// Queries summaries in deterministic typed-task order.
    fn list_encoded<'a>(&'a self, query: TypedTaskQuery) -> TaskFuture<'a, Result<TypedTaskPage, StoreError>>;

    /// Acquires exclusive ownership before recovering unfinished tasks.
    fn acquire_owner<'a>(&'a self) -> TaskFuture<'a, Result<OwnerEpoch, StoreError>>;

    /// Releases ownership after all writes admitted under it have completed.
    fn release_owner<'a>(&'a self, epoch: OwnerEpoch) -> TaskFuture<'a, Result<(), StoreError>>;
}
