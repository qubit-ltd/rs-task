// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::num::NonZeroUsize;

use super::EventOutboxEntry;
use super::StoreError;
use super::TaskFuture;
use crate::model::OwnerEpoch;
use crate::model::StoreCapabilities;
use crate::model::typed::AcceptOutcome as TypedAcceptOutcome;
use crate::model::typed::ProgressCommand as TypedProgressCommand;
use crate::model::typed::StartCommand as TypedStartCommand;
use crate::model::typed::StoredTask as TypedStoredTask;
use crate::model::typed::StoredTaskRequest as TypedStoredTaskRequest;
use crate::model::typed::TaskCursor;
use crate::model::typed::TaskId as TypedTaskId;
use crate::model::typed::TaskPage as TypedTaskPage;
use crate::model::typed::TaskQuery as TypedTaskQuery;
use crate::model::typed::TaskStateCheckpoint;
use crate::model::typed::TaskSummary as TypedTaskSummary;
use crate::model::typed::TransitionCommand as TypedTransitionCommand;

/// Persists encoded requests and lifecycle summaries for typed tasks.
///
/// IDs use `qubit_id::Id`; payload type and schema identity remain in the
/// stored payload. Implementations must make acceptance and version-checked
/// updates atomic. Recovery ownership methods coordinate exclusive service
/// recovery.
///
/// Implementations must enforce ownership and write synchronization inside
/// the store. In particular, a persistent store must prevent two services
/// from recovering the same store concurrently and must coordinate release
/// with writes already admitted under that ownership. Dropping a caller's
/// future does not necessarily stop backend work that has already started, so
/// release must also account for writes that may still commit after caller
/// cancellation.
///
/// Write methods do not accept an [`OwnerEpoch`]. The epoch identifies an
/// acquisition and is passed back to [`TaskStore::release_owner`] for release
/// validation; it is not a per-write fencing token. This trait therefore does
/// not promise to reject every operation made through an older business
/// handle. Service lifecycle coordination and the store's internal ownership
/// checks define the ownership boundary.
///
/// In the SQLite implementation, opening the database obtains and retains the
/// process lock before a service acquires an epoch. `acquire_owner` advances
/// the persisted epoch, while writes run through a serialized blocking
/// worker, hold the internal owner-state lock, and check that the process lock
/// is present. They do not compare a caller-supplied epoch. `release_owner`
/// runs through the same serialized worker, waits behind prior work, and
/// compares the supplied epoch before releasing the process lock. The
/// in-memory implementation keeps ownership local to one store instance and
/// does not provide restart or cross-process recovery coordination.
///
/// Existing regression coverage includes the [SQLite cancelled-write and
/// release-wait test](https://github.com/qubit-ltd/rs-task/blob/main/src/store/sqlite_task_store.rs#L1155),
/// [SQLite epoch advancement and stale-release test](https://github.com/qubit-ltd/rs-task/blob/main/tests/store/sqlite_tests.rs#L716),
/// and [owner conflict tests](https://github.com/qubit-ltd/rs-task/blob/main/tests/store_tests.rs#L148).
///
/// # Examples
///
/// Read the current checkpoint for a task using a store supplied by the
/// application:
///
/// ```
/// use qubit_task::store::TaskStore;
/// use qubit_task::TaskId;
///
/// async fn inspect_checkpoint(store: &impl TaskStore, id: TaskId) {
///     match store.get_state_checkpoint(id).await {
///         Ok(Some(checkpoint)) => println!("state version: {}", checkpoint.state_version),
///         Ok(None) => println!("the store has no checkpoint for this task"),
///         Err(error) => eprintln!("failed to read checkpoint: {error}"),
///     }
/// }
/// ```
pub trait TaskStore: Send + Sync {
    /// Enables durable notifications for subsequent state commits under the
    /// current owner. Returns `UnsupportedCapability` for stores without a
    /// persistent outbox, or `OwnerConflict` when no service ownership
    /// epoch is active.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` when notifications are enabled.
    ///
    /// # Errors
    ///
    /// Returns `UnsupportedCapability` when persistent notifications are not
    /// available, or `OwnerConflict` when the service does not own the store.
    fn enable_event_outbox<'a>(&'a self) -> TaskFuture<'a, Result<(), StoreError>> {
        Box::pin(async { Err(StoreError::UnsupportedCapability) })
    }

    /// Reads snapshots in deterministic `(created_at_ms, task_id,
    /// state_version)` order, with a limit of 1 through 256.
    /// Requires active service ownership; invalid limits and storage failures
    /// are errors.
    ///
    /// # Parameters
    ///
    /// * `limit` - Maximum number of snapshots to return; must be between 1 and
    ///   256.
    ///
    /// # Returns
    ///
    /// Returns snapshots in deterministic order, or an empty vector when no
    /// snapshots are pending.
    ///
    /// # Errors
    ///
    /// Returns `InvalidRequest` for an out-of-range limit, `OwnerConflict`
    /// without active ownership, `UnsupportedCapability` when persistent
    /// notifications are unavailable, or `Failure` for storage errors.
    fn list_event_outbox<'a>(&'a self, limit: usize) -> TaskFuture<'a, Result<Vec<EventOutboxEntry>, StoreError>> {
        let _ = limit;
        Box::pin(async { Err(StoreError::UnsupportedCapability) })
    }

    /// Idempotently removes one confirmed snapshot under the current owner.
    /// Returns an ownership or storage error if the deletion cannot be
    /// completed.
    ///
    /// # Parameters
    ///
    /// * `task_id` - Identity of the task whose published snapshot is removed.
    /// * `state_version` - Lifecycle revision identifying the snapshot.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` when the matching snapshot is removed; repeated calls
    /// for an already-removed snapshot are successful.
    ///
    /// # Errors
    ///
    /// Returns `OwnerConflict` without active ownership,
    /// `UnsupportedCapability` when persistent notifications are
    /// unavailable, or `Failure` if storage cannot complete the deletion.
    fn mark_event_published<'a>(
        &'a self,
        task_id: TypedTaskId,
        state_version: u64,
    ) -> TaskFuture<'a, Result<(), StoreError>> {
        let _ = (task_id, state_version);
        Box::pin(async { Err(StoreError::UnsupportedCapability) })
    }

    /// Archives and removes the current oldest event only when its complete
    /// expected identity still matches under the active owner.
    ///
    /// The archive retains the original event and timestamp together with the
    /// operator reason, last publishing error, and quarantine time. Stores
    /// without an atomic persistent outbox return `UnsupportedCapability`.
    ///
    /// # Parameters
    ///
    /// * `expected_task_id` - Task identity expected at the outbox head.
    /// * `expected_state_version` - Lifecycle revision expected at the head.
    /// * `expected_event_id` - Stable event identity expected at the head.
    /// * `reason` - Nonblank operator explanation of the isolation decision,
    ///   limited to 4096 UTF-8 bytes.
    /// * `last_error` - Publishing error captured for the failed head.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` after the archive and outbox deletion commit together.
    ///
    /// # Errors
    ///
    /// Returns `Conflict` if the expected identity is no longer the oldest
    /// event, `OwnerConflict` without active ownership, `InvalidRequest` for an
    /// invalid reason, `UnsupportedCapability` when atomic isolation is not
    /// supported, or `Failure` for persistence errors.
    fn quarantine_event_outbox_head<'a>(
        &'a self,
        expected_task_id: TypedTaskId,
        expected_state_version: u64,
        expected_event_id: &'a str,
        reason: &'a str,
        last_error: &'a str,
    ) -> TaskFuture<'a, Result<(), StoreError>> {
        let _ = (
            expected_task_id,
            expected_state_version,
            expected_event_id,
            reason,
            last_error,
        );
        Box::pin(async { Err(StoreError::UnsupportedCapability) })
    }

    /// Reports persistence and recovery support.
    ///
    /// # Returns
    ///
    /// Returns the capabilities this store can provide to an assembled service.
    #[must_use]
    fn capabilities(&self) -> StoreCapabilities;

    /// Atomically accepts a type-erased request that was encoded by the codec
    /// registry.
    ///
    /// # Parameters
    ///
    /// * `id` - Stable task identity assigned to this acceptance. The
    ///   application must avoid reusing IDs across process restarts. SQLite
    ///   also treats a retained terminal checkpoint as an existing identity
    ///   after its task row is pruned.
    /// * `request` - Encoded request and metadata produced by the codec
    ///   registry.
    ///
    /// # Returns
    ///
    /// Returns the accepted task summary and whether this was a new or
    /// idempotent acceptance.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::DuplicateTask`] if the identity conflicts with
    /// existing work or a retained SQLite terminal checkpoint, or another
    /// error if the request exceeds store limits or persistence fails.
    fn accept_encoded<'a>(
        &'a self,
        id: TypedTaskId,
        request: TypedStoredTaskRequest,
    ) -> TaskFuture<'a, Result<TypedAcceptOutcome, StoreError>>;

    /// Loads a retained task and its stored request.
    ///
    /// # Parameters
    ///
    /// * `id` - Identity of the task to load.
    ///
    /// # Returns
    ///
    /// Returns the stored task when retained, or `None` when it is absent.
    ///
    /// # Errors
    ///
    /// Returns `Failure` when stored task data cannot be read or decoded.
    fn get_encoded_task<'a>(&'a self, id: TypedTaskId) -> TaskFuture<'a, Result<Option<TypedStoredTask>, StoreError>>;

    /// Reads the retained task summary without requiring the stored request
    /// payload.
    ///
    /// Implementations with payload-bearing records should override this
    /// method to avoid loading or cloning the payload. The default preserves
    /// compatibility with existing stores by deriving the summary from
    /// [`TaskStore::get_encoded_task`]. Pruned or evicted tasks return `None`;
    /// terminal checkpoints remain available through
    /// [`TaskStore::get_state_checkpoint`].
    ///
    /// # Parameters
    ///
    /// * `id` - Stable identity of the retained task to read.
    ///
    /// # Returns
    ///
    /// The current summary, or `None` when no task record is retained.
    ///
    /// # Errors
    ///
    /// Returns any error reported while loading or decoding the stored task.
    fn get_summary<'a>(&'a self, id: TypedTaskId) -> TaskFuture<'a, Result<Option<TypedTaskSummary>, StoreError>> {
        Box::pin(async move { Ok(self.get_encoded_task(id).await?.map(|task| task.summary)) })
    }

    /// Returns the authoritative current state and revision when the store has
    /// one.
    ///
    /// SQLite stores retain a terminal checkpoint after pruning task history;
    /// volatile stores return `None` after a task is pruned or evicted. `None`
    /// also means the task was never accepted by this store.
    ///
    /// # Parameters
    ///
    /// * `id` - Identity of the task whose authoritative state is requested.
    ///
    /// # Returns
    ///
    /// Returns the current checkpoint, or `None` when the store has no
    /// authoritative checkpoint for the task.
    ///
    /// # Errors
    ///
    /// Returns `StoreError` when the stored lifecycle or checkpoint cannot be
    /// read or decoded.
    fn get_state_checkpoint<'a>(
        &'a self,
        id: TypedTaskId,
    ) -> TaskFuture<'a, Result<Option<TaskStateCheckpoint>, StoreError>>;

    /// Starts one attempt if the task is queued at the supplied state version.
    ///
    /// # Parameters
    ///
    /// * `command` - Task identity, expected state version, and attempt
    ///   details.
    ///
    /// # Returns
    ///
    /// Returns the summary after the attempt starts.
    ///
    /// # Errors
    ///
    /// Returns `Conflict` when the expected queued state no longer matches,
    /// `NotFound` when the task is absent, or an error for invalid transitions
    /// and storage failures.
    fn start_encoded<'a>(&'a self, command: TypedStartCommand) -> TaskFuture<'a, Result<TypedTaskSummary, StoreError>>;

    /// Applies a typed task lifecycle transition using optimistic concurrency.
    ///
    /// # Parameters
    ///
    /// * `command` - Transition data including task identity and expected
    ///   lifecycle revision.
    ///
    /// # Returns
    ///
    /// Returns the summary after the transition commits.
    ///
    /// # Errors
    ///
    /// Returns `Conflict` when the expected revision no longer matches,
    /// `NotFound` when the task is absent, or an error for an invalid
    /// transition or persistence failure.
    fn transition_encoded<'a>(
        &'a self,
        command: TypedTransitionCommand,
    ) -> TaskFuture<'a, Result<TypedTaskSummary, StoreError>>;

    /// Persists progress for the matching running attempt and a positive,
    /// newer progress version. An attempt starts at version zero, so its first
    /// update must be at least 1; gaps between versions are allowed.
    ///
    /// # Parameters
    ///
    /// * `command` - Progress update with task identity, attempt, and version.
    ///
    /// # Returns
    ///
    /// Returns the summary containing the persisted progress.
    ///
    /// # Errors
    ///
    /// Returns `Conflict` when the attempt or progress version is stale or
    /// zero,
    /// `NotFound` when the task is absent, or an error for invalid data or
    /// persistence failure.
    fn update_progress<'a>(
        &'a self,
        command: TypedProgressCommand,
    ) -> TaskFuture<'a, Result<TypedTaskSummary, StoreError>>;

    /// Queries summaries in deterministic typed-task order.
    ///
    /// # Parameters
    ///
    /// * `query` - Filters, ordering, and page-size limit for the query.
    ///
    /// # Returns
    ///
    /// Returns one page of matching task summaries and its continuation cursor.
    ///
    /// # Errors
    ///
    /// Returns `InvalidRequest` for invalid query bounds or `Failure` when the
    /// store cannot execute or decode the query.
    fn list_encoded<'a>(&'a self, query: TypedTaskQuery) -> TaskFuture<'a, Result<TypedTaskPage, StoreError>>;

    /// Lists queued tasks that can start at `now_ms`, ordered for keyset scans.
    ///
    /// # Parameters
    ///
    /// * `after` - Optional cursor; when present, scanning resumes after it.
    /// * `limit` - Maximum page size, which is nonzero by construction.
    /// * `now_ms` - Current time used to determine whether a task is ready.
    ///
    /// # Returns
    ///
    /// Returns a page of ready queued tasks and a continuation cursor.
    ///
    /// # Errors
    ///
    /// Returns `Failure` when the store cannot scan or decode queued tasks.
    fn list_ready_queued<'a>(
        &'a self,
        after: Option<TaskCursor>,
        limit: NonZeroUsize,
        now_ms: u64,
    ) -> TaskFuture<'a, Result<TypedTaskPage, StoreError>>;

    /// Returns the earliest queued retry deadline strictly after `now_ms`.
    ///
    /// # Parameters
    ///
    /// * `now_ms` - Current time; deadlines at or before it are excluded.
    ///
    /// # Returns
    ///
    /// Returns the earliest future retry deadline, or `None` when none exists.
    ///
    /// # Errors
    ///
    /// Returns `Failure` when the store cannot read queued retry data.
    fn next_retry_deadline<'a>(&'a self, now_ms: u64) -> TaskFuture<'a, Result<Option<u64>, StoreError>>;

    /// Deletes a bounded batch of terminal tasks whose finish time is before
    /// the cutoff.
    ///
    /// Deletion is ordered by `(finished_at_ms, task_id)`. Only succeeded,
    /// failed, panicked, and cancelled tasks are eligible. The operation is
    /// explicit; stores never prune history automatically.
    ///
    /// # Parameters
    ///
    /// * `finished_before_ms` - Exclusive cutoff for task finish timestamps.
    /// * `max_rows` - Maximum number of eligible rows to delete; nonzero by
    ///   construction.
    ///
    /// # Returns
    ///
    /// Returns the number of terminal task records deleted.
    ///
    /// # Errors
    ///
    /// Returns `Failure` when the store cannot select or delete eligible rows.
    fn prune_terminal_before<'a>(
        &'a self,
        finished_before_ms: u64,
        max_rows: NonZeroUsize,
    ) -> TaskFuture<'a, Result<usize, StoreError>>;

    /// Acquires exclusive ownership before recovering unfinished tasks.
    /// The store enforces ownership internally; the returned epoch identifies
    /// this acquisition for the matching `release_owner` call. Write methods
    /// do not take this epoch.
    ///
    /// # Returns
    ///
    /// Returns the epoch to pass to [`TaskStore::release_owner`] when releasing
    /// this acquisition. It is not passed to write methods.
    ///
    /// # Errors
    ///
    /// Returns `OwnerConflict` when another service owns the store, or
    /// `UnsupportedCapability` when ownership is not supported.
    fn acquire_owner<'a>(&'a self) -> TaskFuture<'a, Result<OwnerEpoch, StoreError>>;

    /// Releases ownership after writes admitted under it have completed.
    /// Implementations must serialize release with admitted writes, including
    /// backend work that may continue after the caller's write future is
    /// cancelled.
    ///
    /// # Parameters
    ///
    /// * `epoch` - Ownership generation previously returned by this store.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` after ownership is released.
    ///
    /// # Errors
    ///
    /// Returns `OwnerConflict` when the epoch is not the active owner, or
    /// `UnsupportedCapability` when ownership is not supported, or `Failure`
    /// when the release operation fails. Deferred cleanup may retry `Failure`
    /// with the same epoch.
    /// Implementations must ensure an old epoch can never release a newer
    /// owner's lease; callers may stop retrying when `OwnerConflict` is
    /// returned.
    fn release_owner<'a>(&'a self, epoch: OwnerEpoch) -> TaskFuture<'a, Result<(), StoreError>>;
}
