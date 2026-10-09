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
use crate::model::OwnerEpoch;
use crate::model::StoreCapabilities;
use crate::model::typed::AcceptOutcome as TypedAcceptOutcome;
use crate::model::typed::ProgressCommand as TypedProgressCommand;
use crate::model::typed::StartCommand as TypedStartCommand;
use crate::model::typed::StoredTask as TypedStoredTask;
use crate::model::typed::StoredTaskRequest as TypedStoredTaskRequest;
use crate::model::typed::TaskId as TypedTaskId;
use crate::model::typed::TaskPage as TypedTaskPage;
use crate::model::typed::TaskQuery as TypedTaskQuery;
use crate::model::typed::TaskSummary as TypedTaskSummary;
use crate::model::typed::TransitionCommand as TypedTransitionCommand;

/// Persists encoded requests and lifecycle summaries for typed tasks.
///
/// IDs use `qubit_id::Id`; payload type and schema identity remain in the
/// stored payload. Implementations must make acceptance and version-checked
/// updates atomic. Recovery ownership methods fence concurrent services.
pub trait TaskStore: Send + Sync {
    /// Enables durable notifications for subsequent state commits under the
    /// current owner. Returns `UnsupportedCapability` for stores without a
    /// persistent outbox, or `OwnerConflict` when no service ownership
    /// epoch is active.
    fn enable_event_outbox<'a>(&'a self) -> TaskFuture<'a, Result<(), StoreError>> {
        Box::pin(async { Err(StoreError::UnsupportedCapability) })
    }

    /// Reads snapshots in deterministic `(created_at_ms, task_id,
    /// state_version)` order, with a limit of 1 through 256.
    /// Requires active service ownership; invalid limits and storage failures
    /// are errors.
    fn list_event_outbox<'a>(
        &'a self,
        limit: usize,
    ) -> TaskFuture<'a, Result<Vec<super::EventOutboxEntry>, StoreError>> {
        let _ = limit;
        Box::pin(async { Err(StoreError::UnsupportedCapability) })
    }

    /// Idempotently removes one confirmed snapshot under the current owner.
    /// Returns an ownership or storage error if the deletion cannot be
    /// completed.
    fn mark_event_published<'a>(
        &'a self,
        task_id: TypedTaskId,
        state_version: u64,
    ) -> TaskFuture<'a, Result<(), StoreError>> {
        let _ = (task_id, state_version);
        Box::pin(async { Err(StoreError::UnsupportedCapability) })
    }

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

    /// Lists queued tasks that can start at `now_ms`, ordered for keyset scans.
    fn list_ready_queued<'a>(
        &'a self,
        after: Option<crate::model::typed::TaskCursor>,
        limit: std::num::NonZeroUsize,
        now_ms: u64,
    ) -> TaskFuture<'a, Result<TypedTaskPage, StoreError>>;

    /// Returns the earliest queued retry deadline strictly after `now_ms`.
    fn next_retry_deadline<'a>(&'a self, now_ms: u64) -> TaskFuture<'a, Result<Option<u64>, StoreError>>;

    /// Deletes a bounded batch of terminal tasks whose finish time is before
    /// the cutoff.
    ///
    /// Deletion is ordered by `(finished_at_ms, task_id)`. Only succeeded,
    /// failed, panicked, and cancelled tasks are eligible. The operation is
    /// explicit; stores never prune history automatically.
    fn prune_terminal_before<'a>(
        &'a self,
        finished_before_ms: u64,
        max_rows: NonZeroUsize,
    ) -> TaskFuture<'a, Result<usize, StoreError>>;

    /// Acquires exclusive ownership before recovering unfinished tasks.
    fn acquire_owner<'a>(&'a self) -> TaskFuture<'a, Result<OwnerEpoch, StoreError>>;

    /// Releases ownership after all writes admitted under it have completed.
    fn release_owner<'a>(&'a self, epoch: OwnerEpoch) -> TaskFuture<'a, Result<(), StoreError>>;
}
