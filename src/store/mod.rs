// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Pluggable task history and recovery storage.

mod memory;
#[cfg(feature = "sqlite")]
mod sqlite;

use std::future::Future;
use std::pin::Pin;

pub use memory::MemoryTaskStore;
#[cfg(feature = "sqlite")]
pub use sqlite::SqliteTaskStore;
use thiserror::Error;

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
use crate::model::TransitionCommand;

/// Sendable boxed future used by object-safe asynchronous component APIs.
pub type TaskFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Persistent task history implementation contract.
pub trait TaskStore: Send + Sync {
    /// Reports whether history and unfinished task descriptions survive
    /// restart.
    fn capabilities(&self) -> StoreCapabilities;
    /// Atomically accepts a task or returns an existing identical idempotent
    /// task.
    fn accept<'a>(&'a self, id: TaskId, request: TaskRequest) -> TaskFuture<'a, Result<AcceptOutcome, StoreError>>;
    /// Finds an accepted idempotency key without consuming queue capacity.
    fn find_idempotent<'a>(&'a self, request: TaskRequest) -> TaskFuture<'a, Result<Option<TaskRecord>, StoreError>>;
    /// Applies a lifecycle transition only when its expected revision matches.
    fn transition<'a>(&'a self, command: TransitionCommand) -> TaskFuture<'a, Result<TaskRecord, StoreError>>;
    /// Loads one task record by its stable identifier.
    fn get<'a>(&'a self, id: TaskId) -> TaskFuture<'a, Result<Option<TaskRecord>, StoreError>>;
    /// Lists a bounded page of task history.
    fn list<'a>(&'a self, query: TaskQuery) -> TaskFuture<'a, Result<TaskPage, StoreError>>;
    /// Counts every retained lifecycle state in one store snapshot, including
    /// terminal records. Returns a storage error if aggregation fails.
    fn count_states<'a>(&'a self) -> TaskFuture<'a, Result<TaskStateCounts, StoreError>>;
    /// Acquires exclusive ownership before a recoverable service starts.
    fn acquire_owner<'a>(&'a self) -> TaskFuture<'a, Result<OwnerEpoch, StoreError>>;
    /// Scans one bounded page of unfinished tasks during recovery.
    fn scan_unfinished<'a>(&'a self, cursor: Option<TaskId>) -> TaskFuture<'a, Result<StoredTaskPage, StoreError>>;
    /// Releases ownership after the service has stopped accepting work.
    fn release_owner<'a>(&'a self, epoch: OwnerEpoch) -> TaskFuture<'a, Result<(), StoreError>>;
}

/// Storage errors distinguish unsupported recovery from ordinary persistence
/// failures.
#[derive(Debug, Error)]
pub enum StoreError {
    /// The operation requires restart recovery, which this store does not
    /// provide.
    #[error("the selected task store does not support restart recovery")]
    UnsupportedCapability,
    /// A task ID already belongs to a different accepted request.
    #[error("task identifier already exists")]
    DuplicateTask,
    /// An idempotency key was reused with a different request.
    #[error("idempotency key was reused with a different task request")]
    IdempotencyConflict,
    /// The expected state revision or attempt no longer matches.
    #[error("task state changed before the requested transition")]
    Conflict,
    /// A request or persisted diagnostic violates a documented size limit.
    #[error("invalid task data: {0}")]
    InvalidRequest(&'static str),
    /// A valid revision attempted an illegal lifecycle transition.
    #[error("task lifecycle transition is not allowed")]
    InvalidTransition,
    /// No task with the requested identifier is retained.
    #[error("task was not found")]
    NotFound,
    /// Persistence implementation reported an operational failure.
    #[error("task store failure: {0}")]
    Failure(String),
}
