// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::num::NonZeroUsize;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::Mutex;
use qubit_id::Id;
use rusqlite::Connection;
use rusqlite::Error;
use rusqlite::OptionalExtension;
use rusqlite::params;
use serde_json::Value;
use serde_json::from_str;
use serde_json::from_value;
use serde_json::to_string;
use tokio::sync;
use tokio::task;

use self::internal::DatabaseIdentity;
use self::internal::SqliteOwnerState;
#[cfg(test)]
use self::internal::WorkerCounts;
#[cfg(test)]
use self::internal::WorkerGuard;
use self::internal::accept_encoded;
use self::internal::acquire_owner_lock;
use self::internal::get_encoded_task;
use self::internal::initialize_next_schema;
use self::internal::list_encoded;
use self::internal::list_ready_queued;
use self::internal::next_retry_deadline;
use self::internal::prune_terminal_rows;
use self::internal::start_encoded;
use self::internal::transition_encoded;
use self::internal::update_progress;
use super::StoreError;
use super::TaskFuture;
use crate::model::OwnerEpoch;
use crate::model::StoreCapabilities;
use crate::model::TaskState;
use crate::model::typed::AcceptOutcome;
use crate::model::typed::ProgressCommand;
use crate::model::typed::StartCommand;
use crate::model::typed::StoredTask;
use crate::model::typed::StoredTaskRequest;
use crate::model::typed::TaskCursor;
use crate::model::typed::TaskId;
use crate::model::typed::TaskPage as EncodedTaskPage;
use crate::model::typed::TaskQuery as EncodedTaskQuery;
use crate::model::typed::TaskStateCheckpoint;
use crate::model::typed::TaskSummary;
use crate::model::typed::TransitionCommand;

mod internal;
#[cfg(test)]
#[path = "../tests/store/sqlite_task_store/owner_lock_tests.rs"]
mod owner_lock_tests;

/// SQLite-backed history with an exclusive OS lock for one active service
/// process.
///
/// # Examples
///
/// ```
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use std::fs;
/// use std::io::ErrorKind;
/// use std::time::SystemTime;
///
/// use qubit_task::store::TaskStore;
/// use qubit_task::store::SqliteTaskStore;
///
/// let timestamp = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?.as_nanos();
/// let directory = std::env::temp_dir().join(format!("qubit-task-doc-{}-{timestamp}", std::process::id()));
/// match fs::create_dir(&directory) {
///     Ok(()) => {},
///     Err(error) if error.kind() == ErrorKind::AlreadyExists => {
///         return Err(error.into());
///     }
///     Err(error) => return Err(error.into()),
/// }
/// let path = directory.join("tasks.sqlite");
/// let store = SqliteTaskStore::open(&path)?;
/// assert!(store.capabilities().restart_recovery);
/// drop(store);
/// fs::remove_file(&path)?;
/// let mut lock_path = path.as_os_str().to_owned();
/// lock_path.push(".owner.lock");
/// fs::remove_file(std::path::PathBuf::from(lock_path))?;
/// fs::remove_dir(directory)?;
/// # Ok(())
/// # }
/// ```
pub struct SqliteTaskStore {
    /// SQLite connection serialized behind a synchronous mutex.
    connection: Arc<Mutex<Connection>>,
    /// Canonical path used to reacquire the process lock after owner release.
    database_path: PathBuf,
    /// Process-lock file and currently held recovery epoch.
    owner_state: Arc<Mutex<SqliteOwnerState>>,
    /// Bounds connection operations to one blocking worker at a time.
    operation_slot: Arc<sync::Semaphore>,
    /// Controls whether accepted lifecycle changes create outbox snapshots.
    outbox_enabled: Arc<std::sync::atomic::AtomicBool>,
    /// Tracks blocking worker overlap in unit tests.
    #[cfg(test)]
    worker_counts: Arc<WorkerCounts>,
}

impl SqliteTaskStore {
    /// Opens a database using the typed numeric-ID task schema.
    ///
    /// This cutover entry point creates a fresh schema or reopens the same
    /// typed schema. Legacy UUID databases are rejected with a migration
    /// diagnostic and left unchanged; converting their IDs requires an
    /// explicit mapping migration.
    ///
    /// # Parameters
    ///
    /// * `path` - SQLite database path to open or create.
    ///
    /// # Returns
    ///
    /// A store holding the process lock for the canonical database path.
    ///
    /// # Errors
    ///
    /// Returns `OwnerConflict` if another process owns the database,
    /// `UnsupportedDatabaseIdentity` if the file cannot be safely fenced, or
    /// `Failure` when filesystem, identity, schema, or SQLite configuration
    /// operations fail.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let (identity, _database_file) = DatabaseIdentity::open(path.as_ref())?;
        let lock = acquire_owner_lock(identity.path())?;
        identity.verify()?;
        let mut connection = Connection::open(identity.path()).map_err(failure)?;
        identity.verify()?;
        connection
            .busy_timeout(std::time::Duration::from_secs(5))
            .map_err(failure)?;
        initialize_next_schema(&mut connection)?;
        connection
            .execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;")
            .map_err(failure)?;
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
            database_path: identity.path().to_path_buf(),
            owner_state: Arc::new(Mutex::new(SqliteOwnerState {
                lock_file: Some(lock),
                epoch: None,
            })),
            operation_slot: Arc::new(sync::Semaphore::new(1)),
            outbox_enabled: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            #[cfg(test)]
            worker_counts: Arc::new(WorkerCounts::default()),
        })
    }

    /// Enables subsequent transactional snapshots while holding the owner
    /// barrier.
    ///
    /// # Returns
    ///
    /// A future resolving after snapshot creation is enabled.
    ///
    /// # Errors
    ///
    /// Returns `OwnerConflict` without active ownership or a storage failure
    /// if the operation cannot complete.
    pub(super) fn enable_outbox(&self) -> TaskFuture<'_, Result<(), StoreError>> {
        let enabled = Arc::clone(&self.outbox_enabled);
        self.run_outbox(move |_| {
            enabled.store(true, std::sync::atomic::Ordering::Release);
            Ok(())
        })
    }

    /// Reads a bounded oldest-first page; rejects absent ownership and invalid
    /// limits.
    ///
    /// # Parameters
    ///
    /// * `limit` - Maximum number of entries, from 1 through 256.
    ///
    /// # Returns
    ///
    /// Outbox entries in deterministic creation, task, and version order.
    ///
    /// # Errors
    ///
    /// Returns `OwnerConflict` without active ownership, `InvalidRequest` for
    /// an invalid limit, or `Failure` for a database error.
    pub(super) fn list_outbox(&self, limit: usize) -> TaskFuture<'_, Result<Vec<super::EventOutboxEntry>, StoreError>> {
        self.run_outbox(move |connection| {
            if !(1..=256).contains(&limit) {
                return Err(StoreError::InvalidRequest(
                    "outbox page limit must be 1..=256",
                ));
            }
            let mut statement = connection
                .prepare("SELECT task_id,state_version,event_id,event_json FROM task_event_outbox ORDER BY created_at_ms,task_id,state_version LIMIT ?1")
                .map_err(failure)?;
            let rows = statement
                .query_map([limit as i64], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, u64>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                })
                .map_err(failure)?;
            rows.map(|row| {
                let (id, state_version, event_id, event_json) = row.map_err(failure)?;
                let task_id = TaskId::from_id(Id::new(
                    id.parse::<u64>().map_err(failure)?,
                ));
                Ok(super::EventOutboxEntry {
                    task_id,
                    state_version,
                    event_id,
                    event_json,
                })
            })
            .collect()
        })
    }

    /// Deletes a confirmed event idempotently while ownership remains fenced.
    ///
    /// # Parameters
    ///
    /// * `id` - Task identity attached to the published event.
    /// * `version` - Lifecycle version attached to the published event.
    ///
    /// # Returns
    ///
    /// A future resolving after the matching outbox row is removed.
    ///
    /// # Errors
    ///
    /// Returns `OwnerConflict` without active ownership or `Failure` when the
    /// delete operation fails.
    pub(super) fn mark_outbox_published(&self, id: TaskId, version: u64) -> TaskFuture<'_, Result<(), StoreError>> {
        self.run_outbox(move |connection| {
            connection
                .execute(
                    "DELETE FROM task_event_outbox WHERE task_id=?1 AND state_version=?2",
                    params![id.to_padded_decimal(), version],
                )
                .map_err(failure)?;
            Ok(())
        })
    }

    /// Archives and removes the expected current head in one SQLite
    /// transaction while the process owner fence is held.
    ///
    /// # Parameters
    ///
    /// * `expected_task_id` - Task identity expected at the queue head.
    /// * `expected_state_version` - Lifecycle revision expected at the head.
    /// * `expected_event_id` - Event identity expected at the head.
    /// * `reason` - Nonblank operator explanation, limited to 4096 bytes.
    /// * `last_error` - Publishing error retained in the archive.
    ///
    /// # Returns
    ///
    /// A future resolving after the archive and deletion commit together.
    ///
    /// # Errors
    ///
    /// Returns `OwnerConflict` without active ownership, `Conflict` if the
    /// expected head changed, `InvalidRequest` for an invalid reason, or
    /// `Failure` for a transaction error.
    pub(super) fn quarantine_outbox_head<'a>(
        &'a self,
        expected_task_id: TaskId,
        expected_state_version: u64,
        expected_event_id: &'a str,
        reason: &'a str,
        last_error: &'a str,
    ) -> TaskFuture<'a, Result<(), StoreError>> {
        let expected_task_key = expected_task_id.to_padded_decimal();
        let expected_event_id = expected_event_id.to_owned();
        let reason = reason.to_owned();
        let last_error = last_error.to_owned();
        self.run_outbox(move |connection| {
            if reason.trim().is_empty() || reason.len() > 4096 {
                return Err(StoreError::InvalidRequest(
                    "outbox quarantine reason must be nonblank and at most 4096 bytes",
                ));
            }
            let transaction = connection.unchecked_transaction().map_err(failure)?;
            let head = transaction
                .query_row(
                    "SELECT task_id,state_version,event_id,event_json,created_at_ms FROM task_event_outbox ORDER BY created_at_ms,task_id,state_version LIMIT 1",
                    [],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, u64>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, String>(3)?,
                            row.get::<_, i64>(4)?,
                        ))
                    },
                )
                .map_err(|error| match error {
                    Error::QueryReturnedNoRows => StoreError::Conflict,
                    other => failure(other),
                })?;
            let (task_key, state_version, event_id, event_json, created_at_ms) = head;
            if task_key != expected_task_key
                || state_version != expected_state_version
                || event_id != expected_event_id
            {
                return Err(StoreError::Conflict);
            }
            let quarantined_at_ms = i64::try_from(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis(),
            )
            .map_err(failure)?;
            transaction
                .execute(
                    "INSERT INTO task_event_outbox_quarantine(task_id,state_version,event_id,event_json,created_at_ms,quarantined_at_ms,reason,last_error) VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
                    params![
                        task_key,
                        state_version,
                        event_id,
                        event_json,
                        created_at_ms,
                        quarantined_at_ms,
                        reason,
                        last_error,
                    ],
                )
                .map_err(failure)?;
            let deleted = transaction
                .execute(
                    "DELETE FROM task_event_outbox WHERE task_id=?1 AND state_version=?2 AND event_id=?3",
                    params![task_key, state_version, event_id],
                )
                .map_err(failure)?;
            if deleted != 1 {
                return Err(StoreError::Conflict);
            }
            transaction.commit().map_err(failure)
        })
    }

    /// Serializes an outbox operation with epoch validation and owner release.
    /// The owner-state lock remains held while the supplied operation runs.
    ///
    /// # Type Parameters
    ///
    /// * `T` - Value produced by the operation.
    /// * `F` - Outbox operation executed against the SQLite connection.
    ///
    /// # Parameters
    ///
    /// * `operation` - Work to execute while active ownership is confirmed.
    ///
    /// # Returns
    ///
    /// A future resolving to the operation's value.
    ///
    /// # Errors
    ///
    /// Returns `OwnerConflict` without active ownership and propagates errors
    /// from the operation or blocking worker.
    fn run_outbox<'a, T, F>(&'a self, operation: F) -> TaskFuture<'a, Result<T, StoreError>>
    where
        T: Send + 'static,
        F: FnOnce(&Connection) -> Result<T, StoreError> + Send + 'static,
    {
        let owner_state = Arc::clone(&self.owner_state);
        self.run(move |connection| {
            let owner = owner_state.lock();
            if owner.epoch.is_none() || owner.lock_file.is_none() {
                return Err(StoreError::OwnerConflict);
            }
            operation(connection)
        })
    }

    /// Runs one connection operation on the blocking pool through its serial
    /// slot.
    ///
    /// # Type Parameters
    ///
    /// * `T` - Value produced by the operation.
    /// * `F` - Blocking connection operation.
    ///
    /// # Parameters
    ///
    /// * `operation` - Closure executed while holding the operation slot.
    ///
    /// # Returns
    ///
    /// A future resolving to the operation value.
    ///
    /// # Errors
    ///
    /// Returns an error when the slot, blocking worker, or operation fails.
    fn run<'a, T, F>(&'a self, operation: F) -> TaskFuture<'a, Result<T, StoreError>>
    where
        T: Send + 'static,
        F: FnOnce(&Connection) -> Result<T, StoreError> + Send + 'static,
    {
        let connection = self.connection.clone();
        let operation_slot = Arc::clone(&self.operation_slot);
        #[cfg(test)]
        let worker_counts = Arc::clone(&self.worker_counts);
        Box::pin(async move {
            let permit = operation_slot
                .acquire_owned()
                .await
                .map_err(|_| StoreError::Failure("SQLite operation queue is closed".into()))?;
            task::spawn_blocking(move || {
                let _permit = permit;
                #[cfg(test)]
                let _worker_guard = WorkerGuard::enter(worker_counts);
                operation(&connection.lock())
            })
            .await
            .map_err(|error| StoreError::Failure(format!("SQLite blocking operation stopped: {error}")))?
        })
    }

    /// Runs one serialized write only while this store still owns its process
    /// lock.
    ///
    /// # Type Parameters
    ///
    /// * `T` - Value produced by the operation.
    /// * `F` - Blocking connection operation.
    ///
    /// # Parameters
    ///
    /// * `operation` - Write closure executed while ownership is checked.
    ///
    /// # Returns
    ///
    /// A future resolving to the operation value.
    ///
    /// # Errors
    ///
    /// Returns an error when ownership is absent or the write fails.
    fn run_write<'a, T, F>(&'a self, operation: F) -> TaskFuture<'a, Result<T, StoreError>>
    where
        T: Send + 'static,
        F: FnOnce(&Connection) -> Result<T, StoreError> + Send + 'static,
    {
        let owner_state = Arc::clone(&self.owner_state);
        self.run(move |connection| {
            let owner_state = owner_state.lock();
            if owner_state.lock_file.is_none() {
                return Err(StoreError::Failure("SQLite task store has no active owner".into()));
            }
            operation(connection)
        })
    }
}

/// Converts a SQLite or related operation error into the store's public error
/// type.
///
/// The original error is retained as its display string so callers receive a
/// consistent [`StoreError::Failure`] without exposing backend-specific error
/// types.
///
/// # Parameters
///
/// * `error` - SQLite or related error to convert.
///
/// # Returns
///
/// A store failure containing the original display diagnostic.
fn failure(error: impl std::fmt::Display) -> StoreError {
    StoreError::Failure(error.to_string())
}

/// Reads the current lifecycle state or its durable post-prune checkpoint.
///
/// # Parameters
///
/// * `connection` - SQLite connection containing task history.
/// * `id` - Task identity to look up.
///
/// # Returns
///
/// `Some` when a retained task or terminal checkpoint exists, or `None` when
/// neither record exists for the ID.
///
/// # Errors
///
/// Returns `Failure` when SQLite data cannot be read or its lifecycle state
/// cannot be decoded.
fn get_state_checkpoint(connection: &Connection, id: TaskId) -> Result<Option<TaskStateCheckpoint>, StoreError> {
    let id_key = id.to_padded_decimal();
    let current = connection
        .query_row("SELECT lifecycle_json FROM tasks WHERE id=?1", [&id_key], |row| {
            row.get::<_, String>(0)
        })
        .optional()
        .map_err(failure)?;
    if let Some(lifecycle_json) = current {
        let lifecycle: Value = from_str(&lifecycle_json).map_err(failure)?;
        let state: TaskState = from_value(
            lifecycle
                .get("state")
                .cloned()
                .ok_or_else(|| StoreError::Failure("task lifecycle is missing its state".into()))?,
        )
        .map_err(failure)?;
        let state_version = lifecycle
            .get("state_version")
            .and_then(Value::as_i64)
            .ok_or_else(|| StoreError::Failure("task lifecycle has an invalid state version".into()))?;
        let state_version = u64::try_from(state_version).map_err(failure)?;
        return Ok(Some(TaskStateCheckpoint {
            id,
            state_version,
            state,
        }));
    }
    let stored = connection
        .query_row(
            "SELECT state_version,state_json FROM task_terminal_checkpoints WHERE task_id=?1",
            [&id_key],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()
        .map_err(failure)?;
    stored
        .map(|(version, state_json)| {
            Ok(TaskStateCheckpoint {
                id,
                state_version: u64::try_from(version).map_err(failure)?,
                state: from_str(&state_json).map_err(failure)?,
            })
        })
        .transpose()
}

impl super::TaskStore for SqliteTaskStore {
    /// Reports that history is persistent and unfinished records can recover.
    ///
    /// # Returns
    ///
    /// Both persistent-history and restart-recovery capabilities.
    #[inline]
    fn capabilities(&self) -> StoreCapabilities {
        StoreCapabilities {
            persistent_history: true,
            restart_recovery: true,
        }
    }

    /// Atomically accepts an encoded typed request on a store opened with
    /// [`SqliteTaskStore::open`].
    ///
    /// # Parameters
    ///
    /// * `id` - Stable identity assigned to the new task.
    /// * `request` - Encoded request and metadata to persist.
    ///
    /// # Returns
    ///
    /// The acceptance outcome, including whether this request was newly stored.
    ///
    /// # Errors
    ///
    /// Returns an error for inactive ownership, conflicting identity or
    /// idempotency data, or a persistence failure.
    fn accept_encoded<'a>(
        &'a self,
        id: TaskId,
        request: StoredTaskRequest,
    ) -> TaskFuture<'a, Result<AcceptOutcome, StoreError>> {
        let outbox_enabled = Arc::clone(&self.outbox_enabled);
        self.run_write(move |connection| {
            accept_encoded(
                connection,
                id,
                request,
                outbox_enabled.load(std::sync::atomic::Ordering::Acquire),
            )
        })
    }

    /// Loads an encoded typed task without decoding its application payload.
    ///
    /// # Parameters
    ///
    /// * `id` - Stable identity of the task to load.
    ///
    /// # Returns
    ///
    /// The stored task when retained, or `None` when it is absent.
    ///
    /// # Errors
    ///
    /// Returns an error when SQLite data cannot be read or decoded.
    fn get_encoded_task<'a>(&'a self, id: TaskId) -> TaskFuture<'a, Result<Option<StoredTask>, StoreError>> {
        self.run(move |connection| get_encoded_task(connection, id))
    }

    /// Reads the authoritative lifecycle state or retained terminal checkpoint.
    ///
    /// A missing result means the task was never accepted or is no longer
    /// retained by this store.
    ///
    /// # Parameters
    ///
    /// * `id` - Stable identity of the task to inspect.
    ///
    /// # Returns
    ///
    /// The current state and revision, or `None` when no task/checkpoint
    /// exists.
    ///
    /// # Errors
    ///
    /// Returns an error if persisted lifecycle data cannot be read or decoded.
    fn get_state_checkpoint<'a>(
        &'a self,
        id: TaskId,
    ) -> TaskFuture<'a, Result<Option<TaskStateCheckpoint>, StoreError>> {
        self.run(move |connection| get_state_checkpoint(connection, id))
    }

    /// Starts one queued typed task attempt with a revision compare-and-set.
    ///
    /// # Parameters
    ///
    /// * `command` - Task identity, expected revision, and attempt details.
    ///
    /// # Returns
    ///
    /// The updated task summary after the attempt starts.
    ///
    /// # Errors
    ///
    /// Returns an error if the task is absent, its revision changed, its state
    /// cannot start, or persistence fails.
    fn start_encoded<'a>(&'a self, command: StartCommand) -> TaskFuture<'a, Result<TaskSummary, StoreError>> {
        let outbox_enabled = Arc::clone(&self.outbox_enabled);
        self.run_write(move |connection| {
            start_encoded(
                connection,
                command,
                outbox_enabled.load(std::sync::atomic::Ordering::Acquire),
            )
        })
    }

    /// Applies a typed lifecycle transition atomically.
    ///
    /// # Parameters
    ///
    /// * `command` - Expected task revision and requested transition.
    ///
    /// # Returns
    ///
    /// The updated task summary after the transition commits.
    ///
    /// # Errors
    ///
    /// Returns an error if the revision changed, the transition is invalid, or
    /// persistence fails.
    fn transition_encoded<'a>(&'a self, command: TransitionCommand) -> TaskFuture<'a, Result<TaskSummary, StoreError>> {
        let outbox_enabled = Arc::clone(&self.outbox_enabled);
        self.run_write(move |connection| {
            transition_encoded(
                connection,
                command,
                outbox_enabled.load(std::sync::atomic::Ordering::Acquire),
            )
        })
    }

    /// Persists a progress snapshot for the matching running attempt.
    ///
    /// # Parameters
    ///
    /// * `command` - Task attempt identity and progress snapshot to persist.
    ///
    /// # Returns
    ///
    /// The updated task summary after progress is stored.
    ///
    /// # Errors
    ///
    /// Returns an error if the task attempt is not current, progress is
    /// invalid, or persistence fails.
    fn update_progress<'a>(&'a self, command: ProgressCommand) -> TaskFuture<'a, Result<TaskSummary, StoreError>> {
        self.run_write(move |connection| update_progress(connection, command))
    }

    /// Returns a deterministic page of encoded task summaries matching query.
    ///
    /// # Parameters
    ///
    /// * `query` - Typed task filters, ordering, and page cursor.
    ///
    /// # Returns
    ///
    /// A page of task summaries and, when more results exist, its next cursor.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid query values or database failures.
    fn list_encoded<'a>(&'a self, query: EncodedTaskQuery) -> TaskFuture<'a, Result<EncodedTaskPage, StoreError>> {
        self.run(move |connection| list_encoded(connection, query))
    }

    /// Lists queued tasks whose retry time has arrived in keyset order.
    ///
    /// `after` resumes after a prior cursor, and `now_ms` defines readiness.
    ///
    /// # Parameters
    ///
    /// * `after` - Optional cursor from the preceding page.
    /// * `limit` - Maximum number of returned tasks.
    /// * `now_ms` - Current time used to determine retry readiness.
    ///
    /// # Returns
    ///
    /// A page of ready queued tasks and an optional continuation cursor.
    ///
    /// # Errors
    ///
    /// Returns an error for an out-of-range time or page limit, or a database
    /// failure.
    fn list_ready_queued<'a>(
        &'a self,
        after: Option<TaskCursor>,
        limit: NonZeroUsize,
        now_ms: u64,
    ) -> TaskFuture<'a, Result<EncodedTaskPage, StoreError>> {
        self.run(move |connection| list_ready_queued(connection, after, limit, now_ms))
    }

    /// Finds the earliest queued retry time strictly later than `now_ms`.
    ///
    /// # Parameters
    ///
    /// * `now_ms` - Current time in milliseconds since the Unix epoch.
    ///
    /// # Returns
    ///
    /// `Some(deadline)` when a later retry is queued, or `None` when none
    /// exists.
    ///
    /// # Errors
    ///
    /// Returns an error when the time is outside SQLite's integer range or the
    /// query fails.
    fn next_retry_deadline<'a>(&'a self, now_ms: u64) -> TaskFuture<'a, Result<Option<u64>, StoreError>> {
        self.run(move |connection| next_retry_deadline(connection, now_ms))
    }

    /// Removes a bounded batch of old terminal records and retains checkpoints.
    ///
    /// Records with outbox entries are preserved so pending events remain
    /// publishable.
    ///
    /// # Parameters
    ///
    /// * `finished_before_ms` - Exclusive terminal-finish cutoff.
    /// * `max_rows` - Maximum number of records to prune.
    ///
    /// # Returns
    ///
    /// The number of records deleted after their terminal checkpoints are
    /// saved.
    ///
    /// # Errors
    ///
    /// Returns an error for out-of-range values, inconsistent state, or a
    /// transaction failure.
    fn prune_terminal_before<'a>(
        &'a self,
        finished_before_ms: u64,
        max_rows: NonZeroUsize,
    ) -> TaskFuture<'a, Result<usize, StoreError>> {
        let finished_before_ms = match i64::try_from(finished_before_ms) {
            Ok(value) => value,
            Err(_) => {
                return Box::pin(async {
                    Err(StoreError::InvalidRequest(
                        "task finish cutoff exceeds the SQLite integer range",
                    ))
                });
            }
        };
        let max_rows = match i64::try_from(max_rows.get()) {
            Ok(value) => value,
            Err(_) => {
                return Box::pin(async {
                    Err(StoreError::InvalidRequest(
                        "task history prune limit exceeds the SQLite integer range",
                    ))
                });
            }
        };
        self.run_write(move |connection| {
            let transaction = connection.unchecked_transaction().map_err(failure)?;
            let mut statement = transaction
                .prepare("SELECT id,lifecycle_json,CAST(json_extract(lifecycle_json, '$.finished_at_ms') AS INTEGER) FROM tasks WHERE state_kind IN ('Succeeded','Failed','Panicked','Cancelled') AND CAST(json_extract(lifecycle_json, '$.finished_at_ms') AS INTEGER) < ?1 AND NOT EXISTS (SELECT 1 FROM task_event_outbox e WHERE e.task_id=tasks.id) ORDER BY CAST(json_extract(lifecycle_json, '$.finished_at_ms') AS INTEGER), id LIMIT ?2")
                .map_err(failure)?;
            let rows = statement
                .query_map(params![finished_before_ms, max_rows], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                })
                .map_err(failure)?;
            let rows = rows.collect::<Result<Vec<_>, _>>().map_err(failure)?;
            drop(statement);
            prune_terminal_rows(&transaction, &rows)?;
            transaction.commit().map_err(failure)?;
            Ok(rows.len())
        })
    }

    /// Increments and records the exclusive service ownership epoch.
    ///
    /// # Returns
    ///
    /// A future resolving to the new owner epoch.
    ///
    /// # Errors
    ///
    /// Returns an error when the owner lock is absent or metadata access fails.
    fn acquire_owner<'a>(&'a self) -> TaskFuture<'a, Result<OwnerEpoch, StoreError>> {
        let owner_state = Arc::clone(&self.owner_state);
        let database_path = self.database_path.clone();
        self.run(move |connection| {
            let mut owner_state = owner_state.lock();
            if owner_state.epoch.is_some() {
                return Err(StoreError::OwnerConflict);
            }
            if owner_state.lock_file.is_none() {
                owner_state.lock_file = Some(acquire_owner_lock(&database_path)?);
            }
            connection.execute("INSERT INTO metadata(key,value) VALUES('owner_epoch',1) ON CONFLICT(key) DO UPDATE SET value=value+1", []).map_err(failure)?;
            let epoch = connection.query_row("SELECT value FROM metadata WHERE key='owner_epoch'", [], |row| row.get::<_, u64>(0)).map(OwnerEpoch).map_err(failure)?;
            owner_state.epoch = Some(epoch);
            Ok(epoch)
        })
    }

    /// Releases the process lock only when the supplied epoch still matches.
    ///
    /// # Parameters
    ///
    /// * `epoch` - Ownership generation previously issued by this store.
    ///
    /// # Returns
    ///
    /// A future resolving after the lock file is released.
    ///
    /// # Errors
    ///
    /// Returns an error for a stale epoch, absent owner, or lock failure.
    fn release_owner<'a>(&'a self, epoch: OwnerEpoch) -> TaskFuture<'a, Result<(), StoreError>> {
        let owner_state = Arc::clone(&self.owner_state);
        let outbox_enabled = Arc::clone(&self.outbox_enabled);
        self.run(move |_| {
            let mut owner_state = owner_state.lock();
            if owner_state.epoch != Some(epoch) {
                return Err(StoreError::OwnerConflict);
            }
            let file = owner_state.lock_file.take().ok_or(StoreError::OwnerConflict)?;
            owner_state.epoch = None;
            outbox_enabled.store(false, std::sync::atomic::Ordering::Release);
            file.unlock().map_err(failure)
        })
    }

    /// Enables durable event snapshots for subsequent lifecycle commits.
    ///
    /// # Returns
    ///
    /// A future resolving after event snapshots are enabled.
    ///
    /// # Errors
    ///
    /// Returns `OwnerConflict` without active ownership or a persistence error.
    #[inline]
    fn enable_event_outbox<'a>(&'a self) -> TaskFuture<'a, Result<(), StoreError>> {
        self.enable_outbox()
    }
    /// Reads oldest event snapshots in deterministic order, up to `limit`.
    ///
    /// # Parameters
    ///
    /// * `limit` - Maximum number of snapshots, from 1 through 256.
    ///
    /// # Returns
    ///
    /// A future resolving to the ordered event snapshots.
    ///
    /// # Errors
    ///
    /// Returns `OwnerConflict` without active ownership, `InvalidRequest` for
    /// an invalid limit, or a persistence error.
    #[inline]
    fn list_event_outbox<'a>(
        &'a self,
        limit: usize,
    ) -> TaskFuture<'a, Result<Vec<super::EventOutboxEntry>, StoreError>> {
        self.list_outbox(limit)
    }
    /// Removes the event snapshot confirmed by the publisher.
    ///
    /// # Parameters
    ///
    /// * `task_id` - Task identity attached to the published event.
    /// * `state_version` - Lifecycle revision attached to the event.
    ///
    /// # Returns
    ///
    /// A future resolving after the matching snapshot is removed.
    ///
    /// # Errors
    ///
    /// Returns `OwnerConflict` without active ownership or a persistence error.
    #[inline]
    fn mark_event_published<'a>(
        &'a self,
        task_id: TaskId,
        state_version: u64,
    ) -> TaskFuture<'a, Result<(), StoreError>> {
        self.mark_outbox_published(task_id, state_version)
    }

    /// Archives and removes the oldest event when its expected identity
    /// matches.
    ///
    /// The reason must be nonblank and no longer than 4096 bytes.
    ///
    /// # Parameters
    ///
    /// * `expected_task_id` - Task identity expected at the outbox head.
    /// * `expected_state_version` - Lifecycle revision expected at the head.
    /// * `expected_event_id` - Stable event identity expected at the head.
    /// * `reason` - Nonblank operator explanation, limited to 4096 bytes.
    /// * `last_error` - Publishing error retained in the archive.
    ///
    /// # Returns
    ///
    /// A future resolving after the archive and deletion commit together.
    ///
    /// # Errors
    ///
    /// Returns `OwnerConflict` without active ownership, `Conflict` when the
    /// head identity changed, `InvalidRequest` for an invalid reason, or a
    /// persistence error.
    #[inline]
    fn quarantine_event_outbox_head<'a>(
        &'a self,
        expected_task_id: TaskId,
        expected_state_version: u64,
        expected_event_id: &'a str,
        reason: &'a str,
        last_error: &'a str,
    ) -> TaskFuture<'a, Result<(), StoreError>> {
        self.quarantine_outbox_head(
            expected_task_id,
            expected_state_version,
            expected_event_id,
            reason,
            last_error,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use futures::poll;
    use rusqlite::Connection;
    use tokio as tokio_crate;
    use tokio::pin;
    use tokio::spawn;
    use tokio::sync::mpsc;
    use tokio::sync::oneshot;
    use tokio::task;
    use tokio::time;

    use super::SqliteTaskStore;
    use crate::store::StoreError;
    use crate::store::TaskStore;

    /// Creates a unique database path for one worker scheduling test.
    fn test_database_path() -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "qubit-task-sqlite-worker-{}-{}.sqlite",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock is after epoch")
                .as_nanos()
        ))
    }

    /// Removes only disposable files created by this worker test.
    fn remove_database(path: &std::path::Path) {
        let _ = std::fs::remove_file(path);
        let mut lock_path = path.as_os_str().to_owned();
        lock_path.push(".owner.lock");
        let _ = std::fs::remove_file(std::path::PathBuf::from(lock_path));
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }

    #[tokio_crate::test]
    async fn test_blocking_worker_peak_is_bounded_to_one() {
        let path = test_database_path();
        let store = Arc::new(SqliteTaskStore::open(&path).expect("SQLite store opens"));
        let (started_sender, started_receiver) = oneshot::channel();
        let (release_sender, release_receiver) = std::sync::mpsc::channel();
        let running_store = Arc::clone(&store);
        let first = spawn(async move {
            running_store
                .run(move |_| {
                    let _ = started_sender.send(());
                    release_receiver.recv().expect("first operation is released");
                    Ok(())
                })
                .await
        });
        time::timeout(std::time::Duration::from_secs(2), started_receiver)
            .await
            .expect("first operation enters the blocking worker")
            .expect("first operation signals its start");

        let (ready_sender, mut ready_receiver) = mpsc::unbounded_channel();
        let mut waiters = Vec::new();
        for _ in 0..8 {
            let waiting_store = Arc::clone(&store);
            let ready_sender = ready_sender.clone();
            waiters.push(spawn(async move {
                let operation = waiting_store.run(|_| Ok(()));
                let _ = ready_sender.send(());
                operation.await
            }));
        }
        for _ in 0..8 {
            ready_receiver
                .recv()
                .await
                .expect("all waiting operations reach their permit wait");
        }
        let peak = store.worker_counts.peak.load(std::sync::atomic::Ordering::Acquire);
        release_sender.send(()).expect("first operation is still waiting");
        first
            .await
            .expect("first operation joins")
            .expect("first operation succeeds");
        for waiter in waiters {
            waiter
                .await
                .expect("waiting operation joins")
                .expect("waiting operation succeeds");
        }
        assert_eq!(peak, 1, "only one worker enters before the connection lock");

        drop(store);
        remove_database(&path);
    }

    /// Cancelling an async caller must leave its real blocking transaction
    /// fenced until commit. Channels mark transaction entry and continuation;
    /// worker counters and a second connection observe the actual store path.
    #[tokio_crate::test]
    async fn test_cancelled_write_caller_keeps_owner_until_transaction_commits() {
        let path = test_database_path();
        let store = Arc::new(SqliteTaskStore::open(&path).expect("SQLite store opens"));
        let epoch = store.acquire_owner().await.expect("store acquires owner");
        store
            .run_write(|connection| {
                connection
                    .execute_batch("CREATE TABLE barrier_probe (value INTEGER NOT NULL)")
                    .map_err(super::failure)
            })
            .await
            .expect("create probe in this test's database");

        let (entered_sender, entered_receiver) = oneshot::channel();
        let (continue_sender, continue_receiver) = std::sync::mpsc::channel();
        let (committed_sender, committed_receiver) = oneshot::channel();
        let writing_store = Arc::clone(&store);
        let caller = spawn(async move {
            writing_store
                .run_write(move |connection| {
                    let transaction = connection.unchecked_transaction().map_err(super::failure)?;
                    transaction
                        .execute("INSERT INTO barrier_probe(value) VALUES(42)", [])
                        .map_err(super::failure)?;
                    entered_sender.send(()).expect("test observes transaction entry");
                    // A dropped test continuation also lets the blocking worker
                    // exit on assertion failure instead of hanging runtime teardown.
                    continue_receiver.recv().map_err(super::failure)?;
                    transaction.commit().map_err(super::failure)?;
                    committed_sender.send(()).expect("test observes commit");
                    Ok(())
                })
                .await
        });
        time::timeout(Duration::from_secs(5), entered_receiver)
            .await
            .expect("blocking write enters its transaction")
            .expect("transaction entry signal");
        assert_eq!(store.worker_counts.active.load(Ordering::Acquire), 1);
        assert_eq!(store.operation_slot.available_permits(), 0);
        let observer = Connection::open(&path).expect("independent SQLite reader opens");
        let uncommitted: usize = observer
            .query_row("SELECT COUNT(*) FROM barrier_probe", [], |row| row.get(0))
            .expect("reader observes committed rows only");
        assert_eq!(uncommitted, 0, "the entered write has not committed");

        caller.abort();
        assert!(caller.await.expect_err("caller is cancelled").is_cancelled());
        assert_eq!(store.worker_counts.active.load(Ordering::Acquire), 1);
        {
            let release = store.release_owner(epoch);
            pin!(release);
            assert!(
                poll!(release.as_mut()).is_pending(),
                "owner release must wait for the cancelled caller's worker"
            );
            assert_eq!(store.worker_counts.active.load(Ordering::Acquire), 1);
            assert!(
                matches!(SqliteTaskStore::open(&path), Err(StoreError::OwnerConflict)),
                "OS ownership remains fenced"
            );

            continue_sender.send(()).expect("allow transaction to commit");
            time::timeout(Duration::from_secs(5), committed_receiver)
                .await
                .expect("transaction commits after continuation")
                .expect("transaction commit signal");
            time::timeout(Duration::from_secs(5), release)
                .await
                .expect("owner release finishes after the worker")
                .expect("owner release succeeds");
        }
        assert_eq!(store.worker_counts.active.load(Ordering::Acquire), 0);
        assert_eq!(store.operation_slot.available_permits(), 1);
        let committed: usize = observer
            .query_row("SELECT COUNT(*) FROM barrier_probe", [], |row| row.get(0))
            .expect("reader observes the committed write");
        assert_eq!(committed, 1);
        let replacement = SqliteTaskStore::open(&path).expect("ownership can transfer after the completion barrier");
        assert!(
            matches!(store.run_write(|_| Ok(())).await, Err(StoreError::Failure(_))),
            "old owner stays fenced from later writes"
        );
        drop(replacement);
        drop(observer);
        drop(store);
        remove_database(&path);
    }

    #[tokio_crate::test]
    async fn test_run_waits_for_its_single_operation_slot() {
        let path = test_database_path();
        let store = Arc::new(SqliteTaskStore::open(&path).expect("SQLite store opens"));
        let permit = store
            .operation_slot
            .clone()
            .acquire_owned()
            .await
            .expect("operation slot is available");
        let (started_sender, mut started_receiver) = oneshot::channel();
        let running_store = Arc::clone(&store);
        let operation = spawn(async move {
            running_store
                .run(move |_| {
                    let _ = started_sender.send(());
                    Ok(())
                })
                .await
        });
        task::yield_now().await;
        assert!(matches!(
            started_receiver.try_recv(),
            Err(oneshot::error::TryRecvError::Empty | oneshot::error::TryRecvError::Closed)
        ));
        drop(permit);
        time::timeout(std::time::Duration::from_secs(2), started_receiver)
            .await
            .expect("operation starts after the slot is released")
            .expect("operation signals its start");
        operation
            .await
            .expect("operation task joins")
            .expect("SQLite run succeeds");

        drop(store);
        remove_database(&path);
    }

    #[tokio_crate::test]
    async fn test_cancelled_slot_waiter_does_not_block_later_operations() {
        let path = test_database_path();
        let store = Arc::new(SqliteTaskStore::open(&path).expect("SQLite store opens"));
        let permit = store
            .operation_slot
            .clone()
            .acquire_owned()
            .await
            .expect("operation slot is available");
        let (started_sender, mut started_receiver) = oneshot::channel();
        let waiting_store = Arc::clone(&store);
        let waiting = spawn(async move {
            waiting_store
                .run(move |_| {
                    let _ = started_sender.send(());
                    Ok(())
                })
                .await
        });
        task::yield_now().await;
        waiting.abort();
        drop(permit);
        let result = store.run(|_| Ok(())).await;
        assert!(result.is_ok());
        assert!(matches!(
            started_receiver.try_recv(),
            Err(oneshot::error::TryRecvError::Empty | oneshot::error::TryRecvError::Closed)
        ));

        drop(store);
        remove_database(&path);
    }
}
