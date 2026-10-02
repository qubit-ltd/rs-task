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

use internal::DatabaseIdentity;
use internal::SqliteOwnerState;
#[cfg(test)]
use internal::WorkerCounts;
#[cfg(test)]
use internal::WorkerGuard;
use internal::accept_encoded;
use internal::acquire_owner_lock;
#[cfg(test)]
use internal::build_history_query;
#[cfg(test)]
use internal::build_recovery_query;
#[cfg(test)]
use internal::decode_stored_summary_row;
#[cfg(test)]
use internal::decode_stored_task_row;
#[cfg(test)]
use internal::encode_lifecycle;
#[cfg(test)]
use internal::encode_summary_lifecycle;
use internal::get_encoded_task;
use internal::initialize_next_schema;
#[cfg(test)]
use internal::initialize_schema;
use internal::list_encoded;
use internal::list_ready_queued;
use internal::next_retry_deadline;
#[cfg(test)]
use internal::read_stored_summary_row;
#[cfg(test)]
use internal::read_stored_task_row;
use internal::start_encoded;
use internal::transition_encoded;
use internal::update_progress;
use parking_lot::Mutex;
use rusqlite::Connection;
#[cfg(test)]
use rusqlite::OptionalExtension;
use rusqlite::params;
#[cfg(test)]
use rusqlite::params_from_iter;
use tokio::sync;
use tokio::task;

use super::LegacyTaskStore;
use super::StoreError;
use super::TaskFuture;
#[cfg(test)]
use crate::model::MAX_TASK_OUTPUT_SUMMARY_BYTES;
use crate::model::OwnerEpoch;
use crate::model::StoreCapabilities;
#[cfg(test)]
use crate::model::TaskState;
#[cfg(test)]
use crate::model::legacy::AcceptOutcome;
#[cfg(test)]
use crate::model::legacy::RecoveryPage;
#[cfg(test)]
use crate::model::legacy::TaskCursor;
#[cfg(test)]
use crate::model::legacy::TaskId;
#[cfg(test)]
use crate::model::legacy::TaskPage;
#[cfg(test)]
use crate::model::legacy::TaskQuery;
#[cfg(test)]
use crate::model::legacy::TaskRecord;
#[cfg(test)]
use crate::model::legacy::TaskRequest;
#[cfg(test)]
use crate::model::legacy::TaskRequestInfo;
#[cfg(test)]
#[cfg(test)]
use crate::model::legacy::TaskSummary;
#[cfg(test)]
use crate::model::legacy::TransitionCommand;
#[cfg(test)]
use crate::model::legacy::checked_page_size;
use crate::model::next::TaskPage as EncodedTaskPage;
use crate::model::next::TaskQuery as EncodedTaskQuery;

mod internal;

/// Current SQLite schema version written after successful migration.
#[cfg(test)]
const SCHEMA_VERSION: i64 = 3;
/// Current serialized record version required by row decoders.
#[cfg(test)]
const RECORD_FORMAT_VERSION: i64 = 3;
/// Ordered columns used by payload-free task summary queries.
#[cfg(test)]
const SUMMARY_COLUMNS: &str =
    "id,state_kind,accepted_at,correlation_key,idempotency_key,record_format_version,request_info_json,lifecycle_json";

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
/// let store = SqliteTaskStore::open_next(&path)?;
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
    /// Selects the in-progress typed numeric-ID schema path.
    typed_schema: bool,
    /// Tracks blocking worker overlap in unit tests.
    #[cfg(test)]
    worker_counts: Arc<WorkerCounts>,
}

impl SqliteTaskStore {
    /// Opens a database, applies its schema, and locks its physical file.
    ///
    /// # Parameters
    ///
    /// * `path` - Database path; parent directories are created when needed.
    ///
    /// # Returns
    ///
    /// An initialized store holding the database file lock. The service owner
    /// epoch is acquired separately through
    /// [`crate::store::TaskStore::acquire_owner`].
    ///
    /// # Errors
    ///
    /// Returns a store error when opening, locking, initializing, or migrating
    /// the database fails.
    #[cfg(test)]
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let (identity, _database_file) = DatabaseIdentity::open(path.as_ref())?;
        let lock = acquire_owner_lock(identity.path())?;
        identity.verify()?;
        let mut connection = Connection::open(identity.path()).map_err(failure)?;
        identity.verify()?;
        connection
            .busy_timeout(std::time::Duration::from_secs(5))
            .map_err(failure)?;
        connection
            .execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;")
            .map_err(failure)?;
        initialize_schema(&mut connection)?;
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
            database_path: identity.path().to_path_buf(),
            owner_state: Arc::new(Mutex::new(SqliteOwnerState {
                lock_file: Some(lock),
                epoch: None,
            })),
            operation_slot: Arc::new(sync::Semaphore::new(1)),
            typed_schema: false,
            #[cfg(test)]
            worker_counts: Arc::new(WorkerCounts::default()),
        })
    }

    /// Opens a database using the typed numeric-ID task schema.
    ///
    /// This cutover entry point creates a fresh schema or reopens the same
    /// typed schema. Legacy UUID databases are rejected with a migration
    /// diagnostic and left unchanged; converting their IDs requires an
    /// explicit mapping migration.
    pub fn open_next(path: impl AsRef<Path>) -> Result<Self, StoreError> {
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
            typed_schema: true,
            #[cfg(test)]
            worker_counts: Arc::new(WorkerCounts::default()),
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

impl LegacyTaskStore for SqliteTaskStore {
    /// Reports that history is persistent and unfinished records can recover.
    ///
    /// # Returns
    ///
    /// Both persistent-history and restart-recovery capabilities.
    fn capabilities(&self) -> StoreCapabilities {
        StoreCapabilities {
            persistent_history: true,
            restart_recovery: true,
        }
    }

    /// Atomically accepts an encoded typed request on a store opened with
    /// [`SqliteTaskStore::open_next`].
    fn accept_encoded<'a>(
        &'a self,
        id: crate::model::next::TaskId,
        request: crate::model::next::StoredTaskRequest,
    ) -> TaskFuture<'a, Result<crate::model::next::AcceptOutcome, StoreError>> {
        if !self.typed_schema {
            return Box::pin(async {
                Err(StoreError::Failure(
                    "typed task operations require SqliteTaskStore::open_next".into(),
                ))
            });
        }
        self.run_write(move |connection| accept_encoded(connection, id, request))
    }

    /// Loads an encoded typed task without decoding its application payload.
    fn get_encoded_task<'a>(
        &'a self,
        id: crate::model::next::TaskId,
    ) -> TaskFuture<'a, Result<Option<crate::model::next::StoredTask>, StoreError>> {
        if !self.typed_schema {
            return Box::pin(async {
                Err(StoreError::Failure(
                    "typed task operations require SqliteTaskStore::open_next".into(),
                ))
            });
        }
        self.run(move |connection| get_encoded_task(connection, id))
    }

    /// Persists a progress snapshot for the matching running attempt.
    fn update_progress<'a>(
        &'a self,
        command: crate::model::next::ProgressCommand,
    ) -> TaskFuture<'a, Result<crate::model::next::TaskSummary, StoreError>> {
        if !self.typed_schema {
            return Box::pin(async {
                Err(StoreError::Failure(
                    "typed task operations require SqliteTaskStore::open_next".into(),
                ))
            });
        }
        self.run_write(move |connection| update_progress(connection, command))
    }

    /// Starts one queued typed task attempt with a revision compare-and-set.
    fn start_encoded<'a>(
        &'a self,
        command: crate::model::next::StartCommand,
    ) -> TaskFuture<'a, Result<crate::model::next::TaskSummary, StoreError>> {
        if !self.typed_schema {
            return Box::pin(async {
                Err(StoreError::Failure(
                    "typed task operations require SqliteTaskStore::open_next".into(),
                ))
            });
        }
        self.run_write(move |connection| start_encoded(connection, command))
    }

    /// Applies a typed lifecycle transition atomically.
    fn transition_encoded<'a>(
        &'a self,
        command: crate::model::next::TransitionCommand,
    ) -> TaskFuture<'a, Result<crate::model::next::TaskSummary, StoreError>> {
        if !self.typed_schema {
            return Box::pin(async { Err(StoreError::UnsupportedCapability) });
        }
        self.run_write(move |connection| transition_encoded(connection, command))
    }

    /// Acceptance and idempotency lookup share one SQLite transaction.
    ///
    /// # Parameters
    ///
    /// * `id` - Service-generated identity for a new request.
    /// * `request` - Bounded immutable request to retain.
    ///
    /// # Returns
    ///
    /// A future resolving to a newly accepted or identical existing task.
    ///
    /// # Errors
    ///
    /// Returns validation, idempotency, or SQLite persistence errors.
    #[cfg(test)]
    fn accept<'a>(&'a self, id: TaskId, request: TaskRequest) -> TaskFuture<'a, Result<AcceptOutcome, StoreError>> {
        if self.typed_schema {
            return Box::pin(async { Err(StoreError::UnsupportedCapability) });
        }
        if let Err(error) = request.validate_limits() {
            return Box::pin(async move { Err(StoreError::InvalidRequest(error.message())) });
        }
        let initial = initial_record(id, request.clone());
        self.run_write(move |connection| {
            let transaction = connection.unchecked_transaction().map_err(failure)?;
            if let Some(key) = &request.idempotency_key {
                let stored = transaction.query_row("SELECT id,state_kind,accepted_at,correlation_key,idempotency_key,record_format_version,request_info_json,payload,lifecycle_json FROM tasks WHERE idempotency_key=?1", [key], read_stored_task_row).optional().map_err(failure)?;
                if let Some(row) = stored {
                    let record = decode_stored_task_row(row)?;
                    if record.request != request { return Err(StoreError::IdempotencyConflict); }
                    transaction.commit().map_err(failure)?;
                    return Ok(AcceptOutcome::Existing(record));
                }
            }
            let request_info_json = serde_json::to_string(&TaskRequestInfo::from(&request)).map_err(failure)?;
            let lifecycle_json = encode_lifecycle(&initial)?;
            transaction.execute("INSERT INTO tasks (id,state_kind,accepted_at,correlation_key,idempotency_key,request_info_json,payload,record_format_version,lifecycle_json) VALUES (?1,'Queued',?2,?3,?4,?5,?6,?7,?8)", params![id.to_string(), initial.accepted_at_ms, request.correlation_key, request.idempotency_key, request_info_json, request.payload, RECORD_FORMAT_VERSION, lifecycle_json]).map_err(failure)?;
            transaction.commit().map_err(failure)?;
            Ok(AcceptOutcome::Accepted(initial))
        })
    }

    /// Commits a version-checked lifecycle update without loading payload.
    ///
    /// # Parameters
    ///
    /// * `command` - Expected revision, state, resources, and result data.
    ///
    /// # Returns
    ///
    /// A future resolving to the committed payload-free summary.
    ///
    /// # Errors
    ///
    /// Returns missing, stale, invalid, oversized, or persistence errors.
    #[cfg(test)]
    fn transition<'a>(&'a self, command: TransitionCommand) -> TaskFuture<'a, Result<TaskSummary, StoreError>> {
        if self.typed_schema {
            return Box::pin(async { Err(StoreError::UnsupportedCapability) });
        }
        if let Err(error) = command.state.validate_diagnostics() {
            return Box::pin(async move { Err(StoreError::InvalidRequest(error)) });
        }
        if command
            .output
            .as_ref()
            .is_some_and(|output| output.summary.len() > MAX_TASK_OUTPUT_SUMMARY_BYTES)
        {
            return Box::pin(async {
                Err(StoreError::InvalidRequest(
                    "task output summary exceeds the 65536-byte limit",
                ))
            });
        }
        self.run_write(move |connection| {
            let transaction = connection.unchecked_transaction().map_err(failure)?;
            let stored = transaction
                .query_row(
                    &format!("SELECT {SUMMARY_COLUMNS} FROM tasks WHERE id=?1"),
                    [command.id.to_string()],
                    read_stored_summary_row,
                )
                .optional()
                .map_err(failure)?
                .ok_or(StoreError::NotFound)?;
            let mut record = decode_stored_summary_row(stored)?;
            if record.state_version != command.expected_version || record.attempt != command.expected_attempt {
                return Err(StoreError::Conflict);
            }
            if !record.state.allows_transition_to(&command.state) {
                return Err(StoreError::InvalidTransition);
            }
            if command.retry_not_before_ms.is_some() && !matches!(command.state, TaskState::Queued) {
                return Err(StoreError::InvalidRequest(
                    "only queued tasks may have a retry deadline",
                ));
            }
            let starting = !matches!(record.state, TaskState::Running) && matches!(command.state, TaskState::Running);
            record.state = command.state;
            record.retry_not_before_ms = command.retry_not_before_ms;
            record.state_version += 1;
            if starting {
                record.attempt += 1;
                record.started_at_ms = Some(now_ms());
            }
            if record.state.is_terminal() {
                record.finished_at_ms = Some(now_ms());
            }
            record.cancel_requested = command.cancel_requested;
            record.assigned_resources = command.assigned_resources;
            record.output = command.output;
            transaction
                .execute(
                    "UPDATE tasks SET state_kind=?2, lifecycle_json=?3 WHERE id=?1",
                    params![
                        record.id.to_string(),
                        state_kind(&record.state),
                        encode_summary_lifecycle(&record)?
                    ],
                )
                .map_err(failure)?;
            transaction.commit().map_err(failure)?;
            Ok(record)
        })
    }

    /// Loads the matching complete record, including its payload.
    ///
    /// # Parameters
    ///
    /// * `key` - Exact persisted idempotency key.
    ///
    /// # Returns
    ///
    /// A future resolving to the matching retained record, if present.
    ///
    /// # Errors
    ///
    /// Returns an error if the query or persisted row decoding fails.
    #[cfg(test)]
    fn get_by_idempotency_key<'a>(&'a self, key: &'a str) -> TaskFuture<'a, Result<Option<TaskRecord>, StoreError>> {
        if self.typed_schema {
            return Box::pin(async { Err(StoreError::UnsupportedCapability) });
        }
        let key = key.to_owned();
        self.run(move |connection| {
            let stored = connection
                .query_row(
                    "SELECT id,state_kind,accepted_at,correlation_key,idempotency_key,record_format_version,request_info_json,payload,lifecycle_json FROM tasks WHERE idempotency_key=?1",
                    [key],
                    read_stored_task_row,
                )
                .optional()
                .map_err(failure)?;
            stored.map(decode_stored_task_row).transpose()
        })
    }

    /// Reads lifecycle metadata using the payload-free summary projection.
    ///
    /// # Parameters
    ///
    /// * `key` - Exact persisted idempotency key to look up.
    ///
    /// # Returns
    ///
    /// A future resolving to the matching payload-free summary, if present.
    ///
    /// # Errors
    ///
    /// Returns an error if the query or persisted summary decoding fails.
    #[cfg(test)]
    fn get_summary_by_idempotency_key<'a>(
        &'a self,
        key: &'a str,
    ) -> TaskFuture<'a, Result<Option<TaskSummary>, StoreError>> {
        if self.typed_schema {
            return Box::pin(async { Err(StoreError::UnsupportedCapability) });
        }
        let key = key.to_owned();
        self.run(move |connection| {
            let stored = connection
                .query_row(
                    &format!("SELECT {SUMMARY_COLUMNS} FROM tasks WHERE idempotency_key=?1"),
                    [key],
                    read_stored_summary_row,
                )
                .optional()
                .map_err(failure)?;
            stored.map(decode_stored_summary_row).transpose()
        })
    }

    /// Loads one complete task record, including its payload bytes.
    ///
    /// # Parameters
    ///
    /// * `id` - Stable task identity.
    ///
    /// # Returns
    ///
    /// A future resolving to the retained record, if present.
    ///
    /// # Errors
    ///
    /// Returns an error if the query or persisted row decoding fails.
    #[cfg(test)]
    fn get<'a>(&'a self, id: TaskId) -> TaskFuture<'a, Result<Option<TaskRecord>, StoreError>> {
        if self.typed_schema {
            return Box::pin(async { Err(StoreError::UnsupportedCapability) });
        }
        Box::pin(async move {
            let stored = self
                .run(move |connection| {
                    connection
                        .query_row(
                            "SELECT id,state_kind,accepted_at,correlation_key,idempotency_key,record_format_version,request_info_json,payload,lifecycle_json FROM tasks WHERE id=?1",
                            [id.to_string()],
                            read_stored_task_row,
                        )
                        .optional()
                        .map_err(failure)
                })
                .await?;
            stored.map(decode_stored_task_row).transpose()
        })
    }

    /// Reads one ordered history page using metadata-only row projection.
    ///
    /// # Parameters
    ///
    /// * `query` - State, correlation, cursor, and page-size filters.
    ///
    /// # Returns
    ///
    /// A future resolving to payload-free summaries and a continuation.
    ///
    /// # Errors
    ///
    /// Returns invalid query or SQLite decoding errors.
    #[cfg(test)]
    fn list<'a>(&'a self, query: TaskQuery) -> TaskFuture<'a, Result<TaskPage, StoreError>> {
        if self.typed_schema {
            return Box::pin(async { Err(StoreError::UnsupportedCapability) });
        }
        self.run(move |connection| {
            let page_size = checked_page_size(query.limit)?;
            let built = build_history_query(&query, page_size)?;
            let mut statement = connection.prepare(&built.sql).map_err(failure)?;
            let mut rows = statement.query(params_from_iter(built.params)).map_err(failure)?;
            let mut records = Vec::new();
            while let Some(row) = rows.next().map_err(failure)? {
                records.push(decode_stored_summary_row(
                    read_stored_summary_row(row).map_err(failure)?,
                )?);
            }
            let has_more = records.len() > page_size;
            if has_more {
                records.truncate(page_size);
            }
            let next = has_more.then(|| records.last().map(TaskCursor::from)).flatten();
            Ok(TaskPage { records, next })
        })
    }

    fn list_encoded<'a>(&'a self, query: EncodedTaskQuery) -> TaskFuture<'a, Result<EncodedTaskPage, StoreError>> {
        if !self.typed_schema {
            return Box::pin(async { Err(StoreError::UnsupportedCapability) });
        }
        self.run(move |connection| list_encoded(connection, query))
    }

    fn list_ready_queued<'a>(
        &'a self,
        after: Option<crate::model::next::TaskCursor>,
        limit: NonZeroUsize,
        now_ms: u64,
    ) -> TaskFuture<'a, Result<EncodedTaskPage, StoreError>> {
        if !self.typed_schema {
            return Box::pin(async { Err(StoreError::UnsupportedCapability) });
        }
        self.run(move |connection| list_ready_queued(connection, after, limit, now_ms))
    }

    fn next_retry_deadline<'a>(&'a self, now_ms: u64) -> TaskFuture<'a, Result<Option<u64>, StoreError>> {
        if !self.typed_schema {
            return Box::pin(async { Err(StoreError::UnsupportedCapability) });
        }
        self.run(move |connection| next_retry_deadline(connection, now_ms))
    }

    fn prune_typed_terminal_before<'a>(
        &'a self,
        finished_before_ms: u64,
        max_rows: NonZeroUsize,
    ) -> TaskFuture<'a, Result<usize, StoreError>> {
        if !self.typed_schema {
            return Box::pin(async { Err(StoreError::UnsupportedCapability) });
        }
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
                .prepare("SELECT id FROM tasks WHERE state_kind IN ('Succeeded','Failed','Panicked','Cancelled') AND CAST(json_extract(lifecycle_json, '$.finished_at_ms') AS INTEGER) < ?1 ORDER BY CAST(json_extract(lifecycle_json, '$.finished_at_ms') AS INTEGER), id LIMIT ?2")
                .map_err(failure)?;
            let rows = statement
                .query_map(params![finished_before_ms, max_rows], |row| row.get::<_, String>(0))
                .map_err(failure)?;
            let ids = rows.collect::<Result<Vec<_>, _>>().map_err(failure)?;
            drop(statement);
            for id in &ids {
                transaction
                    .execute("DELETE FROM tasks WHERE id=?1", [id])
                    .map_err(failure)?;
            }
            transaction.commit().map_err(failure)?;
            Ok(ids.len())
        })
    }

    /// Loads task lifecycle and immutable metadata without the payload.
    ///
    /// # Parameters
    ///
    /// * `id` - Stable task identity.
    ///
    /// # Returns
    ///
    /// A future resolving to the retained summary, if present.
    ///
    /// # Errors
    ///
    /// Returns an error if the query or persisted summary decoding fails.
    #[cfg(test)]
    fn get_summary<'a>(&'a self, id: TaskId) -> TaskFuture<'a, Result<Option<TaskSummary>, StoreError>> {
        if self.typed_schema {
            return Box::pin(async { Err(StoreError::UnsupportedCapability) });
        }
        self.run(move |connection| {
            let stored = connection
                .query_row(
                    &format!("SELECT {SUMMARY_COLUMNS} FROM tasks WHERE id=?1"),
                    [id.to_string()],
                    read_stored_summary_row,
                )
                .optional()
                .map_err(failure)?;
            stored.map(decode_stored_summary_row).transpose()
        })
    }

    /// Deletes a bounded batch of old terminal rows and their idempotency keys.
    ///
    /// # Parameters
    ///
    /// * `accepted_before_ms` - Exclusive acceptance-time cutoff.
    /// * `max_rows` - Maximum rows removed by this call.
    ///
    /// # Returns
    ///
    /// A future resolving to the number of deleted records.
    ///
    /// # Errors
    ///
    /// Returns range, transaction, or SQLite errors.
    #[cfg(test)]
    fn prune_terminal_before<'a>(
        &'a self,
        accepted_before_ms: u64,
        max_rows: NonZeroUsize,
    ) -> TaskFuture<'a, Result<usize, StoreError>> {
        if self.typed_schema {
            return Box::pin(async { Err(StoreError::UnsupportedCapability) });
        }
        let accepted_before_ms = match i64::try_from(accepted_before_ms) {
            Ok(value) => value,
            Err(_) => {
                return Box::pin(async {
                    Err(StoreError::InvalidRequest(
                        "task history cutoff exceeds the SQLite integer range",
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
                .prepare("SELECT id FROM tasks WHERE state_kind IN ('Succeeded','Failed','Panicked','Cancelled') AND accepted_at < ?1 ORDER BY accepted_at, id LIMIT ?2")
                .map_err(failure)?;
            let rows = statement
                .query_map(params![accepted_before_ms, max_rows], |row| row.get::<_, String>(0))
                .map_err(failure)?;
            let ids = rows.collect::<Result<Vec<_>, _>>().map_err(failure)?;
            drop(statement);
            for id in &ids {
                transaction
                    .execute("DELETE FROM tasks WHERE id=?1", [id])
                    .map_err(failure)?;
            }
            transaction.commit().map_err(failure)?;
            Ok(ids.len())
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

    /// Checks queued and running rows through a payload-free SQL probe.
    ///
    /// # Parameters
    ///
    /// * `limit` - Maximum allowed unfinished record count.
    ///
    /// # Returns
    ///
    /// A future resolving to whether the count strictly exceeds the limit.
    ///
    /// # Errors
    ///
    /// Returns an error when SQLite cannot perform the probe.
    #[cfg(test)]
    fn has_unfinished_over_limit<'a>(&'a self, limit: usize) -> TaskFuture<'a, Result<bool, StoreError>> {
        if self.typed_schema {
            return Box::pin(async { Err(StoreError::UnsupportedCapability) });
        }
        if limit > i64::MAX as usize {
            return Box::pin(async { Ok(false) });
        }
        self.run(move |connection| {
            let mut statement = connection
                .prepare("SELECT 1 FROM tasks WHERE state_kind IN ('Queued','Running') LIMIT 1 OFFSET ?1")
                .map_err(failure)?;
            let mut rows = statement.query([limit as i64]).map_err(failure)?;
            Ok(rows.next().map_err(failure)?.is_some())
        })
    }

    /// Loads at most 256 unfinished summaries after an exclusive time/ID key.
    ///
    /// # Parameters
    ///
    /// * `cursor` - Exclusive acceptance-time/ID lower bound, or `None` for the
    ///   first page.
    ///
    /// # Returns
    ///
    /// A payload-free page strictly ordered by `(accepted_at_ms, id)`.
    /// Its next cursor equals the last row key only when more rows exist;
    /// full terminal pages and empty pages return `None`.
    ///
    /// # Errors
    ///
    /// Returns an error if a row is malformed or SQLite access fails.
    #[cfg(test)]
    fn scan_unfinished<'a>(&'a self, cursor: Option<TaskCursor>) -> TaskFuture<'a, Result<RecoveryPage, StoreError>> {
        if self.typed_schema {
            return Box::pin(async { Err(StoreError::UnsupportedCapability) });
        }
        self.run(move |connection| {
            let built = build_recovery_query(cursor)?;
            let mut statement = connection.prepare(&built.sql).map_err(failure)?;
            let mut rows = statement.query(params_from_iter(built.params)).map_err(failure)?;
            let mut tasks = Vec::new();
            while let Some(row) = rows.next().map_err(failure)? {
                tasks.push(decode_stored_summary_row(
                    read_stored_summary_row(row).map_err(failure)?,
                )?);
            }
            let has_more = tasks.len() > 256;
            if has_more {
                tasks.truncate(256);
            }
            let next = has_more.then(|| tasks.last().map(TaskCursor::from)).flatten();
            Ok(RecoveryPage { tasks, next })
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
        self.run(move |_| {
            let mut owner_state = owner_state.lock();
            if owner_state.epoch != Some(epoch) {
                return Err(StoreError::OwnerConflict);
            }
            let file = owner_state.lock_file.take().ok_or(StoreError::OwnerConflict)?;
            owner_state.epoch = None;
            file.unlock().map_err(failure)
        })
    }
}

/// Builds the initial queued record before the SQLite acceptance transaction.
///
/// # Parameters
///
/// * `id` - Service-assigned identity for the new task.
/// * `request` - Validated reconstructable request to accept.
///
/// # Returns
///
/// A queued record with initial lifecycle timestamps and revision.
#[cfg(test)]
fn initial_record(id: TaskId, request: TaskRequest) -> TaskRecord {
    TaskRecord {
        id,
        request,
        state: TaskState::Queued,
        state_version: 0,
        attempt: 0,
        retry_not_before_ms: None,
        accepted_at_ms: now_ms(),
        started_at_ms: None,
        finished_at_ms: None,
        assigned_resources: Vec::new(),
        output: None,
        cancel_requested: false,
    }
}

/// Maps a lifecycle state to its stable SQLite index key.
///
/// # Parameters
///
/// * `state` - Lifecycle value to map.
///
/// # Returns
///
/// The case-sensitive variant name used in the indexed state column.
#[cfg(test)]
fn state_kind(state: &TaskState) -> &'static str {
    state.kind().as_str()
}

/// Reads the current Unix epoch time in milliseconds, defaulting on clock
/// error.
///
/// # Returns
///
/// Current epoch milliseconds, or zero if the system clock predates the epoch.
#[cfg(test)]
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
/// Converts a SQLite, filesystem, or serialization diagnostic to store failure.
///
/// # Type Parameters
///
/// * `E` - Diagnostic value implementing `Display`.
///
/// # Parameters
///
/// * `error` - Underlying operation failure.
///
/// # Returns
///
/// A store failure preserving the diagnostic text.
fn failure(error: impl std::fmt::Display) -> StoreError {
    StoreError::Failure(error.to_string())
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
    use super::TaskId;
    use crate::model::legacy::AcceptOutcome;
    use crate::model::legacy::TaskQuery;
    use crate::model::legacy::TaskRequest;
    use crate::store::LegacyTaskStore;
    use crate::store::StoreError;

    /// Creates a unique database path for one worker scheduling test.
    fn test_database_path() -> std::path::PathBuf {
        std::env::temp_dir().join(format!("qubit-task-sqlite-worker-{}.sqlite", TaskId::generate()))
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
    async fn test_same_millisecond_pages_use_task_id_as_tie_breaker() {
        let path = test_database_path();
        let store = SqliteTaskStore::open(&path).expect("SQLite store opens");
        let ids = [TaskId::generate(), TaskId::generate()];
        for id in ids {
            assert!(matches!(
                store
                    .accept(id, TaskRequest::new("cursor-test", "1", Vec::new()))
                    .await
                    .expect("task accepted"),
                AcceptOutcome::Accepted(_)
            ));
        }
        store
            .run(move |connection| {
                for id in ids {
                    connection
                        .execute(
                            "UPDATE tasks SET accepted_at=42, lifecycle_json=json_set(lifecycle_json, '$.accepted_at_ms', 42) WHERE id=?1",
                            [id.to_string()],
                        )
                        .map_err(super::failure)?;
                }
                Ok(())
            })
            .await
            .expect("timestamps are aligned in the test database");

        let first = store
            .list(TaskQuery {
                limit: 1,
                ..TaskQuery::default()
            })
            .await
            .expect("first page succeeds");
        let second = store
            .list(TaskQuery {
                after: first.next,
                limit: 1,
                ..TaskQuery::default()
            })
            .await
            .expect("second page succeeds");
        assert_eq!(first.records[0].id, ids[0].min(ids[1]));
        assert_eq!(second.records[0].id, ids[0].max(ids[1]));
        drop(store);
        remove_database(&path);
    }

    #[tokio_crate::test]
    async fn test_pruning_rejects_values_outside_sqlite_integer_range() {
        let path = test_database_path();
        let store = SqliteTaskStore::open(&path).expect("SQLite store opens");
        let cutoff = store
            .prune_terminal_before(u64::MAX, std::num::NonZeroUsize::new(1).expect("positive limit"))
            .await;
        assert!(matches!(cutoff, Err(crate::store::StoreError::InvalidRequest(_))));
        let limit = store
            .prune_terminal_before(
                0,
                std::num::NonZeroUsize::new(i64::MAX as usize + 1).expect("positive limit"),
            )
            .await;
        assert!(matches!(limit, Err(crate::store::StoreError::InvalidRequest(_))));
        drop(store);
        remove_database(&path);
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

#[cfg(test)]
mod summary_query_tests {
    #[test]
    fn test_summary_projection_never_selects_payload() {
        assert!(!super::SUMMARY_COLUMNS.split(',').any(|column| column == "payload"));
        assert_eq!(super::SUMMARY_COLUMNS.split(',').count(), 8);
    }
}
