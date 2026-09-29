// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
// qubit-style: allow multiple-public-types
mod internal;

use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;

use internal::DatabaseIdentity;
use internal::SqliteOwnerState;
#[cfg(test)]
use internal::WorkerCounts;
#[cfg(test)]
use internal::WorkerGuard;
use internal::acquire_owner_lock;
use internal::decode_stored_summary_row;
use internal::decode_stored_task_row;
use internal::encode_lifecycle;
use internal::encode_summary_lifecycle;
use internal::initialize_schema;
use internal::read_stored_summary_row;
use internal::read_stored_task_row;
use parking_lot::Mutex;
use rusqlite::Connection;
use rusqlite::OptionalExtension;
use rusqlite::params;
use rusqlite::params_from_iter;
use rusqlite::types::Value;
use tokio::sync;
use tokio::task;

use super::StoreError;
use super::TaskFuture;
use super::TaskStore;
use crate::model::AcceptOutcome;
use crate::model::MAX_TASK_OUTPUT_SUMMARY_BYTES;
use crate::model::OwnerEpoch;
use crate::model::RecoveryPage;
use crate::model::StoreCapabilities;
use crate::model::TaskCursor;
use crate::model::TaskId;
use crate::model::TaskPage;
use crate::model::TaskQuery;
use crate::model::TaskRecord;
use crate::model::TaskRequest;
use crate::model::TaskRequestInfo;
use crate::model::TaskState;
use crate::model::TaskStateCounts;
use crate::model::TaskSummary;
use crate::model::TransitionCommand;
use crate::model::checked_page_size;

const SCHEMA_VERSION: i64 = 3;
const RECORD_FORMAT_VERSION: i64 = 3;
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
///
/// use qubit_task::model::TaskId;
/// use qubit_task::store::SqliteTaskStore;
/// use qubit_task::store::TaskStore;
///
/// let path = std::env::temp_dir().join(format!("qubit-task-{}.sqlite", TaskId::generate()));
/// let store = SqliteTaskStore::open(&path)?;
/// assert!(store.capabilities().restart_recovery);
/// drop(store);
/// fs::remove_file(&path)?;
/// let mut lock_path = path.as_os_str().to_owned();
/// lock_path.push(".owner.lock");
/// fs::remove_file(std::path::PathBuf::from(lock_path))?;
/// # Ok(())
/// # }
/// ```
pub struct SqliteTaskStore {
    /// SQLite connection serialized behind a synchronous mutex.
    connection: Arc<Mutex<Connection>>,
    /// Process-lock file and currently held recovery epoch.
    owner_state: Arc<Mutex<SqliteOwnerState>>,
    /// Bounds connection operations to one blocking worker at a time.
    operation_slot: Arc<sync::Semaphore>,
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
    /// epoch is acquired separately through [`TaskStore::acquire_owner`].
    ///
    /// # Errors
    ///
    /// Returns a store error when opening, locking, initializing, or migrating
    /// the database fails.
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
            owner_state: Arc::new(Mutex::new(SqliteOwnerState {
                lock_file: Some(lock),
                epoch: None,
            })),
            operation_slot: Arc::new(sync::Semaphore::new(1)),
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

impl TaskStore for SqliteTaskStore {
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
    fn accept<'a>(&'a self, id: TaskId, request: TaskRequest) -> TaskFuture<'a, Result<AcceptOutcome, StoreError>> {
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
    fn transition<'a>(&'a self, command: TransitionCommand) -> TaskFuture<'a, Result<TaskSummary, StoreError>> {
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
    fn get_by_idempotency_key<'a>(&'a self, key: &'a str) -> TaskFuture<'a, Result<Option<TaskRecord>, StoreError>> {
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
    fn get_summary_by_idempotency_key<'a>(
        &'a self,
        key: &'a str,
    ) -> TaskFuture<'a, Result<Option<TaskSummary>, StoreError>> {
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
    fn get<'a>(&'a self, id: TaskId) -> TaskFuture<'a, Result<Option<TaskRecord>, StoreError>> {
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
    fn list<'a>(&'a self, query: TaskQuery) -> TaskFuture<'a, Result<TaskPage, StoreError>> {
        self.run(move |connection| {
            let page_size = checked_page_size(query.limit)?;
            let fetch_limit = page_size
                .checked_add(1)
                .ok_or(StoreError::InvalidRequest("task history page limit is too large"))?;
            let fetch_limit = i64::try_from(fetch_limit)
                .map_err(|_| StoreError::InvalidRequest("task history page limit is too large"))?;
            let after_time = query
                .after
                .map(|cursor| i64::try_from(cursor.accepted_at_ms))
                .transpose()
                .map_err(|_| StoreError::InvalidRequest("task history cursor timestamp is too large"))?;
            let state_kinds = query.states.iter().map(|kind| kind.as_str()).collect::<Vec<_>>();
            let mut sql = String::from(
                &format!("SELECT {SUMMARY_COLUMNS} FROM tasks WHERE (?1 IS NULL OR accepted_at > ?1 OR (accepted_at = ?1 AND id > ?2)) AND (?3 IS NULL OR correlation_key = ?3)"),
            );
            if !state_kinds.is_empty() {
                let placeholders = (4..4 + state_kinds.len())
                    .map(|i| format!("?{i}"))
                    .collect::<Vec<_>>()
                    .join(",");
                sql.push_str(&format!(" AND state_kind IN ({placeholders})"));
            }
            sql.push_str(&format!(" ORDER BY accepted_at, id LIMIT ?{}", 4 + state_kinds.len()));
            let mut values = vec![
                after_time.map_or(Value::Null, Value::Integer),
                query.after.map(|cursor| cursor.id.to_string()).map_or(
                    Value::Null,
                    Value::Text,
                ),
                query
                    .correlation_key
                    .map_or(Value::Null, Value::Text),
            ];
            values.extend(
                state_kinds
                    .into_iter()
                    .map(|kind| Value::Text(kind.into())),
            );
            values.push(Value::Integer(fetch_limit));
            let mut statement = connection.prepare(&sql).map_err(failure)?;
            let mut rows = statement.query(params_from_iter(values)).map_err(failure)?;
            let mut records = Vec::new();
            while let Some(row) = rows.next().map_err(failure)? {
                records.push(decode_stored_summary_row(read_stored_summary_row(row).map_err(failure)?)?);
            }
            let has_more = records.len() > page_size;
            if has_more {
                records.truncate(page_size);
            }
            let next = has_more
                .then(|| records.last().map(TaskCursor::from))
                .flatten();
            Ok(TaskPage { records, next })
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
    fn get_summary<'a>(&'a self, id: TaskId) -> TaskFuture<'a, Result<Option<TaskSummary>, StoreError>> {
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

    /// Atomically cancels a blocked task at the caller's observed revision.
    ///
    /// # Parameters
    ///
    /// * `id` - Stable identity of the blocked task.
    /// * `expected_version` - State revision observed by the caller.
    ///
    /// # Returns
    ///
    /// A future resolving to the committed cancelled summary.
    ///
    /// # Errors
    ///
    /// Returns not-found, conflict, invalid-state, or SQLite errors.
    fn abandon_blocked<'a>(
        &'a self,
        id: TaskId,
        expected_version: u64,
    ) -> TaskFuture<'a, Result<TaskSummary, StoreError>> {
        self.run_write(move |connection| {
            let transaction = connection.unchecked_transaction().map_err(failure)?;
            let row = transaction
                .query_row(
                    &format!("SELECT {SUMMARY_COLUMNS} FROM tasks WHERE id=?1"),
                    [id.to_string()],
                    read_stored_summary_row,
                )
                .optional()
                .map_err(failure)?
                .ok_or(StoreError::NotFound)?;
            let mut record = decode_stored_summary_row(row)?;
            if record.state_version != expected_version {
                return Err(StoreError::Conflict);
            }
            if !matches!(record.state, TaskState::Blocked { .. }) {
                return Err(StoreError::InvalidTransition);
            }
            record.state = TaskState::Cancelled;
            record.state_version += 1;
            record.retry_not_before_ms = None;
            record.finished_at_ms = Some(now_ms());
            record.cancel_requested = false;
            record.assigned_resources.clear();
            transaction
                .execute(
                    "UPDATE tasks SET state_kind='Cancelled', lifecycle_json=?2 WHERE id=?1",
                    params![id.to_string(), encode_summary_lifecycle(&record)?],
                )
                .map_err(failure)?;
            transaction.commit().map_err(failure)?;
            Ok(record)
        })
    }

    /// Aggregates retained lifecycle categories in one SQL query.
    ///
    /// # Returns
    ///
    /// A future resolving to one consistent state-count snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error if SQLite reports an unknown state or count failure.
    fn count_states<'a>(&'a self) -> TaskFuture<'a, Result<TaskStateCounts, StoreError>> {
        self.run(|connection| {
            let mut statement = connection
                .prepare("SELECT state_kind, COUNT(*) FROM tasks GROUP BY state_kind")
                .map_err(failure)?;
            let mut rows = statement.query([]).map_err(failure)?;
            let mut counts = TaskStateCounts::default();
            while let Some(row) = rows.next().map_err(failure)? {
                let kind: String = row.get(0).map_err(failure)?;
                let count: i64 = row.get(1).map_err(failure)?;
                let count = usize::try_from(count).map_err(failure)?;
                match kind.as_str() {
                    "Queued" => counts.queued = count,
                    "Running" => counts.running = count,
                    "Blocked" => counts.blocked = count,
                    "Succeeded" | "Failed" | "Panicked" | "Cancelled" => {
                        counts.terminal = counts
                            .terminal
                            .checked_add(count)
                            .ok_or_else(|| StoreError::Failure("terminal state count exceeds usize".into()))?;
                    }
                    _ => {
                        return Err(StoreError::Failure(format!("unknown task state kind: {kind}")));
                    }
                }
            }
            Ok(counts)
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
    fn prune_terminal_before<'a>(
        &'a self,
        accepted_before_ms: u64,
        max_rows: NonZeroUsize,
    ) -> TaskFuture<'a, Result<usize, StoreError>> {
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
        self.run(move |connection| {
            let mut owner_state = owner_state.lock();
            if owner_state.epoch.is_some() {
                return Err(StoreError::OwnerConflict);
            }
            if owner_state.lock_file.is_none() {
                return Err(StoreError::OwnerConflict);
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
    fn has_unfinished_over_limit<'a>(&'a self, limit: usize) -> TaskFuture<'a, Result<bool, StoreError>> {
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

    /// Loads at most 256 unfinished records after an exclusive task ID.
    ///
    /// # Parameters
    ///
    /// * `cursor` - Last task ID returned by the preceding page.
    ///
    /// # Returns
    ///
    /// A future resolving to records and an optional next cursor.
    ///
    /// # Errors
    ///
    /// Returns an error if a row is malformed or SQLite access fails.
    fn scan_unfinished<'a>(&'a self, cursor: Option<TaskId>) -> TaskFuture<'a, Result<RecoveryPage, StoreError>> {
        self.run(move |connection| {
            let mut statement = connection.prepare(&format!("SELECT {SUMMARY_COLUMNS} FROM tasks WHERE state_kind IN ('Queued','Running') AND (?1 IS NULL OR id > ?1) ORDER BY id LIMIT 257")).map_err(failure)?;
            let mut rows = statement.query([cursor.map(|id| id.to_string())]).map_err(failure)?;
            let mut tasks = Vec::new();
            while let Some(row) = rows.next().map_err(failure)? {
                tasks.push(decode_stored_summary_row(read_stored_summary_row(row).map_err(failure)?)?);
            }
            let has_more = tasks.len() > 256;
            if has_more { tasks.truncate(256); }
            let next = has_more.then(|| tasks.last().map(|task| task.id)).flatten();
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
fn state_kind(state: &TaskState) -> &'static str {
    state.kind().as_str()
}

/// Reads the current Unix epoch time in milliseconds, defaulting on clock
/// error.
///
/// # Returns
///
/// Current epoch milliseconds, or zero if the system clock predates the epoch.
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

    use tokio as tokio_crate;
    use tokio::spawn;
    use tokio::sync::mpsc;
    use tokio::sync::oneshot;
    use tokio::task;
    use tokio::time;

    use super::SqliteTaskStore;
    use super::TaskId;
    use crate::model::AcceptOutcome;
    use crate::model::TaskQuery;
    use crate::model::TaskRequest;
    use crate::store::TaskStore;

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
