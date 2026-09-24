// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use fs2::FileExt;
use parking_lot::Mutex;
use rusqlite::Connection;
use rusqlite::OptionalExtension;

use super::StoreError;
use super::TaskFuture;
use super::TaskStore;
use crate::model::AcceptOutcome;
use crate::model::OwnerEpoch;
use crate::model::StoreCapabilities;
use crate::model::StoredTask;
use crate::model::StoredTaskPage;
use crate::model::TaskId;
use crate::model::TaskPage;
use crate::model::TaskQuery;
use crate::model::TaskRecord;
use crate::model::TaskRequest;
use crate::model::TaskState;
use crate::model::TaskStateCounts;
use crate::model::TransitionCommand;

/// SQLite-backed history with an exclusive OS lock for one active service
/// process.
pub struct SqliteTaskStore {
    connection: Arc<Mutex<Connection>>,
    owner_lock: Mutex<Option<File>>,
}

impl SqliteTaskStore {
    /// Opens a database, applies its schema, and reserves process ownership.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent().filter(|parent| !parent.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent).map_err(failure)?;
        }
        let lock_path = path.with_extension("owner.lock");
        let lock = File::options()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(lock_path)
            .map_err(failure)?;
        lock.try_lock_exclusive()
            .map_err(|error| StoreError::Failure(format!("database is owned by another service: {error}")))?;
        let connection = Connection::open(&path).map_err(failure)?;
        connection
            .busy_timeout(std::time::Duration::from_secs(5))
            .map_err(failure)?;
        connection.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; CREATE TABLE IF NOT EXISTS tasks (id TEXT PRIMARY KEY NOT NULL, state_kind TEXT NOT NULL, accepted_at INTEGER NOT NULL, correlation_key TEXT, idempotency_key TEXT UNIQUE, request_json TEXT NOT NULL, record_json TEXT NOT NULL); CREATE INDEX IF NOT EXISTS tasks_state_accepted ON tasks(state_kind, accepted_at); CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY NOT NULL, value INTEGER NOT NULL);").map_err(failure)?;
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
            owner_lock: Mutex::new(Some(lock)),
        })
    }

    fn run<'a, T, F>(&'a self, operation: F) -> TaskFuture<'a, Result<T, StoreError>>
    where
        T: Send + 'static,
        F: FnOnce(&Connection) -> Result<T, StoreError> + Send + 'static,
    {
        let connection = self.connection.clone();
        let (sender, receiver) = futures::channel::oneshot::channel();
        std::thread::spawn(move || {
            let result = operation(&connection.lock());
            let _ = sender.send(result);
        });
        Box::pin(async move {
            receiver
                .await
                .map_err(|_| StoreError::Failure("SQLite worker thread stopped".into()))?
        })
    }
}

impl TaskStore for SqliteTaskStore {
    fn capabilities(&self) -> StoreCapabilities {
        StoreCapabilities {
            persistent_history: true,
            restart_recovery: true,
        }
    }

    fn accept<'a>(&'a self, id: TaskId, request: TaskRequest) -> TaskFuture<'a, Result<AcceptOutcome, StoreError>> {
        let initial = initial_record(id, request.clone());
        self.run(move |connection| {
            let transaction = connection.unchecked_transaction().map_err(failure)?;
            if let Some(key) = &request.idempotency_key {
                let stored = transaction.query_row("SELECT record_json FROM tasks WHERE idempotency_key=?1", [key], |row| row.get::<_, String>(0)).optional().map_err(failure)?;
                if let Some(json) = stored {
                    let record: TaskRecord = serde_json::from_str(&json).map_err(failure)?;
                    if record.request != request { return Err(StoreError::IdempotencyConflict); }
                    transaction.commit().map_err(failure)?;
                    return Ok(AcceptOutcome::Existing(record));
                }
            }
            let request_json = serde_json::to_string(&request).map_err(failure)?;
            let record_json = serde_json::to_string(&initial).map_err(failure)?;
            transaction.execute("INSERT INTO tasks (id,state_kind,accepted_at,correlation_key,idempotency_key,request_json,record_json) VALUES (?1,'Queued',?2,?3,?4,?5,?6)", rusqlite::params![id.to_string(), initial.accepted_at_ms, request.correlation_key, request.idempotency_key, request_json, record_json]).map_err(failure)?;
            transaction.commit().map_err(failure)?;
            Ok(AcceptOutcome::Accepted(initial))
        })
    }

    fn transition<'a>(&'a self, command: TransitionCommand) -> TaskFuture<'a, Result<TaskRecord, StoreError>> {
        self.run(move |connection| {
            let transaction = connection.unchecked_transaction().map_err(failure)?;
            let json = transaction
                .query_row(
                    "SELECT record_json FROM tasks WHERE id=?1",
                    [command.id.to_string()],
                    |row| row.get::<_, String>(0),
                )
                .optional()
                .map_err(failure)?
                .ok_or(StoreError::NotFound)?;
            let mut record: TaskRecord = serde_json::from_str(&json).map_err(failure)?;
            if record.state_version != command.expected_version || record.attempt != command.expected_attempt {
                return Err(StoreError::Conflict);
            }
            if !record.state.allows_transition_to(&command.state) {
                return Err(StoreError::InvalidTransition);
            }
            let starting = !matches!(record.state, TaskState::Running) && matches!(command.state, TaskState::Running);
            record.state = command.state;
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
                    "UPDATE tasks SET state_kind=?2, record_json=?3 WHERE id=?1",
                    rusqlite::params![
                        record.id.to_string(),
                        state_kind(&record.state),
                        serde_json::to_string(&record).map_err(failure)?
                    ],
                )
                .map_err(failure)?;
            transaction.commit().map_err(failure)?;
            Ok(record)
        })
    }

    fn find_idempotent<'a>(&'a self, request: TaskRequest) -> TaskFuture<'a, Result<Option<TaskRecord>, StoreError>> {
        self.run(move |connection| {
            let Some(key) = request.idempotency_key.as_ref() else {
                return Ok(None);
            };
            let json = connection
                .query_row("SELECT record_json FROM tasks WHERE idempotency_key=?1", [key], |row| {
                    row.get::<_, String>(0)
                })
                .optional()
                .map_err(failure)?;
            match json {
                Some(json) => {
                    let record: TaskRecord = serde_json::from_str(&json).map_err(failure)?;
                    if record.request != request {
                        return Err(StoreError::IdempotencyConflict);
                    }
                    Ok(Some(record))
                }
                None => Ok(None),
            }
        })
    }

    fn get<'a>(&'a self, id: TaskId) -> TaskFuture<'a, Result<Option<TaskRecord>, StoreError>> {
        Box::pin(async move {
            let json = self
                .run(move |connection| {
                    connection
                        .query_row("SELECT record_json FROM tasks WHERE id=?1", [id.to_string()], |row| {
                            row.get::<_, String>(0)
                        })
                        .optional()
                        .map_err(failure)
                })
                .await?;
            json.map(|value| serde_json::from_str(&value).map_err(failure))
                .transpose()
        })
    }

    fn list<'a>(&'a self, query: TaskQuery) -> TaskFuture<'a, Result<TaskPage, StoreError>> {
        self.run(move |connection| {
            let state_kinds = query.states.iter().map(state_kind).collect::<Vec<_>>();
            let mut sql = String::from(
                "SELECT record_json FROM tasks WHERE (?1 IS NULL OR id > ?1) AND (?2 IS NULL OR correlation_key = ?2)",
            );
            if !state_kinds.is_empty() {
                let placeholders = (3..3 + state_kinds.len())
                    .map(|i| format!("?{i}"))
                    .collect::<Vec<_>>()
                    .join(",");
                sql.push_str(&format!(" AND state_kind IN ({placeholders})"));
            }
            sql.push_str(&format!(" ORDER BY id LIMIT ?{}", 3 + state_kinds.len()));
            let mut values = vec![
                query
                    .after
                    .map(|id| id.to_string())
                    .map_or(rusqlite::types::Value::Null, rusqlite::types::Value::Text),
                query
                    .correlation_key
                    .map_or(rusqlite::types::Value::Null, rusqlite::types::Value::Text),
            ];
            values.extend(
                state_kinds
                    .into_iter()
                    .map(|kind| rusqlite::types::Value::Text(kind.into())),
            );
            values.push(rusqlite::types::Value::Integer((query.limit.max(1) + 1) as i64));
            let mut statement = connection.prepare(&sql).map_err(failure)?;
            let mut rows = statement.query(rusqlite::params_from_iter(values)).map_err(failure)?;
            let mut records = Vec::new();
            while let Some(row) = rows.next().map_err(failure)? {
                records.push(
                    serde_json::from_str::<TaskRecord>(&row.get::<_, String>(0).map_err(failure)?).map_err(failure)?,
                );
            }
            let has_more = records.len() > query.limit.max(1);
            if has_more {
                records.truncate(query.limit.max(1));
            }
            let next = has_more.then(|| records.last().map(|record| record.id)).flatten();
            Ok(TaskPage { records, next })
        })
    }

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
                        counts.terminal = counts.terminal.checked_add(count).ok_or_else(|| {
                            StoreError::Failure("terminal state count exceeds usize".into())
                        })?;
                    }
                    _ => return Err(StoreError::Failure(format!("unknown task state kind: {kind}"))),
                }
            }
            Ok(counts)
        })
    }

    fn acquire_owner<'a>(&'a self) -> TaskFuture<'a, Result<OwnerEpoch, StoreError>> {
        if self.owner_lock.lock().is_none() {
            return Box::pin(async { Err(StoreError::Failure("SQLite owner lock has been released".into())) });
        }
        self.run(|connection| {
            connection.execute("INSERT INTO metadata(key,value) VALUES('owner_epoch',1) ON CONFLICT(key) DO UPDATE SET value=value+1", []).map_err(failure)?;
            connection.query_row("SELECT value FROM metadata WHERE key='owner_epoch'", [], |row| row.get::<_, u64>(0)).map(OwnerEpoch).map_err(failure)
        })
    }

    fn scan_unfinished<'a>(&'a self, cursor: Option<TaskId>) -> TaskFuture<'a, Result<StoredTaskPage, StoreError>> {
        self.run(move |connection| {
            let mut statement = connection.prepare("SELECT record_json FROM tasks WHERE state_kind IN ('Queued','Running') AND (?1 IS NULL OR id > ?1) ORDER BY id LIMIT 257").map_err(failure)?;
            let mut rows = statement.query([cursor.map(|id| id.to_string())]).map_err(failure)?;
            let mut tasks = Vec::new();
            while let Some(row) = rows.next().map_err(failure)? { tasks.push(StoredTask { record: serde_json::from_str(&row.get::<_, String>(0).map_err(failure)?).map_err(failure)? }); }
            let has_more = tasks.len() > 256;
            if has_more { tasks.truncate(256); }
            let next = has_more.then(|| tasks.last().map(|task| task.record.id)).flatten();
            Ok(StoredTaskPage { tasks, next })
        })
    }

    fn release_owner<'a>(&'a self, _epoch: OwnerEpoch) -> TaskFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            if let Some(file) = self.owner_lock.lock().take() {
                file.unlock().map_err(failure)?;
            }
            Ok(())
        })
    }
}

fn initial_record(id: TaskId, request: TaskRequest) -> TaskRecord {
    TaskRecord {
        id,
        request,
        state: TaskState::Queued,
        state_version: 0,
        attempt: 0,
        accepted_at_ms: now_ms(),
        started_at_ms: None,
        finished_at_ms: None,
        assigned_resources: Vec::new(),
        output: None,
        cancel_requested: false,
    }
}

fn state_kind(state: &TaskState) -> &'static str {
    match state {
        TaskState::Queued => "Queued",
        TaskState::Running => "Running",
        TaskState::Blocked { .. } => "Blocked",
        TaskState::Succeeded => "Succeeded",
        TaskState::Failed { .. } => "Failed",
        TaskState::Panicked { .. } => "Panicked",
        TaskState::Cancelled => "Cancelled",
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
fn failure(error: impl std::fmt::Display) -> StoreError {
    StoreError::Failure(error.to_string())
}
