// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use rusqlite::Connection;
use rusqlite::Transaction;

use super::super::failure;
use crate::store::StoreError;

/// Current schema version for typed numeric-ID task storage.
pub(in crate::store::sqlite_task_store) const NEXT_SCHEMA_VERSION: i64 = 8;

/// Initializes the typed numeric-ID schema or upgrades a supported typed
/// schema version in place.
///
/// Versions 4 through 7 are upgraded transactionally to version 8. A UUID-based
/// schema is rejected without changing its tables or rows.
///
/// # Parameters
///
/// * `connection` - Mutable SQLite connection for the schema transaction.
///
/// # Returns
///
/// Returns `Ok(())` after initialization or a supported upgrade commits.
///
/// # Errors
///
/// Returns an error for unsupported versions, invalid typed schemas, or
/// SQLite failures. The transaction rolls back if validation or a schema change
/// fails.
pub(in crate::store::sqlite_task_store) fn initialize_next_schema(
    connection: &mut Connection,
) -> Result<(), StoreError> {
    let version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .map_err(failure)?;
    if version != 0 && !(4..=NEXT_SCHEMA_VERSION).contains(&version) {
        return Err(StoreError::Failure(format!(
            "SQLite task schema version {version} uses the legacy UUID format; explicit task ID mapping is required before opening with the typed task API"
        )));
    }
    let transaction = connection.transaction().map_err(failure)?;
    let table_exists: bool = transaction
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='tasks')",
            [],
            |row| row.get(0),
        )
        .map_err(failure)?;
    if version == 4 {
        if !table_exists {
            return Err(StoreError::Failure(
                "typed SQLite task schema is missing table `tasks`".into(),
            ));
        }
        transaction
            .execute_batch("ALTER TABLE tasks ADD COLUMN retry_not_before_ms INTEGER;")
            .map_err(failure)?;
        transaction.pragma_update(None, "user_version", 5).map_err(failure)?;
    }
    if version >= 4 {
        if !table_exists {
            return Err(StoreError::Failure(
                "typed SQLite task schema is missing table `tasks`".into(),
            ));
        }
        validate_next_schema(&transaction)?;
    } else if table_exists {
        return Err(StoreError::Failure(
            "SQLite task database contains an unversioned legacy `tasks` table; explicit task ID mapping is required before opening with the typed task API".into(),
        ));
    } else {
        transaction
            .execute_batch(
                "CREATE TABLE tasks (
                    id TEXT PRIMARY KEY NOT NULL CHECK(length(id)=20 AND id NOT GLOB '*[^0-9]*'),
                    state_kind TEXT NOT NULL,
                    accepted_at INTEGER NOT NULL,
                    kind_id TEXT NOT NULL,
                    category TEXT,
                    payload_type_id TEXT NOT NULL,
                    payload_schema_version INTEGER NOT NULL,
                    codec_id TEXT NOT NULL,
                    correlation_key TEXT,
                    idempotency_key TEXT UNIQUE,
                    request_info_json TEXT NOT NULL,
                    payload BLOB NOT NULL,
                    record_format_version INTEGER NOT NULL,
                    lifecycle_json TEXT NOT NULL,
                    state_version INTEGER NOT NULL DEFAULT 0,
                    attempt INTEGER NOT NULL DEFAULT 0,
                    retry_not_before_ms INTEGER,
                    started_at INTEGER,
                    progress_attempt INTEGER,
                    progress_version INTEGER NOT NULL DEFAULT 0,
                    progress_json TEXT
                );
                CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY NOT NULL, value INTEGER NOT NULL);
                CREATE INDEX tasks_kind_category_accepted_id ON tasks(kind_id, category, accepted_at, id);
                CREATE INDEX tasks_category_accepted_id ON tasks(category, accepted_at, id);
                CREATE INDEX tasks_state_accepted_id ON tasks(state_kind, accepted_at, id);
                CREATE INDEX tasks_correlation_accepted_id ON tasks(correlation_key, accepted_at, id);
                CREATE INDEX tasks_unfinished_accepted_id ON tasks(accepted_at, id) WHERE state_kind IN ('Queued','Running');
                CREATE INDEX tasks_queued_accepted_id ON tasks(accepted_at, id) WHERE state_kind='Queued';
                CREATE INDEX tasks_queued_retry_deadline ON tasks(retry_not_before_ms) WHERE state_kind='Queued' AND retry_not_before_ms IS NOT NULL;
                CREATE INDEX tasks_terminal_finished_id ON tasks(CAST(json_extract(lifecycle_json, '$.finished_at_ms') AS INTEGER), id) WHERE state_kind IN ('Succeeded','Failed','Panicked','Cancelled');",
            )
            .map_err(failure)?;
        transaction
            .pragma_update(None, "user_version", NEXT_SCHEMA_VERSION)
            .map_err(failure)?;
    }
    transaction
        .execute_batch(
            "CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY NOT NULL, value INTEGER NOT NULL);
             CREATE INDEX IF NOT EXISTS tasks_kind_category_accepted_id ON tasks(kind_id, category, accepted_at, id);
             CREATE INDEX IF NOT EXISTS tasks_category_accepted_id ON tasks(category, accepted_at, id);
             CREATE INDEX IF NOT EXISTS tasks_state_accepted_id ON tasks(state_kind, accepted_at, id);
             CREATE INDEX IF NOT EXISTS tasks_correlation_accepted_id ON tasks(correlation_key, accepted_at, id);
             CREATE INDEX IF NOT EXISTS tasks_unfinished_accepted_id ON tasks(accepted_at, id) WHERE state_kind IN ('Queued','Running');
             CREATE INDEX IF NOT EXISTS tasks_queued_accepted_id ON tasks(accepted_at, id) WHERE state_kind='Queued';
             CREATE INDEX IF NOT EXISTS tasks_queued_retry_deadline ON tasks(retry_not_before_ms) WHERE state_kind='Queued' AND retry_not_before_ms IS NOT NULL;
             CREATE INDEX IF NOT EXISTS tasks_terminal_finished_id ON tasks(CAST(json_extract(lifecycle_json, '$.finished_at_ms') AS INTEGER), id) WHERE state_kind IN ('Succeeded','Failed','Panicked','Cancelled');",
        )
        .map_err(failure)?;
    if version == 0 || version == 4 || version == 5 {
        transaction
            .execute_batch(
                "CREATE TABLE task_event_outbox (
            task_id TEXT NOT NULL, state_version INTEGER NOT NULL,
            event_id TEXT NOT NULL, event_json TEXT NOT NULL, created_at_ms INTEGER NOT NULL,
            PRIMARY KEY(task_id,state_version));
            CREATE INDEX task_event_outbox_created ON task_event_outbox(created_at_ms,task_id,state_version);",
            )
            .map_err(failure)?;
        if version == 4 || version == 5 {
            transaction.pragma_update(None, "user_version", 6).map_err(failure)?;
        }
    }
    if version < 7 {
        transaction
            .execute_batch(
                "CREATE TABLE task_event_outbox_quarantine (
                    task_id TEXT NOT NULL,
                    state_version INTEGER NOT NULL,
                    event_id TEXT NOT NULL,
                    event_json TEXT NOT NULL,
                    created_at_ms INTEGER NOT NULL,
                    quarantined_at_ms INTEGER NOT NULL,
                    reason TEXT NOT NULL,
                    last_error TEXT NOT NULL,
                    PRIMARY KEY(task_id,state_version)
                );
                CREATE INDEX task_event_outbox_quarantine_archived
                    ON task_event_outbox_quarantine(quarantined_at_ms,task_id,state_version);",
            )
            .map_err(failure)?;
    }
    if version < NEXT_SCHEMA_VERSION {
        transaction
            .execute_batch(
                "CREATE TABLE task_terminal_checkpoints (
                task_id TEXT PRIMARY KEY NOT NULL,
                state_version INTEGER NOT NULL CHECK(state_version >= 0),
                state_json TEXT NOT NULL,
                finished_at_ms INTEGER NOT NULL CHECK(finished_at_ms >= 0)
            );",
            )
            .map_err(failure)?;
    }
    if version != NEXT_SCHEMA_VERSION {
        transaction
            .pragma_update(None, "user_version", NEXT_SCHEMA_VERSION)
            .map_err(failure)?;
    }
    validate_outbox_schema(&transaction)?;
    validate_quarantine_schema(&transaction)?;
    validate_checkpoint_schema(&transaction)?;
    transaction.commit().map_err(failure)
}

/// Validates the durable terminal checkpoint columns and primary key.
///
/// # Parameters
///
/// * `transaction` - Active transaction whose checkpoint schema is inspected.
///
/// # Returns
///
/// Returns `Ok(())` when the required columns, constraints, and lack of foreign
/// keys are valid.
///
/// # Errors
///
/// Returns an error if SQLite cannot inspect the schema or the checkpoint
/// table has invalid columns, constraints, or foreign keys.
fn validate_checkpoint_schema(transaction: &Transaction<'_>) -> Result<(), StoreError> {
    let mut statement = transaction
        .prepare("PRAGMA table_info(task_terminal_checkpoints)")
        .map_err(failure)?;
    let columns = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, i64>(5)?,
            ))
        })
        .map_err(failure)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(failure)?;
    for (name, kind, primary) in [
        ("task_id", "TEXT", 1),
        ("state_version", "INTEGER", 0),
        ("state_json", "TEXT", 0),
        ("finished_at_ms", "INTEGER", 0),
    ] {
        if !columns
            .iter()
            .any(|(column, ty, required, pk)| column == name && ty == kind && *required == 1 && *pk == primary)
        {
            return Err(StoreError::Failure(format!(
                "SQLite terminal checkpoints have invalid required column `{name}`"
            )));
        }
    }
    let has_foreign_key: bool = transaction
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM pragma_foreign_key_list('task_terminal_checkpoints'))",
            [],
            |row| row.get(0),
        )
        .map_err(failure)?;
    if has_foreign_key {
        return Err(StoreError::Failure(
            "SQLite terminal checkpoints must not have foreign keys".into(),
        ));
    }
    let definition: String = transaction
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='task_terminal_checkpoints'",
            [],
            |row| row.get(0),
        )
        .map_err(failure)?;
    let normalized_definition = definition
        .to_ascii_lowercase()
        .chars()
        .filter(|character| !character.is_whitespace())
        .collect::<String>();
    if !normalized_definition.contains("check(state_version>=0)")
        || !normalized_definition.contains("check(finished_at_ms>=0)")
    {
        return Err(StoreError::Failure(
            "SQLite terminal checkpoints are missing nonnegative value constraints".into(),
        ));
    }
    Ok(())
}

/// Validates the typed event archive columns, composite primary key, and index.
///
/// # Parameters
///
/// * `transaction` - Active transaction whose quarantine schema is inspected.
///
/// # Returns
///
/// Returns `Ok(())` when the required columns and ordered archive index are
/// valid.
///
/// # Errors
///
/// Returns an error if SQLite cannot inspect the schema or the quarantine
/// table has invalid columns or an invalid archive index.
fn validate_quarantine_schema(transaction: &Transaction<'_>) -> Result<(), StoreError> {
    let mut statement = transaction
        .prepare("PRAGMA table_info(task_event_outbox_quarantine)")
        .map_err(failure)?;
    let columns = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, i64>(5)?,
            ))
        })
        .map_err(failure)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(failure)?;
    for (name, kind, primary) in [
        ("task_id", "TEXT", 1),
        ("state_version", "INTEGER", 2),
        ("event_id", "TEXT", 0),
        ("event_json", "TEXT", 0),
        ("created_at_ms", "INTEGER", 0),
        ("quarantined_at_ms", "INTEGER", 0),
        ("reason", "TEXT", 0),
        ("last_error", "TEXT", 0),
    ] {
        if !columns
            .iter()
            .any(|(column, ty, required, pk)| column == name && ty == kind && *required == 1 && *pk == primary)
        {
            return Err(StoreError::Failure(format!(
                "SQLite event quarantine has invalid required column `{name}`"
            )));
        }
    }
    let owns_index: bool = transaction
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='index' AND name='task_event_outbox_quarantine_archived' AND tbl_name='task_event_outbox_quarantine')",
            [],
            |row| row.get(0),
        )
        .map_err(failure)?;
    if !owns_index {
        return Err(StoreError::Failure(
            "SQLite event quarantine is missing its archive index".into(),
        ));
    }
    let mut statement = transaction
        .prepare("PRAGMA index_info(task_event_outbox_quarantine_archived)")
        .map_err(failure)?;
    let index = statement
        .query_map([], |row| row.get::<_, String>(2))
        .map_err(failure)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(failure)?;
    if index != ["quarantined_at_ms", "task_id", "state_version"] {
        return Err(StoreError::Failure(
            "SQLite event quarantine is missing its archive index".into(),
        ));
    }
    Ok(())
}

/// Confirms that `tasks` contains every column required by the typed request
/// storage format. Extra columns are allowed so compatible local extensions
/// remain usable.
///
/// # Parameters
///
/// * `transaction` - Active transaction whose `tasks` table is inspected.
///
/// # Returns
///
/// Returns `Ok(())` when all required columns exist.
///
/// # Errors
///
/// Returns an error if SQLite cannot inspect the table or any required column
/// is missing.
fn validate_next_schema(transaction: &Transaction<'_>) -> Result<(), StoreError> {
    let columns = {
        let mut statement = transaction.prepare("PRAGMA table_info(tasks)").map_err(failure)?;
        let rows = statement
            .query_map([], |row| row.get::<_, String>(1))
            .map_err(failure)?;
        rows.collect::<Result<std::collections::HashSet<_>, _>>()
            .map_err(failure)?
    };
    for required in [
        "id",
        "state_kind",
        "accepted_at",
        "kind_id",
        "category",
        "payload_type_id",
        "payload_schema_version",
        "codec_id",
        "request_info_json",
        "payload",
        "record_format_version",
        "lifecycle_json",
        "state_version",
        "attempt",
        "retry_not_before_ms",
        "started_at",
        "progress_attempt",
        "progress_version",
        "progress_json",
    ] {
        if !columns.contains(required) {
            return Err(StoreError::Failure(format!(
                "typed SQLite task schema is missing required column `{required}`"
            )));
        }
    }
    Ok(())
}

/// Checks that the event outbox has its required columns and ordered index.
/// Existing outbox data is left untouched, including when the schema is
/// invalid.
///
/// # Parameters
///
/// * `transaction` - Active transaction whose outbox schema is inspected.
///
/// # Returns
///
/// Returns `Ok(())` when the outbox schema has the required ordering contract.
///
/// # Errors
///
/// Returns an error when the outbox columns or ordered index are invalid, or
/// when SQLite cannot inspect them.
fn validate_outbox_schema(transaction: &Transaction<'_>) -> Result<(), StoreError> {
    let mut statement = transaction
        .prepare("PRAGMA table_info(task_event_outbox)")
        .map_err(failure)?;
    let columns = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, i64>(5)?,
            ))
        })
        .map_err(failure)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(failure)?;
    for (name, kind, primary) in [
        ("task_id", "TEXT", 1),
        ("state_version", "INTEGER", 2),
        ("event_id", "TEXT", 0),
        ("event_json", "TEXT", 0),
        ("created_at_ms", "INTEGER", 0),
    ] {
        if !columns
            .iter()
            .any(|(column, ty, required, pk)| column == name && ty == kind && *required == 1 && *pk == primary)
        {
            return Err(StoreError::Failure(format!(
                "SQLite event outbox has invalid required column `{name}`"
            )));
        }
    }
    let owns_index: bool = transaction
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='index' AND name='task_event_outbox_created' AND tbl_name='task_event_outbox')",
            [],
            |row| row.get(0),
        )
        .map_err(failure)?;
    if !owns_index {
        return Err(StoreError::Failure(
            "SQLite event outbox is missing its ordered index".into(),
        ));
    }
    let mut statement = transaction
        .prepare("PRAGMA index_info(task_event_outbox_created)")
        .map_err(failure)?;
    let index = statement
        .query_map([], |row| row.get::<_, String>(2))
        .map_err(failure)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(failure)?;
    if index != ["created_at_ms", "task_id", "state_version"] {
        return Err(StoreError::Failure(
            "SQLite event outbox is missing its ordered index".into(),
        ));
    }
    Ok(())
}
