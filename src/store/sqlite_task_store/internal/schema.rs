// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use rusqlite::Connection;
#[cfg(test)]
use rusqlite::OptionalExtension;
use rusqlite::Transaction;
#[cfg(test)]
use rusqlite::params;

#[cfg(test)]
use super::super::SCHEMA_VERSION;
use super::super::failure;
#[cfg(test)]
use super::super::state_kind;
#[cfg(test)]
use super::StoredLifecycle;
#[cfg(test)]
use super::row_codec::decode_legacy_record;
#[cfg(test)]
use super::row_codec::encode_lifecycle;
#[cfg(test)]
use crate::model::legacy::TaskRequest;
#[cfg(test)]
use crate::model::legacy::TaskRequestInfo;
use crate::store::StoreError;

/// Schema version for the numeric-ID typed request format.
pub(in crate::store::sqlite_task_store) const NEXT_SCHEMA_VERSION: i64 = 6;

/// Canonical recovery index SQL; equality makes reopen repairs idempotent.
#[cfg(test)]
const UNFINISHED_INDEX_SQL: &str =
    "CREATE INDEX tasks_unfinished_accepted_id ON tasks(accepted_at, id) WHERE +state_kind IN ('Queued','Running')";

/// Initializes the current SQLite schema or upgrades a supported older schema.
///
/// # Parameters
///
/// * `connection` - Mutable connection used for the schema transaction.
///
/// # Returns
///
/// Success after the current schema is ready.
///
/// # Errors
///
/// Returns a store error for unsupported versions, invalid schemas, migration
/// failures, or SQLite operation failures.
#[cfg(test)]
pub(in crate::store::sqlite_task_store) fn initialize_schema(connection: &mut Connection) -> Result<(), StoreError> {
    let version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .map_err(failure)?;
    if !(0..=SCHEMA_VERSION).contains(&version) {
        return Err(StoreError::Failure(format!(
            "unsupported SQLite task schema version {version}; supported version is {SCHEMA_VERSION}"
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
    if version == SCHEMA_VERSION {
        if !table_exists {
            return Err(StoreError::Failure(
                "SQLite task schema is missing table `tasks`".into(),
            ));
        }
        validate_schema_three(&transaction)?;
        ensure_indexes(&transaction)?;
        transaction
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY NOT NULL, value INTEGER NOT NULL);",
            )
            .map_err(failure)?;
        transaction.commit().map_err(failure)?;
        return Ok(());
    }
    if table_exists {
        if version <= 1 {
            migrate_legacy_schema(&transaction, version)?;
        }
        migrate_schema_two_to_three(&transaction)?;
    } else {
        transaction.execute_batch("CREATE TABLE tasks (id TEXT PRIMARY KEY NOT NULL, state_kind TEXT NOT NULL, accepted_at INTEGER NOT NULL, correlation_key TEXT, idempotency_key TEXT UNIQUE, request_info_json TEXT NOT NULL, payload BLOB NOT NULL, record_format_version INTEGER NOT NULL DEFAULT 3, lifecycle_json TEXT NOT NULL);").map_err(failure)?;
    }
    ensure_indexes(&transaction)?;
    transaction
        .execute_batch("CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY NOT NULL, value INTEGER NOT NULL);")
        .map_err(failure)?;
    transaction
        .pragma_update(None, "user_version", SCHEMA_VERSION)
        .map_err(failure)?;
    transaction.commit().map_err(failure)
}

/// Initializes the typed-request schema without rewriting legacy task data.
///
/// This entry point is intentionally separate from [`initialize_schema`]
/// during the API cutover. A database with any older task schema is rejected
/// with an explicit migration diagnostic; no UUID key or task record is
/// rewritten or removed.
pub(in crate::store::sqlite_task_store) fn initialize_next_schema(
    connection: &mut Connection,
) -> Result<(), StoreError> {
    let version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .map_err(failure)?;
    if version != 0 && version != 4 && version != 5 && version != NEXT_SCHEMA_VERSION {
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
        transaction
            .pragma_update(None, "user_version", 5)
            .map_err(failure)?;
    }
    if version == NEXT_SCHEMA_VERSION || version == 5 || version == 4 {
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
                CREATE INDEX tasks_queued_accepted_id ON tasks(accepted_at, id) WHERE state_kind='Queued';",
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
             CREATE INDEX IF NOT EXISTS tasks_queued_accepted_id ON tasks(accepted_at, id) WHERE state_kind='Queued';",
        )
        .map_err(failure)?;
    if version != NEXT_SCHEMA_VERSION {
        transaction.execute_batch("CREATE TABLE task_event_outbox (
            task_id TEXT NOT NULL, state_version INTEGER NOT NULL,
            event_id TEXT NOT NULL, event_json TEXT NOT NULL, created_at_ms INTEGER NOT NULL,
            PRIMARY KEY(task_id,state_version));
            CREATE INDEX task_event_outbox_created ON task_event_outbox(created_at_ms,task_id,state_version);").map_err(failure)?;
        transaction.pragma_update(None, "user_version", NEXT_SCHEMA_VERSION).map_err(failure)?;
    }
    validate_outbox_schema(&transaction)?;
    transaction.commit().map_err(failure)
}

/// Validates required columns for the typed request schema.
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

/// Ensures every history/recovery index exists in the schema transaction.
///
/// History indexes and record bytes are retained. An older recovery index
/// definition is replaced atomically in the caller's transaction. SQLite DDL
/// failures roll back every change for fresh, upgraded and version-3 databases.
#[cfg(test)]
fn ensure_indexes(transaction: &Transaction<'_>) -> Result<(), StoreError> {
    transaction
        .execute_batch(
            "CREATE INDEX IF NOT EXISTS tasks_state_accepted ON tasks(state_kind, accepted_at);
         CREATE INDEX IF NOT EXISTS tasks_accepted_id ON tasks(accepted_at, id);
         CREATE INDEX IF NOT EXISTS tasks_correlation_accepted_id ON tasks(correlation_key, accepted_at, id);
         CREATE INDEX IF NOT EXISTS tasks_state_accepted_id ON tasks(state_kind, accepted_at, id);",
        )
        .map_err(failure)?;
    ensure_unfinished_index(transaction)
}

/// Installs or atomically repairs the recovery index without rewriting tasks.
///
/// The canonical SQL is checked before DDL, so repeated opens do not rebuild
/// the index. A failed replacement is rolled back with the schema transaction.
#[cfg(test)]
fn ensure_unfinished_index(transaction: &Transaction<'_>) -> Result<(), StoreError> {
    let existing: Option<String> = transaction
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type='index' AND name='tasks_unfinished_accepted_id'",
            [],
            |row| row.get(0),
        )
        .optional()
        .map_err(failure)?;
    if existing.as_deref() == Some(UNFINISHED_INDEX_SQL) {
        return Ok(());
    }
    if existing.is_some() {
        transaction
            .execute_batch("DROP INDEX tasks_unfinished_accepted_id;")
            .map_err(failure)?;
    }
    transaction.execute_batch(UNFINISHED_INDEX_SQL).map_err(failure)
}

/// Verifies that schema 3 has the columns expected by the current store.
///
/// # Parameters
///
/// * `transaction` - Active schema transaction used to inspect the table.
///
/// # Returns
///
/// Success when every required column is present.
///
/// # Errors
///
/// Returns a store error when inspection fails or a required column is absent.
#[cfg(test)]
fn validate_schema_three(transaction: &Transaction<'_>) -> Result<(), StoreError> {
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
        "correlation_key",
        "idempotency_key",
        "request_info_json",
        "payload",
        "record_format_version",
        "lifecycle_json",
    ] {
        if !columns.contains(required) {
            return Err(StoreError::Failure(format!(
                "SQLite task schema is missing required column `{required}`"
            )));
        }
    }
    Ok(())
}

/// Migrates schema 2 request JSON into an indexed header and separate payload.
///
/// # Parameters
///
/// * `transaction` - Active schema transaction that owns the migration.
///
/// # Returns
///
/// Success after all rows are migrated to schema 3.
///
/// # Errors
///
/// Returns a store error when the old schema is invalid, a row cannot be
/// decoded, indexed values disagree, or a SQLite operation fails.
#[cfg(test)]
fn migrate_schema_two_to_three(transaction: &Transaction<'_>) -> Result<(), StoreError> {
    let columns = {
        let mut statement = transaction.prepare("PRAGMA table_info(tasks)").map_err(failure)?;
        let rows = statement
            .query_map([], |row| row.get::<_, String>(1))
            .map_err(failure)?;
        rows.collect::<Result<std::collections::HashSet<_>, _>>()
            .map_err(failure)?
    };
    if columns.contains("request_info_json") && columns.contains("payload") {
        return Ok(());
    }
    if !columns.contains("request_json") {
        return Err(StoreError::Failure("SQLite schema 2 is missing `request_json`".into()));
    }
    transaction.execute_batch("CREATE TABLE tasks_v3 (id TEXT PRIMARY KEY NOT NULL, state_kind TEXT NOT NULL, accepted_at INTEGER NOT NULL, correlation_key TEXT, idempotency_key TEXT UNIQUE, request_info_json TEXT NOT NULL, payload BLOB NOT NULL, record_format_version INTEGER NOT NULL DEFAULT 3, lifecycle_json TEXT NOT NULL);").map_err(failure)?;
    let mut statement = transaction.prepare("SELECT id,state_kind,accepted_at,correlation_key,idempotency_key,record_format_version,request_json,lifecycle_json FROM tasks ORDER BY id").map_err(failure)?;
    let mut rows = statement.query([]).map_err(failure)?;
    while let Some(row) = rows.next().map_err(failure)? {
        let id: String = row.get(0).map_err(failure)?;
        let state: String = row.get(1).map_err(failure)?;
        let accepted: i64 = row.get(2).map_err(failure)?;
        let correlation: Option<String> = row.get(3).map_err(failure)?;
        let key: Option<String> = row.get(4).map_err(failure)?;
        let format: i64 = row.get(5).map_err(failure)?;
        let request_json: String = row.get(6).map_err(failure)?;
        let lifecycle_json: String = row.get(7).map_err(failure)?;
        if format != 2 {
            return Err(StoreError::Failure(format!(
                "unsupported SQLite schema 2 record format {format}"
            )));
        }
        let request: TaskRequest = serde_json::from_str(&request_json).map_err(failure)?;
        let lifecycle: StoredLifecycle = serde_json::from_str(&lifecycle_json).map_err(failure)?;
        let old_record = lifecycle.into_record(request.clone());
        if request.correlation_key != correlation || request.idempotency_key != key {
            return Err(StoreError::Failure(format!(
                "SQLite schema 2 task row `{id}` disagrees with request_json"
            )));
        }
        if old_record.id.to_string() != id
            || state_kind(&old_record.state) != state
            || i64::try_from(old_record.accepted_at_ms).map_err(failure)? != accepted
        {
            return Err(StoreError::Failure(format!(
                "SQLite schema 2 task row `{id}` disagrees with lifecycle_json"
            )));
        }
        let info = serde_json::to_string(&TaskRequestInfo::from(&request)).map_err(failure)?;
        let lifecycle_json = encode_lifecycle(&old_record)?;
        transaction.execute("INSERT INTO tasks_v3 (id,state_kind,accepted_at,correlation_key,idempotency_key,request_info_json,payload,record_format_version,lifecycle_json) VALUES (?1,?2,?3,?4,?5,?6,?7,3,?8)", params![id, state, accepted, correlation, key, info, request.payload, lifecycle_json]).map_err(failure)?;
    }
    drop(rows);
    drop(statement);
    transaction
        .execute_batch("DROP TABLE tasks; ALTER TABLE tasks_v3 RENAME TO tasks;")
        .map_err(failure)?;
    Ok(())
}

/// Migrates a schema 0 or 1 database inside the caller's transaction.
///
/// # Parameters
///
/// * `transaction` - Active transaction that owns the migration.
/// * `schema_version` - Legacy database version being upgraded.
///
/// # Returns
///
/// Success after legacy records are represented in the schema 2 layout.
///
/// # Errors
///
/// Returns a store error when required columns are absent, records are invalid,
/// or a SQLite operation fails.
#[cfg(test)]
fn migrate_legacy_schema(transaction: &Transaction<'_>, schema_version: i64) -> Result<(), StoreError> {
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
        "correlation_key",
        "idempotency_key",
        "request_json",
        "record_json",
    ] {
        if !columns.contains(required) {
            return Err(StoreError::Failure(format!(
                "SQLite task schema is missing required column `{required}`"
            )));
        }
    }
    if schema_version == 1 && !columns.contains("record_format_version") {
        return Err(StoreError::Failure(
            "SQLite schema 1 is missing `record_format_version`".into(),
        ));
    }
    let migration_table_exists: bool = transaction
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='tasks_v2')",
            [],
            |row| row.get(0),
        )
        .map_err(failure)?;
    if migration_table_exists {
        return Err(StoreError::Failure(
            "SQLite schema migration table `tasks_v2` already exists".into(),
        ));
    }
    transaction.execute_batch("CREATE TABLE tasks_v2 (id TEXT PRIMARY KEY NOT NULL, state_kind TEXT NOT NULL, accepted_at INTEGER NOT NULL, correlation_key TEXT, idempotency_key TEXT UNIQUE, request_json TEXT NOT NULL, record_format_version INTEGER NOT NULL DEFAULT 2, lifecycle_json TEXT NOT NULL);").map_err(failure)?;
    let mut cursor: Option<String> = None;
    loop {
        let old = if columns.contains("record_format_version") {
            transaction.query_row("SELECT id,state_kind,accepted_at,correlation_key,idempotency_key,record_format_version,record_json FROM tasks WHERE (?1 IS NULL OR id>?1) ORDER BY id LIMIT 1", [cursor.as_deref()], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, i64>(2)?, row.get::<_, Option<String>>(3)?, row.get::<_, Option<String>>(4)?, row.get::<_, i64>(5)?, row.get::<_, String>(6)?))).optional().map_err(failure)?
        } else {
            transaction.query_row("SELECT id,state_kind,accepted_at,correlation_key,idempotency_key,1,record_json FROM tasks WHERE (?1 IS NULL OR id>?1) ORDER BY id LIMIT 1", [cursor.as_deref()], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, i64>(2)?, row.get::<_, Option<String>>(3)?, row.get::<_, Option<String>>(4)?, row.get::<_, i64>(5)?, row.get::<_, String>(6)?))).optional().map_err(failure)?
        };
        let Some((id, state, accepted_at, correlation, idempotency, format, json)) = old else {
            break;
        };
        let record = decode_legacy_record(format, &json)?;
        let accepted_at_ms = i64::try_from(record.accepted_at_ms).map_err(failure)?;
        if record.id.to_string() != id
            || state != state_kind(&record.state)
            || accepted_at != accepted_at_ms
            || correlation != record.request.correlation_key
            || idempotency != record.request.idempotency_key
        {
            return Err(StoreError::Failure(format!(
                "legacy SQLite task row `{id}` disagrees with its record_json"
            )));
        }
        let request_json = serde_json::to_string(&record.request).map_err(failure)?;
        let lifecycle_json = encode_lifecycle(&record)?;
        transaction.execute("INSERT INTO tasks_v2 (id,state_kind,accepted_at,correlation_key,idempotency_key,request_json,record_format_version,lifecycle_json) VALUES (?1,?2,?3,?4,?5,?6,2,?7)", params![id, state, accepted_at, correlation, idempotency, request_json, lifecycle_json]).map_err(failure)?;
        cursor = Some(id);
    }
    transaction
        .execute_batch("DROP TABLE tasks; ALTER TABLE tasks_v2 RENAME TO tasks;")
        .map_err(failure)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;
    use rusqlite::types::Value;

    use super::ensure_indexes;
    use super::failure;
    use super::initialize_schema;
    use crate::store::StoreError;

    /// A failure after rebuilding rolls back the real schema transaction
    /// completely.
    #[test]
    fn test_schema_unfinished_index_rebuild_rolls_back_on_later_sql_failure() {
        let mut connection = Connection::open_in_memory().expect("rollback database opens");
        initialize_schema(&mut connection).expect("rollback schema initializes");
        connection.execute_batch("INSERT INTO tasks (id,state_kind,accepted_at,request_info_json,payload,lifecycle_json) VALUES ('sentinel','Queued',42,'{}',X'00FF','{}'); INSERT INTO metadata VALUES ('sentinel',17); DROP INDEX tasks_unfinished_accepted_id; CREATE INDEX tasks_unfinished_accepted_id ON tasks(accepted_at,id) WHERE state_kind IN ('Queued','Running');").expect("old index and data seed");
        let original_sql: String = connection
            .query_row(
                "SELECT sql FROM sqlite_master WHERE name='tasks_unfinished_accepted_id'",
                [],
                |row| row.get(0),
            )
            .expect("original index SQL reads");
        let original_row: Vec<Value> = connection
            .query_row("SELECT * FROM tasks", [], |row| {
                (0..9).map(|column| row.get(column)).collect()
            })
            .expect("original task values read");
        let original_schema: i64 = connection
            .pragma_query_value(None, "schema_version", |row| row.get(0))
            .expect("original DDL revision reads");
        let result = (|| -> Result<(), StoreError> {
            let transaction = connection.transaction().map_err(failure)?;
            ensure_indexes(&transaction)?;
            let rebuilt_sql: String = transaction
                .query_row(
                    "SELECT sql FROM sqlite_master WHERE name='tasks_unfinished_accepted_id'",
                    [],
                    |row| row.get(0),
                )
                .map_err(failure)?;
            assert!(
                rebuilt_sql.contains("WHERE +state_kind"),
                "rebuild must happen before injected failure"
            );
            transaction
                .execute_batch("INSERT INTO metadata VALUES ('sentinel',99);")
                .map_err(failure)?;
            transaction.commit().map_err(failure)
        })();
        assert!(matches!(result, Err(StoreError::Failure(_))));
        let restored_sql: String = connection
            .query_row(
                "SELECT sql FROM sqlite_master WHERE name='tasks_unfinished_accepted_id'",
                [],
                |row| row.get(0),
            )
            .expect("restored index SQL reads");
        assert_eq!(restored_sql, original_sql);
        let restored_row: Vec<Value> = connection
            .query_row("SELECT * FROM tasks", [], |row| {
                (0..9).map(|column| row.get(column)).collect()
            })
            .expect("restored task values read");
        assert_eq!(restored_row, original_row);
        let metadata: (String, i64) = connection
            .query_row("SELECT key,value FROM metadata", [], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })
            .expect("restored metadata reads");
        assert_eq!(metadata, ("sentinel".into(), 17));
        let restored_schema: i64 = connection
            .pragma_query_value(None, "schema_version", |row| row.get(0))
            .expect("restored DDL revision reads");
        assert_eq!(restored_schema, original_schema);
        let version: i64 = connection
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .expect("restored user version reads");
        assert_eq!(version, 3);
    }
}

/// Rejects incomplete v6 databases without silently replacing their outbox.
fn validate_outbox_schema(transaction: &Transaction<'_>) -> Result<(), StoreError> {
    let mut statement = transaction.prepare("PRAGMA table_info(task_event_outbox)").map_err(failure)?;
    let columns = statement.query_map([], |row| Ok((row.get::<_, String>(1)?, row.get::<_, String>(2)?, row.get::<_, i64>(3)?, row.get::<_, i64>(5)?))).map_err(failure)?
        .collect::<Result<Vec<_>, _>>().map_err(failure)?;
    for (name, kind, primary) in [("task_id", "TEXT", 1), ("state_version", "INTEGER", 2), ("event_id", "TEXT", 0), ("event_json", "TEXT", 0), ("created_at_ms", "INTEGER", 0)] {
        if !columns.iter().any(|(column, ty, required, pk)| column == name && ty == kind && *required == 1 && *pk == primary) {
            return Err(StoreError::Failure(format!("SQLite event outbox has invalid required column `{name}`")));
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
        return Err(StoreError::Failure("SQLite event outbox is missing its ordered index".into()));
    }
    let mut statement = transaction.prepare("PRAGMA index_info(task_event_outbox_created)").map_err(failure)?;
    let index = statement.query_map([], |row| row.get::<_, String>(2)).map_err(failure)?
        .collect::<Result<Vec<_>, _>>().map_err(failure)?;
    if index != ["created_at_ms", "task_id", "state_version"] {
        return Err(StoreError::Failure("SQLite event outbox is missing its ordered index".into()));
    }
    Ok(())
}
