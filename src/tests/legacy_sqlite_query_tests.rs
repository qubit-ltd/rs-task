// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//    SPDX-License-Identifier: Apache-2.0
// =============================================================================
#![cfg(feature = "sqlite")]

use std::path::PathBuf;

use rusqlite::Connection;
use rusqlite::types::Value;

use crate::model::AcceptOutcome;
use crate::model::TaskId;
use crate::model::TaskRequest;
use crate::store::LegacyTaskStore as TaskStore;
use crate::store::SqliteTaskStore;

/// Owns a private temporary directory; cleanup never touches a caller path.
struct TestDatabase {
    directory: PathBuf,
    path: PathBuf,
}

impl TestDatabase {
    /// Creates a fresh directory and database name for one scenario.
    fn new() -> Self {
        let directory = std::env::temp_dir().join(format!("qubit-task-query-{}", TaskId::generate()));
        std::fs::create_dir(&directory).expect("unique test directory creates");
        Self {
            path: directory.join("tasks.sqlite"),
            directory,
        }
    }
}

impl Drop for TestDatabase {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

/// Captures every persisted task value without interpreting its encoding.
fn task_rows(connection: &Connection) -> Vec<Vec<Value>> {
    connection
        .prepare("SELECT * FROM tasks ORDER BY id")
        .expect("snapshot prepares")
        .query_map([], |row| {
            (0..9).map(|column| row.get(column)).collect::<Result<Vec<Value>, _>>()
        })
        .expect("snapshot executes")
        .collect::<Result<Vec<_>, _>>()
        .expect("snapshot reads")
}

/// Reads metadata in stable key order for lossless reopen comparison.
fn metadata(connection: &Connection) -> Vec<(String, i64)> {
    connection
        .prepare("SELECT key,value FROM metadata ORDER BY key")
        .expect("metadata prepares")
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .expect("metadata executes")
        .collect::<Result<Vec<_>, _>>()
        .expect("metadata reads")
}

/// Reopening schema three must install missing indexes atomically and
/// idempotently.
#[tokio::test]
async fn test_schema_three_reopen_adds_indexes_without_rewriting_records() {
    let database = TestDatabase::new();
    let store = SqliteTaskStore::open(&database.path).expect("fresh store opens");
    for index in 0..3 {
        let request =
            TaskRequest::new("query", "1", vec![index, 0, 255]).with_idempotency_key(format!("unique-{index}"));
        assert!(matches!(
            store.accept(TaskId::generate(), request).await.expect("record accepts"),
            AcceptOutcome::Accepted(_)
        ));
    }
    drop(store);
    let connection = Connection::open(&database.path).expect("test database inspects");
    connection.execute_batch("DROP INDEX IF EXISTS tasks_correlation_accepted_id; DROP INDEX IF EXISTS tasks_state_accepted_id; DROP INDEX IF EXISTS tasks_unfinished_accepted_id; INSERT INTO metadata (key,value) VALUES ('test-sentinel',42);").expect("old schema three fixture prepares");
    let rows_before = task_rows(&connection);
    let metadata_before = metadata(&connection);
    drop(connection);

    for _ in 0..2 {
        let reopened = SqliteTaskStore::open(&database.path).expect("old schema three reopens");
        drop(reopened);
        let connection = Connection::open(&database.path).expect("reopened database inspects");
        let version: i64 = connection
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .expect("version reads");
        assert_eq!(version, 3);
        assert_eq!(task_rows(&connection), rows_before);
        assert_eq!(metadata(&connection), metadata_before);
        assert_indexes(&connection);
    }
}

/// Produces UUIDs with predictable ordering rather than relying on randomness.
fn task_id(number: usize) -> TaskId {
    serde_json::from_str(&format!("\"00000000-0000-0000-0000-{number:012x}\"")).expect("fixed UUID decodes")
}

/// Seeds real lifecycles, then aligns the indexed timestamp and its JSON
/// mirror.
async fn seed_history(database: &TestDatabase) -> Vec<crate::model::TaskSummary> {
    use crate::model::TaskState;
    use crate::model::TransitionCommand;
    let store = SqliteTaskStore::open(&database.path).expect("history store opens");
    let mut summaries = Vec::new();
    for index in 0..300 {
        let id = task_id(300 - index);
        let mut request = TaskRequest::new("query", "1", vec![0, 255]);
        request.correlation_key = Some(if index % 2 == 0 { "key' OR 1=1 --" } else { "other" }.into());
        let record = match store.accept(id, request).await.expect("history acceptance commits") {
            AcceptOutcome::Accepted(record) => record,
            AcceptOutcome::Existing(_) => panic!("fixture IDs are unique"),
        };
        let mut summary = record.summary();
        if index % 3 != 0 {
            summary = store
                .transition(TransitionCommand {
                    id,
                    expected_version: record.state_version,
                    expected_attempt: record.attempt,
                    state: if index % 3 == 1 {
                        TaskState::Running
                    } else {
                        TaskState::Cancelled
                    },
                    retry_not_before_ms: None,
                    output: None,
                    assigned_resources: Vec::new(),
                    cancel_requested: false,
                })
                .await
                .expect("fixture lifecycle changes");
        }
        summary.accepted_at_ms = 1_000 + u64::try_from(index / 5).expect("fixture time fits");
        summaries.push(summary);
    }
    drop(store);
    let mut connection = Connection::open(&database.path).expect("timestamp fixture opens");
    let transaction = connection.transaction().expect("timestamp transaction starts");
    for summary in &summaries {
        transaction.execute("UPDATE tasks SET accepted_at=?1, lifecycle_json=json_set(lifecycle_json, '$.accepted_at_ms', ?1) WHERE id=?2",
            rusqlite::params![summary.accepted_at_ms, summary.id.to_string()]).expect("timestamp mirrors align");
    }
    transaction.commit().expect("timestamp fixture commits");
    summaries.sort_by_key(|summary| crate::model::TaskCursor::from(summary));
    summaries
}

/// Compares every bounded page and continuation against the immutable
/// reference.
async fn assert_history_pages(
    store: &SqliteTaskStore,
    query: crate::model::TaskQuery,
    summaries: &[crate::model::TaskSummary],
) {
    use crate::model::TaskCursor;
    let expected = summaries
        .iter()
        .filter(|summary| {
            query.after.is_none_or(|cursor| TaskCursor::from(*summary) > cursor)
                && query
                    .correlation_key
                    .as_ref()
                    .is_none_or(|key| summary.request.correlation_key.as_ref() == Some(key))
                && (query.states.is_empty() || query.states.contains(&summary.state.kind()))
        })
        .cloned()
        .collect::<Vec<_>>();
    let page_size = query.limit.max(1);
    let mut after = query.after;
    let mut actual = Vec::new();
    loop {
        let page = store
            .list(crate::model::TaskQuery { after, ..query.clone() })
            .await
            .expect("history page reads");
        let consumed = actual.len();
        let end = (consumed + page_size).min(expected.len());
        assert_eq!(page.records, expected[consumed..end]);
        assert_eq!(
            page.next,
            if end < expected.len() {
                page.records.last().map(TaskCursor::from)
            } else {
                None
            }
        );
        actual.extend(page.records);
        match page.next {
            Some(cursor) => {
                assert!(after.is_none_or(|previous| cursor > previous));
                after = Some(cursor);
            }
            None => break,
        }
    }
    assert_eq!(actual, expected, "pagination has neither gaps nor duplicates");
}

/// Verifies ties, bound values, all filter shapes, zero normalization and max
/// pages.
#[tokio::test]
async fn test_history_keyset_filters_and_limits_match_reference() {
    use crate::model::TaskCursor;
    use crate::model::TaskQuery;
    use crate::model::TaskStateKind;
    let database = TestDatabase::new();
    let summaries = seed_history(&database).await;
    let store = SqliteTaskStore::open(&database.path).expect("history store reopens");
    for limit in [0, 7, 256] {
        for states in [
            Vec::new(),
            vec![TaskStateKind::Running],
            vec![TaskStateKind::Queued, TaskStateKind::Running, TaskStateKind::Queued],
        ] {
            for correlation_key in [None, Some("key' OR 1=1 --".into()), Some("missing".into())] {
                for after in [None, Some(TaskCursor::from(&summaries[151]))] {
                    assert_history_pages(
                        &store,
                        TaskQuery {
                            limit,
                            states: states.clone(),
                            correlation_key: correlation_key.clone(),
                            after,
                        },
                        &summaries,
                    )
                    .await;
                }
            }
        }
    }
    // Recovery shares the ordering key, and excludes all terminal history.
    for after in [None, Some(TaskCursor::from(&summaries[151]))] {
        let expected = summaries
            .iter()
            .filter(|summary| {
                matches!(summary.state.kind(), TaskStateKind::Queued | TaskStateKind::Running)
                    && after.is_none_or(|cursor| TaskCursor::from(*summary) > cursor)
            })
            .cloned()
            .collect::<Vec<_>>();
        let page = store
            .scan_unfinished(after)
            .await
            .expect("filtered recovery page reads");
        assert_eq!(page.tasks, expected);
        assert_eq!(page.next, None, "fixture unfinished rows fit a terminal page");
    }
}

/// Verifies new indexes plus both retained legacy indexes are present.
fn assert_indexes(connection: &Connection) {
    for name in [
        "tasks_accepted_id",
        "tasks_state_accepted",
        "tasks_correlation_accepted_id",
        "tasks_state_accepted_id",
        "tasks_unfinished_accepted_id",
    ] {
        let exists: bool = connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='index' AND name=?1)",
                [name],
                |row| row.get(0),
            )
            .expect("index existence reads");
        assert!(exists, "initialization must install index {name}");
    }
}

/// Creates one supported legacy row using the existing public record encoding.
async fn seed_old_schema(database: &TestDatabase, version: i64) -> crate::model::TaskRecord {
    use crate::store::MemoryTaskStore;
    let memory = MemoryTaskStore::new(1);
    let request = TaskRequest::new("legacy-query", "1", vec![0, 42, 255]).with_idempotency_key("old-key");
    let record = match memory
        .accept(TaskId::generate(), request)
        .await
        .expect("reference record accepts")
    {
        AcceptOutcome::Accepted(record) => record,
        AcceptOutcome::Existing(_) => panic!("fixture starts empty"),
    };
    let connection = Connection::open(&database.path).expect("legacy fixture opens");
    if version <= 1 {
        connection.execute_batch("CREATE TABLE tasks (id TEXT PRIMARY KEY NOT NULL, state_kind TEXT NOT NULL, accepted_at INTEGER NOT NULL, correlation_key TEXT, idempotency_key TEXT UNIQUE, request_json TEXT NOT NULL, record_json TEXT NOT NULL);").expect("legacy tasks create");
        if version == 1 {
            connection
                .execute_batch("ALTER TABLE tasks ADD COLUMN record_format_version INTEGER NOT NULL DEFAULT 1;")
                .expect("version one column adds");
        }
        connection.execute("INSERT INTO tasks (id,state_kind,accepted_at,correlation_key,idempotency_key,request_json,record_json) VALUES (?1,'Queued',?2,NULL,?3,?4,?5)",
            rusqlite::params![record.id.to_string(), record.accepted_at_ms, record.request.idempotency_key, serde_json::to_string(&record.request).expect("request encodes"), serde_json::to_string(&record).expect("record encodes")]).expect("legacy row inserts");
    } else {
        connection.execute_batch("CREATE TABLE tasks (id TEXT PRIMARY KEY NOT NULL, state_kind TEXT NOT NULL, accepted_at INTEGER NOT NULL, correlation_key TEXT, idempotency_key TEXT UNIQUE, request_json TEXT NOT NULL, record_format_version INTEGER NOT NULL DEFAULT 2, lifecycle_json TEXT NOT NULL);").expect("version two tasks create");
        let mut lifecycle = serde_json::to_value(&record).expect("lifecycle encodes");
        lifecycle
            .as_object_mut()
            .expect("record encodes as object")
            .remove("request");
        connection.execute("INSERT INTO tasks (id,state_kind,accepted_at,correlation_key,idempotency_key,request_json,lifecycle_json) VALUES (?1,'Queued',?2,NULL,?3,?4,?5)",
            rusqlite::params![record.id.to_string(), record.accepted_at_ms, record.request.idempotency_key, serde_json::to_string(&record.request).expect("request encodes"), serde_json::to_string(&lifecycle).expect("lifecycle encodes")]).expect("version two row inserts");
    }
    connection.execute_batch("CREATE TABLE metadata (key TEXT PRIMARY KEY NOT NULL, value INTEGER NOT NULL); INSERT INTO metadata VALUES ('sentinel',99);").expect("metadata creates");
    connection
        .pragma_update(None, "user_version", version)
        .expect("legacy version sets");
    record
}

/// All supported migration branches create the same indexes without losing
/// data.
#[tokio::test]
async fn test_schema_zero_one_two_migrations_ensure_indexes() {
    for version in 0..=2 {
        let database = TestDatabase::new();
        let before = seed_old_schema(&database, version).await;
        let store = SqliteTaskStore::open(&database.path).expect("legacy schema migrates");
        assert_eq!(store.get(before.id).await.expect("migrated record reads"), Some(before));
        drop(store);
        let connection = Connection::open(&database.path).expect("migrated database inspects");
        assert_indexes(&connection);
        assert_eq!(metadata(&connection), vec![("sentinel".into(), 99)]);
        let version: i64 = connection
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .expect("schema version reads");
        assert_eq!(version, 3);
        let format: i64 = connection
            .query_row("SELECT record_format_version FROM tasks", [], |row| row.get(0))
            .expect("record format reads");
        assert_eq!(format, 3);
    }
}

/// Index creation failure rolls back earlier DDL and leaves all record bytes
/// intact.
#[tokio::test]
async fn test_schema_index_installation_failure_is_atomic() {
    let database = TestDatabase::new();
    let store = SqliteTaskStore::open(&database.path).expect("fresh store opens");
    assert!(matches!(
        store
            .accept(TaskId::generate(), TaskRequest::new("atomic-index", "1", vec![255]))
            .await
            .expect("task accepts"),
        AcceptOutcome::Accepted(_)
    ));
    drop(store);
    let connection = Connection::open(&database.path).expect("atomic fixture opens");
    connection.execute_batch("DROP INDEX tasks_correlation_accepted_id; DROP INDEX tasks_state_accepted_id; DROP INDEX tasks_unfinished_accepted_id; CREATE TABLE tasks_state_accepted_id (sentinel TEXT); INSERT INTO tasks_state_accepted_id VALUES ('preserve');").expect("index-name collision prepares");
    let before = task_rows(&connection);
    let metadata_before = metadata(&connection);
    drop(connection);
    assert!(
        matches!(
            SqliteTaskStore::open(&database.path),
            Err(crate::store::StoreError::Failure(_))
        ),
        "name collision must reject initialization"
    );
    let connection = Connection::open(&database.path).expect("failed initialization inspects");
    assert_eq!(task_rows(&connection), before);
    assert_eq!(metadata(&connection), metadata_before);
    for name in [
        "tasks_correlation_accepted_id",
        "tasks_state_accepted_id",
        "tasks_unfinished_accepted_id",
    ] {
        let exists: bool = connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='index' AND name=?1)",
                [name],
                |row| row.get(0),
            )
            .expect("rollback index reads");
        assert!(!exists, "failed transaction must not leave {name}");
    }
    let sentinel: String = connection
        .query_row("SELECT sentinel FROM tasks_state_accepted_id", [], |row| row.get(0))
        .expect("collision table reads");
    assert_eq!(sentinel, "preserve");
    let version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .expect("failed schema version reads");
    assert_eq!(version, 3);
}

/// Returns one named index's persisted definition for atomic rebuild
/// assertions.
fn unfinished_index_sql(connection: &Connection) -> String {
    connection
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type='index' AND name='tasks_unfinished_accepted_id'",
            [],
            |row| row.get(0),
        )
        .expect("partial index definition reads")
}

/// Old version-three definitions are rebuilt once while every persisted value
/// survives.
#[tokio::test]
async fn test_schema_rebuilds_old_unfinished_index_once_without_data_changes() {
    let database = TestDatabase::new();
    let store = SqliteTaskStore::open(&database.path).expect("index fixture opens");
    assert!(matches!(
        store
            .accept(TaskId::generate(), TaskRequest::new("reindex", "1", vec![0, 255]))
            .await
            .expect("fixture accepts"),
        AcceptOutcome::Accepted(_)
    ));
    drop(store);
    let connection = Connection::open(&database.path).expect("old index fixture opens");
    connection.execute_batch("DROP INDEX tasks_unfinished_accepted_id; CREATE INDEX tasks_unfinished_accepted_id ON tasks(accepted_at,id) WHERE state_kind IN ('Queued','Running'); INSERT INTO metadata VALUES ('reindex-sentinel',17);").expect("old index definition seeds");
    let rows_before = task_rows(&connection);
    let metadata_before = metadata(&connection);
    drop(connection);
    let mut first_schema_version = None;
    let mut first_definition = None;
    for _ in 0..2 {
        let store = SqliteTaskStore::open(&database.path).expect("old definition upgrades");
        drop(store);
        let connection = Connection::open(&database.path).expect("upgraded index inspects");
        assert_eq!(task_rows(&connection), rows_before);
        assert_eq!(metadata(&connection), metadata_before);
        assert_indexes(&connection);
        let definition = unfinished_index_sql(&connection);
        assert!(
            definition.contains("WHERE +state_kind IN ('Queued','Running')"),
            "recovery index must match unary query predicate: {definition}"
        );
        let schema_version: i64 = connection
            .pragma_query_value(None, "schema_version", |row| row.get(0))
            .expect("DDL revision reads");
        if let Some(first) = first_schema_version {
            assert_eq!(schema_version, first, "second open must execute no index DDL");
        } else {
            first_schema_version = Some(schema_version);
            first_definition = Some(definition.clone());
        }
        assert_eq!(Some(definition), first_definition);
        let user_version: i64 = connection
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .expect("user version reads");
        assert_eq!(user_version, 3);
        let statistics: bool = connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name LIKE 'sqlite_stat%')",
                [],
                |row| row.get(0),
            )
            .expect("statistics absence reads");
        assert!(!statistics);
    }
}

/// Memory history filtering and SQLite recovery agree on every public state
/// variant.
#[tokio::test]
async fn test_recovery_unary_membership_matches_memory_for_all_task_states() {
    use crate::model::TaskQuery;
    use crate::model::TaskState;
    use crate::model::TaskStateKind;
    use crate::model::TransitionCommand;
    use crate::store::MemoryTaskStore;
    let database = TestDatabase::new();
    let sqlite = SqliteTaskStore::open(&database.path).expect("state membership SQLite opens");
    let memory = MemoryTaskStore::new(10);
    let targets = [
        TaskState::Queued,
        TaskState::Running,
        TaskState::Blocked { reason: "test".into() },
        TaskState::Succeeded,
        TaskState::Failed {
            category: "test".into(),
            message: "test".into(),
        },
        TaskState::Panicked { message: "test".into() },
        TaskState::Cancelled,
    ];
    let mut expected = Vec::new();
    for (index, target) in targets.into_iter().enumerate() {
        let id = task_id(index + 1);
        if matches!(target, TaskState::Queued | TaskState::Running) {
            expected.push(id);
        }
        for store in [&memory as &dyn TaskStore, &sqlite as &dyn TaskStore] {
            let record = match store
                .accept(id, TaskRequest::new("state-membership", "1", vec![]))
                .await
                .expect("membership accepts")
            {
                AcceptOutcome::Accepted(record) => record,
                AcceptOutcome::Existing(_) => panic!("membership IDs are unique"),
            };
            let mut current = record.summary();
            let states = if matches!(
                target,
                TaskState::Succeeded | TaskState::Failed { .. } | TaskState::Panicked { .. }
            ) {
                vec![TaskState::Running, target.clone()]
            } else if target == TaskState::Queued {
                Vec::new()
            } else {
                vec![target.clone()]
            };
            for state in states {
                current = store
                    .transition(TransitionCommand {
                        id,
                        expected_version: current.state_version,
                        expected_attempt: current.attempt,
                        state,
                        retry_not_before_ms: None,
                        output: None,
                        assigned_resources: Vec::new(),
                        cancel_requested: false,
                    })
                    .await
                    .expect("membership transitions");
            }
        }
    }
    let mut memory_ids = memory
        .list(TaskQuery {
            states: vec![TaskStateKind::Queued, TaskStateKind::Running],
            limit: 256,
            ..TaskQuery::default()
        })
        .await
        .expect("memory unfinished filter reads")
        .records
        .into_iter()
        .map(|row| row.id)
        .collect::<Vec<_>>();
    let mut sqlite_ids = sqlite
        .scan_unfinished(None)
        .await
        .expect("SQLite unfinished recovery reads")
        .tasks
        .into_iter()
        .map(|row| row.id)
        .collect::<Vec<_>>();
    expected.sort();
    memory_ids.sort();
    sqlite_ids.sort();
    assert_eq!(memory_ids, expected);
    assert_eq!(sqlite_ids, memory_ids);
}
