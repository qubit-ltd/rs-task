//! Durable task notification storage and publisher behavior.
#![cfg(feature = "sqlite")]

use qubit_task::store::{SqliteTaskStore, StoreError, TaskStore};

/// Creates an isolated temporary database path.
fn database_path() -> std::path::PathBuf {
    std::env::temp_dir().join(format!("task-outbox-{}.sqlite", uuid::Uuid::new_v4()))
}

#[tokio::test]
async fn test_outbox_requires_owner_and_valid_page_size() {
    let path = database_path();
    let store = SqliteTaskStore::open_next(&path).expect("open database");
    assert!(matches!(store.enable_event_outbox().await, Err(StoreError::OwnerConflict)));
    let epoch = store.acquire_owner().await.expect("acquire owner");
    store.enable_event_outbox().await.expect("enable outbox");
    assert!(store.list_event_outbox(128).await.expect("read page").is_empty());
    assert!(matches!(store.list_event_outbox(0).await, Err(StoreError::InvalidRequest(_))));
    store.release_owner(epoch).await.expect("release owner");
    assert!(matches!(store.list_event_outbox(128).await, Err(StoreError::OwnerConflict)));
}

#[tokio::test]
async fn test_schema_five_migrates_and_reopens_without_rewriting_tasks() {
    let path = database_path();
    let store = SqliteTaskStore::open_next(&path).expect("initialize database");
    let id = qubit_task::model::TaskId::from_id(qubit_id::Id::new(41));
    store.accept_encoded(id, request()).await.expect("persist existing task");
    drop(store);
    let connection = rusqlite::Connection::open(&path).expect("open fixture");
    let before: (String, Vec<u8>, String) = connection.query_row("SELECT request_info_json,payload,lifecycle_json FROM tasks", [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))).expect("original row");
    connection.execute_batch("DROP TABLE IF EXISTS task_event_outbox; PRAGMA user_version=5;").expect("simulate v5");
    drop(connection);
    drop(SqliteTaskStore::open_next(&path).expect("migrate v5"));
    drop(SqliteTaskStore::open_next(&path).expect("reopen v6"));
    let connection = rusqlite::Connection::open(&path).expect("inspect schema");
    let version: i64 = connection.pragma_query_value(None, "user_version", |row| row.get(0)).expect("version");
    assert_eq!(version, 6);
    let after: (String, Vec<u8>, String) = connection.query_row("SELECT request_info_json,payload,lifecycle_json FROM tasks", [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))).expect("migrated row");
    assert_eq!(before, after);
    let index: i64 = connection.query_row("SELECT COUNT(*) FROM sqlite_master WHERE name='task_event_outbox_created' AND type='index'", [], |row| row.get(0)).expect("index");
    assert_eq!(index, 1);
    connection.prepare("SELECT task_id,state_version,event_id,event_json,created_at_ms FROM task_event_outbox").expect("outbox schema");
}

#[test]
fn test_invalid_v6_outbox_is_rejected_without_repair() {
    let path = database_path();
    drop(SqliteTaskStore::open_next(&path).expect("initialize database"));
    let connection = rusqlite::Connection::open(&path).expect("open database");
    connection.execute_batch("ALTER TABLE task_event_outbox RENAME COLUMN event_json TO corrupt_json").expect("inject invalid schema");
    assert!(matches!(SqliteTaskStore::open_next(&path), Err(StoreError::Failure(_))));
    connection.prepare("SELECT corrupt_json FROM task_event_outbox").expect("invalid schema was preserved");
}

/// Encoded request independent of the event-bus feature.
fn request() -> qubit_task::model::StoredTaskRequest {
    use qubit_task::model::{ResourceRequest, StoredPayload, StoredTaskRequest};
    StoredTaskRequest {
        kind_id: "outbox.test".into(), category: None,
        payload: StoredPayload { type_id: "qubit_task.tests.Payload".try_into().expect("type ID"), schema_version: 1, codec_id: "json".into(), bytes: vec![1] },
        metadata: qubit_metadata::Metadata::new(), resource_limit: ResourceRequest::default(),
        correlation_key: None, idempotency_key: Some("same-task".into()),
    }
}

#[tokio::test]
async fn test_lifecycle_outbox_snapshots_are_atomic_and_idempotent() {
    use qubit_task::model::{TaskId, StartCommand, TaskState, TransitionCommand};
    let path = database_path();
    let store = SqliteTaskStore::open_next(&path).expect("open database");
    let epoch = store.acquire_owner().await.expect("owner");
    store.enable_event_outbox().await.expect("enable");
    let id = TaskId::from_id(qubit_id::Id::new(42));
    store.accept_encoded(id, request()).await.expect("accept");
    assert!(!store.accept_encoded(id, request()).await.expect("repeat").created);
    let running = store.start_encoded(StartCommand { id, expected_state_version: 0, started_at_ms: 10 }).await.expect("start");
    store.update_progress(qubit_task::model::ProgressCommand::new(id, running.attempt, 1, None, Vec::new(), 10)).await.expect("progress");
    assert_eq!(store.list_event_outbox(128).await.expect("after progress").len(), 2);
    let transition = TransitionCommand { id, expected_state_version: 1, expected_attempt: running.attempt, state: TaskState::Succeeded, cancel_requested: false, cancel_error: None, retry_not_before_ms: None, finished_at_ms: Some(11), output: None };
    let connection = rusqlite::Connection::open(&path).expect("fault injection connection");
    connection.execute_batch("CREATE TRIGGER fail_outbox BEFORE INSERT ON task_event_outbox BEGIN SELECT RAISE(ABORT, 'injected outbox failure'); END;").expect("inject failure");
    assert!(matches!(store.transition_encoded(transition.clone()).await, Err(StoreError::Failure(_))));
    assert_eq!(store.get_encoded_task(id).await.expect("get").expect("task").summary.state_version, 1);
    connection.execute_batch("DROP TRIGGER fail_outbox;").expect("restore outbox");
    store.transition_encoded(transition.clone()).await.expect("finish");
    assert!(matches!(store.transition_encoded(transition).await, Err(StoreError::Conflict)));
    let events = store.list_event_outbox(128).await.expect("events");
    assert_eq!(events.iter().map(|entry| entry.state_version).collect::<Vec<_>>(), vec![0,1,2]);
    assert_eq!(events[0].event_id, "task:42:0");
    assert!(events[0].event_json.contains("Queued"));
    assert!(events[1].event_json.contains("Running"));
    assert!(events[2].event_json.contains("Succeeded"));
    for _ in 0..2 { store.mark_event_published(id, 0).await.expect("idempotent deletion"); }
    assert_eq!(store.list_event_outbox(128).await.expect("remaining").len(), 2);
    store.release_owner(epoch).await.expect("release");
    assert!(matches!(store.mark_event_published(id, 1).await, Err(StoreError::OwnerConflict)));
}

#[test]
fn test_v6_missing_outbox_index_is_rejected() {
    let path = database_path();
    drop(SqliteTaskStore::open_next(&path).expect("initialize database"));
    let connection = rusqlite::Connection::open(&path).expect("open database");
    connection.execute_batch("DROP INDEX task_event_outbox_created").expect("remove index");
    assert!(matches!(SqliteTaskStore::open_next(&path), Err(StoreError::Failure(_))));
}

#[cfg(feature = "event-bus")]
#[path = "task_notification_outbox/publisher_tests.rs"]
mod publisher_tests;

#[tokio::test]
async fn test_oversized_event_rolls_back_acceptance() {
    let store = SqliteTaskStore::open_next(database_path()).expect("store");
    let owner = store.acquire_owner().await.expect("owner");
    store.enable_event_outbox().await.expect("enable");
    let id = qubit_task::model::TaskId::from_id(qubit_id::Id::new(43));
    let mut large = request();
    large.correlation_key = Some("x".repeat(128 * 1024));
    assert!(matches!(store.accept_encoded(id, large).await, Err(StoreError::InvalidRequest(_))));
    assert!(store.get_encoded_task(id).await.expect("lookup").is_none());
    assert!(store.list_event_outbox(128).await.expect("outbox").is_empty());
    store.release_owner(owner).await.expect("release");
}

#[test]
fn test_v6_missing_outbox_primary_key_is_rejected() {
    let path = database_path();
    drop(SqliteTaskStore::open_next(&path).expect("initialize database"));
    let connection = rusqlite::Connection::open(&path).expect("open database");
    connection.execute_batch("DROP TABLE task_event_outbox; CREATE TABLE task_event_outbox(task_id TEXT NOT NULL,state_version INTEGER NOT NULL,event_id TEXT NOT NULL,event_json TEXT NOT NULL,created_at_ms INTEGER NOT NULL); CREATE INDEX task_event_outbox_created ON task_event_outbox(created_at_ms,task_id,state_version);").expect("remove primary key from fixture");
    assert!(matches!(SqliteTaskStore::open_next(&path), Err(StoreError::Failure(_))));
}

#[test]
fn test_v6_outbox_index_on_another_table_is_rejected() {
    let path = database_path();
    drop(SqliteTaskStore::open_next(&path).expect("initialize database"));
    let connection = rusqlite::Connection::open(&path).expect("open database");
    connection.execute_batch(
        "DROP INDEX task_event_outbox_created;
         CREATE TABLE unrelated_events(created_at_ms INTEGER,task_id TEXT,state_version INTEGER);
         CREATE INDEX task_event_outbox_created ON unrelated_events(created_at_ms,task_id,state_version);",
    ).expect("install a same-named index on another table");
    let error = match SqliteTaskStore::open_next(&path) {
        Ok(_) => panic!("an index on another table cannot satisfy the outbox schema"),
        Err(error) => error,
    };
    assert!(matches!(error, StoreError::Failure(message) if message == "SQLite event outbox is missing its ordered index"));
    let indexed_table: String = connection.query_row(
        "SELECT tbl_name FROM sqlite_master WHERE type='index' AND name='task_event_outbox_created'",
        [], |row| row.get(0),
    ).expect("inspect preserved invalid index");
    assert_eq!(indexed_table, "unrelated_events", "opening must not silently repair invalid v6 data");
}
