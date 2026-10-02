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

#[test]
fn test_schema_five_migrates_and_reopens_without_rewriting_tasks() {
    let path = database_path();
    drop(SqliteTaskStore::open_next(&path).expect("initialize database"));
    let connection = rusqlite::Connection::open(&path).expect("open fixture");
    connection.execute_batch("DROP TABLE IF EXISTS task_event_outbox; PRAGMA user_version=5;").expect("simulate v5");
    drop(connection);
    drop(SqliteTaskStore::open_next(&path).expect("migrate v5"));
    drop(SqliteTaskStore::open_next(&path).expect("reopen v6"));
    let connection = rusqlite::Connection::open(&path).expect("inspect schema");
    let version: i64 = connection.pragma_query_value(None, "user_version", |row| row.get(0)).expect("version");
    assert_eq!(version, 6);
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
