use std::env;
use std::fs;
use std::num::NonZeroUsize;
use std::path::Path;

use qubit_id::Id;
use qubit_metadata::Metadata;
use qubit_model_id::ModelIdBuf;
use qubit_task::model::ResourceRequest;
use qubit_task::model::StartCommand;
use qubit_task::model::StoredPayload;
use qubit_task::model::StoredTaskRequest;
use qubit_task::model::TaskId;
use qubit_task::model::TaskState;
use qubit_task::model::TransitionCommand;
use qubit_task::store::SqliteTaskStore;
use qubit_task::store::TaskStore;
use rusqlite::Connection;
use uuid::Uuid;

fn request(value: u64) -> StoredTaskRequest {
    StoredTaskRequest {
        kind_id: "sqlite.ready.test".into(),
        category: None,
        payload: StoredPayload {
            type_id: ModelIdBuf::parse("qubit_task.tests.Payload").unwrap(),
            schema_version: 1,
            codec_id: "qubit.bytes.json".into(),
            bytes: vec![value as u8],
        },
        metadata: Metadata::new(),
        resource_limit: ResourceRequest::default(),
        correlation_key: None,
        idempotency_key: None,
    }
}

fn id(value: u64) -> TaskId {
    TaskId::from_id(Id::new(value))
}

async fn queued_with_deadline(store: &SqliteTaskStore, value: u64, deadline: Option<u64>) {
    let accepted = store
        .accept_encoded(id(value), request(value))
        .await
        .unwrap();
    let running = store
        .start_encoded(StartCommand {
            id: id(value),
            expected_state_version: accepted.summary.state_version,
            started_at_ms: 10,
        })
        .await
        .unwrap();
    store
        .transition_encoded(TransitionCommand {
            id: id(value),
            expected_state_version: running.state_version,
            expected_attempt: running.attempt,
            retry_not_before_ms: deadline,
            state: TaskState::Queued,
            cancel_requested: false,
            cancel_error: None,
            finished_at_ms: None,
            output: None,
        })
        .await
        .unwrap();
}

fn remove_database(path: &Path) {
    let mut lock = path.as_os_str().to_owned();
    lock.push(".owner.lock");
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(lock);
    let _ = fs::remove_file(path.with_extension("sqlite-wal"));
    let _ = fs::remove_file(path.with_extension("sqlite-shm"));
}

#[tokio::test]
async fn ready_query_filters_deadlines_and_pages_ties_in_numeric_id_order() {
    let path = env::temp_dir().join(format!("qubit-task-ready-query-{}.sqlite", Uuid::new_v4()));
    let store = SqliteTaskStore::open(&path).unwrap();
    queued_with_deadline(&store, 1, None).await;
    queued_with_deadline(&store, 2, Some(100)).await;
    queued_with_deadline(&store, 3, Some(101)).await;
    queued_with_deadline(&store, 4, Some(100)).await;

    let page_limit = NonZeroUsize::new(2).unwrap();
    let first = store
        .list_ready_queued(None, page_limit, 100)
        .await
        .unwrap();
    assert_eq!(
        first
            .records
            .iter()
            .map(|row| row.id.into_id().value())
            .collect::<Vec<_>>(),
        [1, 2]
    );
    assert!(first.next.is_some());
    let second = store
        .list_ready_queued(first.next, page_limit, 100)
        .await
        .unwrap();
    assert_eq!(
        second
            .records
            .iter()
            .map(|row| row.id.into_id().value())
            .collect::<Vec<_>>(),
        [4]
    );
    assert_eq!(second.next, None);

    let empty = store
        .list_ready_queued(None, NonZeroUsize::new(256).unwrap(), 101)
        .await
        .unwrap();
    assert_eq!(empty.records.len(), 4);
    assert_eq!(empty.next, None);
    drop(store);
    remove_database(&path);
}

#[test]
fn reopening_v8_recreates_the_ready_index_without_changing_schema_version() {
    let path = env::temp_dir().join(format!("qubit-task-ready-index-{}.sqlite", Uuid::new_v4()));
    drop(SqliteTaskStore::open(&path).unwrap());
    let connection = Connection::open(&path).unwrap();
    connection
        .execute_batch("DROP INDEX tasks_queued_immediate_accepted_id;")
        .unwrap();
    drop(connection);

    drop(SqliteTaskStore::open(&path).unwrap());
    let connection = Connection::open(&path).unwrap();
    let index_exists: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='index' AND name='tasks_queued_immediate_accepted_id')",
        [],
        |row| row.get(0),
    ).unwrap();
    let version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .unwrap();
    assert!(index_exists);
    assert_eq!(version, 8);
    drop(connection);
    remove_database(&path);
}

#[test]
fn failed_v7_migration_rolls_back_ready_index_and_schema_version() {
    let path = env::temp_dir().join(format!(
        "qubit-task-ready-migration-{}.sqlite",
        Uuid::new_v4()
    ));
    drop(SqliteTaskStore::open(&path).unwrap());
    let connection = Connection::open(&path).unwrap();
    connection
        .execute_batch(
            "DROP INDEX tasks_queued_immediate_accepted_id;
             DROP TABLE task_terminal_checkpoints;
             DROP TABLE task_event_outbox_quarantine;
             PRAGMA user_version=7;",
        )
        .unwrap();
    drop(connection);

    assert!(SqliteTaskStore::open(&path).is_err());
    let connection = Connection::open(&path).unwrap();
    let version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .unwrap();
    let immediate_index: bool = connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='index' AND name='tasks_queued_immediate_accepted_id')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let checkpoint_table: bool = connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='task_terminal_checkpoints')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(version, 7);
    assert!(!immediate_index);
    assert!(!checkpoint_table);
    drop(connection);
    remove_database(&path);
}
