use qubit_task::TaskId;
use qubit_task::model::AcceptOutcome;
use qubit_task::model::MAX_TASK_QUERY_LIMIT;
use qubit_task::model::TaskQuery;
use qubit_task::model::TaskRequest;
use qubit_task::store::MemoryTaskStore;
use qubit_task::store::SqliteTaskStore;
use qubit_task::store::StoreError;
use qubit_task::store::TaskStore;

fn database_path(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("qubit-task-{label}-{}.sqlite", TaskId::generate()))
}

fn remove_database(path: &std::path::Path) {
    for candidate in [
        path.to_path_buf(),
        path.with_extension("owner.lock"),
        path.with_extension("sqlite-wal"),
        path.with_extension("sqlite-shm"),
    ] {
        let _ = std::fs::remove_file(candidate);
    }
}

async fn seed_legacy_database(path: &std::path::Path) -> (TaskId, TaskRequest) {
    let id = TaskId::generate();
    let mut request = TaskRequest::new("legacy", "1", b"payload".to_vec());
    request.idempotency_key = Some("legacy-key".into());
    let memory = MemoryTaskStore::new(10);
    let record = match memory.accept(id, request.clone()).await.expect("memory record seeds") {
        AcceptOutcome::Accepted(record) => record,
        AcceptOutcome::Existing(_) => panic!("new task ID cannot already exist"),
    };
    let connection = rusqlite::Connection::open(path).expect("legacy database opens");
    connection
        .execute_batch(
            "CREATE TABLE tasks (id TEXT PRIMARY KEY NOT NULL, state_kind TEXT NOT NULL, accepted_at INTEGER NOT NULL, correlation_key TEXT, idempotency_key TEXT UNIQUE, request_json TEXT NOT NULL, record_json TEXT NOT NULL); CREATE INDEX tasks_state_accepted ON tasks(state_kind, accepted_at); CREATE INDEX tasks_accepted_id ON tasks(accepted_at, id); CREATE TABLE metadata (key TEXT PRIMARY KEY NOT NULL, value INTEGER NOT NULL);",
        )
        .expect("legacy schema is created");
    connection
        .execute(
            "INSERT INTO tasks (id,state_kind,accepted_at,correlation_key,idempotency_key,request_json,record_json) VALUES (?1,'Queued',?2,NULL,?3,?4,?5)",
            rusqlite::params![
                id.to_string(),
                i64::try_from(record.accepted_at_ms).expect("timestamp fits SQLite"),
                request.idempotency_key,
                serde_json::to_string(&request).expect("request serializes"),
                {
                    let mut value = serde_json::to_value(&record).expect("record serializes");
                    value.as_object_mut().unwrap().remove("retry_not_before_ms");
                    serde_json::to_string(&value).expect("legacy record serializes")
                },
            ],
        )
        .expect("legacy task is inserted");
    (id, request)
}

#[tokio::test]
async fn test_sqlite_open_creates_version_two_schema() {
    let path = database_path("schema-fresh");
    let store = SqliteTaskStore::open(&path).expect("SQLite store opens");
    drop(store);
    let connection = rusqlite::Connection::open(&path).expect("database opens for schema inspection");
    let version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .expect("schema version is readable");
    assert_eq!(version, 2);
    let columns = connection
        .prepare("PRAGMA table_info(tasks)")
        .expect("table columns are readable")
        .query_map([], |row| row.get::<_, String>(1))
        .expect("column query succeeds")
        .map(|name| name.expect("column name reads"))
        .collect::<Vec<_>>();
    assert!(columns.iter().any(|name| name == "record_format_version"));
    assert!(columns.iter().any(|name| name == "lifecycle_json"));
    assert!(!columns.iter().any(|name| name == "record_json"));
    drop(connection);
    remove_database(&path);
}

#[tokio::test]
async fn test_sqlite_open_migrates_legacy_records_without_loss() {
    let path = database_path("schema-migrate");
    let (id, request) = seed_legacy_database(&path).await;
    let store = SqliteTaskStore::open(&path).expect("legacy database migrates");
    let record = store
        .get(id)
        .await
        .expect("legacy record reads")
        .expect("record exists");
    assert_eq!(record.request, request);
    assert_eq!(record.id, id);
    assert_eq!(record.retry_not_before_ms, None);
    assert_eq!(
        store
            .get_by_idempotency_key("legacy-key")
            .await
            .expect("idempotency lookup succeeds")
            .unwrap()
            .id,
        id
    );
    drop(store);
    let reopened = SqliteTaskStore::open(&path).expect("migrated database reopens idempotently");
    assert_eq!(
        reopened
            .get(id)
            .await
            .expect("migrated row remains readable")
            .unwrap()
            .retry_not_before_ms,
        None
    );
    assert_eq!(
        reopened.get_by_idempotency_key("legacy-key").await.unwrap().unwrap().id,
        id
    );
    drop(reopened);
    remove_database(&path);
}

/// Migrates schema 1 while retaining its original task request.
#[tokio::test]
async fn test_sqlite_open_migrates_schema_one_records_without_loss() {
    let path = database_path("schema-one-migrate");
    let (id, request) = seed_legacy_database(&path).await;
    let connection = rusqlite::Connection::open(&path).expect("legacy database opens");
    connection
        .execute_batch(
            "ALTER TABLE tasks ADD COLUMN record_format_version INTEGER NOT NULL DEFAULT 1; PRAGMA user_version=1;",
        )
        .expect("schema one format is seeded");
    drop(connection);

    let store = SqliteTaskStore::open(&path).expect("schema one database migrates");
    let record = store.get(id).await.expect("record reads").expect("record exists");
    assert_eq!(record.request, request);
    assert_eq!(record.id, id);
    assert_eq!(record.state_version, 0);
    drop(store);
    let connection = rusqlite::Connection::open(&path).expect("migrated database opens");
    let version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .unwrap();
    assert_eq!(version, 2);
    drop(connection);
    remove_database(&path);
}

/// Preserves queued retry, running, and diagnostic terminal lifecycle fields.
#[tokio::test]
async fn test_sqlite_schema_migration_preserves_lifecycle_variants() {
    use qubit_task::model::TaskRecord;
    use qubit_task::model::TaskState;

    let path = database_path("schema-lifecycle");
    let (queued_id, _) = seed_legacy_database(&path).await;
    let records = [
        TaskRecord {
            id: TaskId::generate(),
            request: TaskRequest::new("legacy-running", "1", b"running".to_vec())
                .with_idempotency_key("legacy-running-key"),
            state: TaskState::Running,
            state_version: 1,
            attempt: 1,
            retry_not_before_ms: None,
            accepted_at_ms: 101,
            started_at_ms: Some(102),
            finished_at_ms: None,
            assigned_resources: vec!["cpu:0".into()],
            output: None,
            cancel_requested: false,
        },
        TaskRecord {
            id: TaskId::generate(),
            request: TaskRequest::new("legacy-failed", "1", b"failure".to_vec())
                .with_idempotency_key("legacy-failed-key"),
            state: TaskState::Failed {
                category: "legacy-category".into(),
                message: "legacy diagnostic".into(),
            },
            state_version: 2,
            attempt: 1,
            retry_not_before_ms: None,
            accepted_at_ms: 103,
            started_at_ms: Some(104),
            finished_at_ms: Some(105),
            assigned_resources: Vec::new(),
            output: None,
            cancel_requested: false,
        },
        TaskRecord {
            id: TaskId::generate(),
            request: TaskRequest::new("legacy-retry", "1", b"retry".to_vec()).with_idempotency_key("legacy-retry-key"),
            state: TaskState::Queued,
            state_version: 2,
            attempt: 1,
            retry_not_before_ms: Some(1234),
            accepted_at_ms: 106,
            started_at_ms: Some(107),
            finished_at_ms: None,
            assigned_resources: Vec::new(),
            output: None,
            cancel_requested: false,
        },
    ];
    let connection = rusqlite::Connection::open(&path).expect("legacy database opens");
    for record in &records {
        connection
            .execute(
                "INSERT INTO tasks (id,state_kind,accepted_at,correlation_key,idempotency_key,request_json,record_json) VALUES (?1,?2,?3,?4,?5,?6,?7)",
                rusqlite::params![
                    record.id.to_string(),
                    match record.state.kind() {
                        qubit_task::model::TaskStateKind::Queued => "Queued",
                        qubit_task::model::TaskStateKind::Running => "Running",
                        qubit_task::model::TaskStateKind::Blocked => "Blocked",
                        qubit_task::model::TaskStateKind::Succeeded => "Succeeded",
                        qubit_task::model::TaskStateKind::Failed => "Failed",
                        qubit_task::model::TaskStateKind::Panicked => "Panicked",
                        qubit_task::model::TaskStateKind::Cancelled => "Cancelled",
                    },
                    i64::try_from(record.accepted_at_ms).unwrap(),
                    record.request.correlation_key,
                    record.request.idempotency_key,
                    serde_json::to_string(&record.request).unwrap(),
                    serde_json::to_string(record).unwrap(),
                ],
            )
            .expect("legacy lifecycle row is inserted");
    }
    drop(connection);

    let store = SqliteTaskStore::open(&path).expect("legacy lifecycle rows migrate");
    let queued = store.get(queued_id).await.unwrap().unwrap();
    assert!(matches!(queued.state, TaskState::Queued));
    for expected in &records {
        assert_eq!(store.get(expected.id).await.unwrap().as_ref(), Some(expected));
    }
    assert_eq!(
        store
            .get_by_idempotency_key("legacy-failed-key")
            .await
            .unwrap()
            .unwrap()
            .id,
        records[1].id
    );
    drop(store);
    remove_database(&path);
}

/// Rolls back schema and version changes when a legacy record cannot decode.
#[tokio::test]
async fn test_sqlite_schema_migration_rolls_back_when_legacy_record_is_corrupt() {
    let path = database_path("schema-corrupt");
    let (id, _) = seed_legacy_database(&path).await;
    let connection = rusqlite::Connection::open(&path).expect("legacy database opens");
    connection
        .execute("UPDATE tasks SET record_json='not-json' WHERE id=?1", [id.to_string()])
        .expect("corruption is seeded");
    drop(connection);

    assert!(SqliteTaskStore::open(&path).is_err());
    let connection = rusqlite::Connection::open(&path).expect("database remains readable");
    let version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .unwrap();
    let legacy_value: String = connection
        .query_row("SELECT record_json FROM tasks WHERE id=?1", [id.to_string()], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(version, 0);
    assert_eq!(legacy_value, "not-json");
    drop(connection);
    remove_database(&path);
}

/// Leaves immutable request bytes unchanged across lifecycle transitions.
#[tokio::test]
async fn test_sqlite_transitions_do_not_rewrite_the_immutable_request() {
    use qubit_task::model::TaskState;
    use qubit_task::model::TransitionCommand;

    let path = database_path("immutable-request");
    let store = SqliteTaskStore::open(&path).expect("SQLite store opens");
    let payload = vec![b'x'; 1024 * 1024];
    let accepted = match store
        .accept(TaskId::generate(), TaskRequest::new("large", "1", payload))
        .await
        .expect("task is accepted")
    {
        AcceptOutcome::Accepted(record) => record,
        AcceptOutcome::Existing(_) => panic!("task is new"),
    };
    let connection = rusqlite::Connection::open(&path).expect("database opens for inspection");
    let before: String = connection
        .query_row(
            "SELECT request_json FROM tasks WHERE id=?1",
            [accepted.id.to_string()],
            |row| row.get(0),
        )
        .expect("request is read");
    drop(connection);
    let running = store
        .transition(TransitionCommand {
            id: accepted.id,
            expected_version: accepted.state_version,
            expected_attempt: accepted.attempt,
            state: TaskState::Running,
            retry_not_before_ms: None,
            output: None,
            assigned_resources: Vec::new(),
            cancel_requested: false,
        })
        .await
        .expect("task starts");
    store
        .transition(TransitionCommand {
            id: running.id,
            expected_version: running.state_version,
            expected_attempt: running.attempt,
            state: TaskState::Succeeded,
            retry_not_before_ms: None,
            output: None,
            assigned_resources: Vec::new(),
            cancel_requested: false,
        })
        .await
        .expect("task completes");
    let connection = rusqlite::Connection::open(&path).expect("database opens for inspection");
    let (after, lifecycle): (String, String) = connection
        .query_row(
            "SELECT request_json,lifecycle_json FROM tasks WHERE id=?1",
            [accepted.id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("stored fields are read");
    drop(connection);
    assert_eq!(before, after);
    assert!(lifecycle.len() < 1024);
    assert!(!lifecycle.contains("payload"));
    drop(store);
    remove_database(&path);
}

#[tokio::test]
/// Rejects SQLite history pages larger than the shared query limit.
async fn test_sqlite_task_query_limit() {
    let path = database_path("query-limit");
    let store = SqliteTaskStore::open(&path).expect("SQLite store opens");
    assert!(matches!(
        store
            .list(TaskQuery {
                limit: MAX_TASK_QUERY_LIMIT + 1,
                ..TaskQuery::default()
            })
            .await,
        Err(StoreError::InvalidRequest("task history page limit exceeds 256"))
    ));
    let page = store
        .list(TaskQuery {
            limit: MAX_TASK_QUERY_LIMIT,
            ..TaskQuery::default()
        })
        .await
        .expect("maximum page is accepted");
    assert!(page.records.is_empty());
    let zero = store
        .list(TaskQuery {
            limit: 0,
            ..TaskQuery::default()
        })
        .await
        .unwrap();
    assert!(zero.records.is_empty());
    drop(store);
    remove_database(&path);
}

#[tokio::test]
async fn test_sqlite_open_rejects_future_schema_without_changing_records() {
    let path = database_path("schema-future");
    let (id, _) = seed_legacy_database(&path).await;
    let connection = rusqlite::Connection::open(&path).expect("legacy database opens");
    connection
        .pragma_update(None, "user_version", 3)
        .expect("future version is set");
    drop(connection);

    let error = match SqliteTaskStore::open(&path) {
        Ok(store) => {
            drop(store);
            panic!("future schema is rejected")
        }
        Err(error) => error,
    };
    assert!(error.to_string().contains("3"));
    let connection = rusqlite::Connection::open(&path).expect("database remains readable");
    let rows: i64 = connection
        .query_row("SELECT COUNT(*) FROM tasks WHERE id=?1", [id.to_string()], |row| {
            row.get(0)
        })
        .expect("legacy row remains intact");
    assert_eq!(rows, 1);
    let version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .expect("schema version remains readable");
    assert_eq!(version, 3);
    drop(connection);
    remove_database(&path);
}

#[tokio::test]
async fn test_sqlite_reads_reject_unknown_row_format_everywhere() {
    let path = database_path("row-format");
    let store = SqliteTaskStore::open(&path).expect("SQLite store opens");
    let mut request = TaskRequest::new("unknown-format", "1", Vec::new());
    request.idempotency_key = Some("unknown-format-key".into());
    let accepted = store
        .accept(TaskId::generate(), request.clone())
        .await
        .expect("record is accepted");
    let id = match accepted {
        AcceptOutcome::Accepted(record) => record.id,
        AcceptOutcome::Existing(_) => panic!("new request is accepted"),
    };
    drop(store);

    let connection = rusqlite::Connection::open(&path).expect("database opens");
    connection
        .execute("UPDATE tasks SET record_format_version=3 WHERE id=?1", [id.to_string()])
        .expect("unknown format is seeded");
    drop(connection);

    let store = SqliteTaskStore::open(&path).expect("schema version remains supported");
    assert!(store.get(id).await.is_err());
    assert!(store.list(TaskQuery::default()).await.is_err());
    assert!(store.scan_unfinished(None).await.is_err());
    assert!(store.get_by_idempotency_key("unknown-format-key").await.is_err());
    drop(store);
    remove_database(&path);
}

#[tokio::test]
async fn test_sqlite_store_accepts_retry_deadlines_only_while_queued() {
    use qubit_task::model::TaskState;
    use qubit_task::model::TransitionCommand;
    let path = database_path("retry-deadline");
    let store = SqliteTaskStore::open(&path).unwrap();
    let accepted = match store
        .accept(TaskId::generate(), TaskRequest::new("echo", "1", Vec::new()))
        .await
        .unwrap()
    {
        AcceptOutcome::Accepted(record) => record,
        AcceptOutcome::Existing(_) => panic!("new task cannot already exist"),
    };
    let running = store
        .transition(TransitionCommand {
            id: accepted.id,
            expected_version: accepted.state_version,
            expected_attempt: accepted.attempt,
            state: TaskState::Running,
            retry_not_before_ms: None,
            output: None,
            assigned_resources: Vec::new(),
            cancel_requested: false,
        })
        .await
        .unwrap();
    let queued = store
        .transition(TransitionCommand {
            id: running.id,
            expected_version: running.state_version,
            expected_attempt: running.attempt,
            state: TaskState::Queued,
            retry_not_before_ms: Some(123),
            output: None,
            assigned_resources: Vec::new(),
            cancel_requested: false,
        })
        .await
        .unwrap();
    assert_eq!(queued.retry_not_before_ms, Some(123));
    assert!(
        store
            .transition(TransitionCommand {
                id: queued.id,
                expected_version: queued.state_version,
                expected_attempt: queued.attempt,
                state: TaskState::Running,
                retry_not_before_ms: Some(123),
                output: None,
                assigned_resources: Vec::new(),
                cancel_requested: false,
            })
            .await
            .is_err()
    );
    let running = store
        .transition(TransitionCommand {
            id: queued.id,
            expected_version: queued.state_version,
            expected_attempt: queued.attempt,
            state: TaskState::Running,
            retry_not_before_ms: None,
            output: None,
            assigned_resources: Vec::new(),
            cancel_requested: false,
        })
        .await
        .unwrap();
    assert_eq!(running.retry_not_before_ms, None);
    drop(store);
    remove_database(&path);
}
