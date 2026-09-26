use qubit_task::TaskId;
use qubit_task::model::AcceptOutcome;
use qubit_task::model::TaskQuery;
use qubit_task::model::TaskRequest;
use qubit_task::store::MemoryTaskStore;
use qubit_task::store::SqliteTaskStore;
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
async fn test_sqlite_open_creates_version_one_schema() {
    let path = database_path("schema-fresh");
    let store = SqliteTaskStore::open(&path).expect("SQLite store opens");
    drop(store);
    let connection = rusqlite::Connection::open(&path).expect("database opens for schema inspection");
    let version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .expect("schema version is readable");
    assert_eq!(version, 1);
    let has_format_column: bool = connection
        .prepare("PRAGMA table_info(tasks)")
        .expect("table columns are readable")
        .query_map([], |row| row.get::<_, String>(1))
        .expect("column query succeeds")
        .map(|name| name.expect("column name reads"))
        .any(|name| name == "record_format_version");
    assert!(has_format_column);
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
            .find_idempotent(request.clone())
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
    assert_eq!(reopened.find_idempotent(request).await.unwrap().unwrap().id, id);
    drop(reopened);
    remove_database(&path);
}

#[tokio::test]
async fn test_sqlite_open_rejects_future_schema_without_changing_records() {
    let path = database_path("schema-future");
    let (id, _) = seed_legacy_database(&path).await;
    let connection = rusqlite::Connection::open(&path).expect("legacy database opens");
    connection
        .pragma_update(None, "user_version", 2)
        .expect("future version is set");
    drop(connection);

    let error = match SqliteTaskStore::open(&path) {
        Ok(store) => {
            drop(store);
            panic!("future schema is rejected")
        }
        Err(error) => error,
    };
    assert!(error.to_string().contains("2"));
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
    assert_eq!(version, 2);
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
        .execute("UPDATE tasks SET record_format_version=2 WHERE id=?1", [id.to_string()])
        .expect("unknown format is seeded");
    drop(connection);

    let store = SqliteTaskStore::open(&path).expect("schema version remains supported");
    assert!(store.get(id).await.is_err());
    assert!(store.list(TaskQuery::default()).await.is_err());
    assert!(store.scan_unfinished(None).await.is_err());
    assert!(store.find_idempotent(request).await.is_err());
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
