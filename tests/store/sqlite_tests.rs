// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use qubit_id::Id;
use rusqlite::Connection;
use rusqlite::params;
use serde_json as json;
use tokio::test as tokio_test;

use crate::model::AcceptOutcome;
use crate::model::MAX_TASK_QUERY_LIMIT;
use crate::model::TaskCursor;
use crate::model::TaskId;
use crate::model::TaskQuery;
use crate::model::TaskRecord;
use crate::model::TaskRequest;
use crate::model::TaskRequestInfo;
use crate::model::TaskState;
use crate::model::TaskState as LegacyTaskState;
use crate::model::TaskStateKind;
use crate::model::TransitionCommand;
use crate::model::next::ProgressCommand;
use crate::model::next::ResourceRequest;
use crate::model::next::StartCommand;
use crate::model::next::StoredPayload;
use crate::model::next::StoredTaskRequest;
use crate::model::next::TaskId as NumericTaskId;
use crate::model::next::TransitionCommand as EncodedTransitionCommand;
use crate::store::LegacyTaskStore as TaskStore;
use crate::store::MemoryTaskStore;
use crate::store::SqliteTaskStore;
use crate::store::StoreError;
use crate::store::TaskStore as TypedTaskStore;

fn database_path(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("qubit-task-{label}-{}.sqlite", TaskId::generate()))
}

fn remove_database(path: &std::path::Path) {
    for candidate in [
        path.to_path_buf(),
        crate::sqlite_paths::owner_lock_path(path),
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
    let connection = Connection::open(path).expect("legacy database opens");
    connection
        .execute_batch(
            "CREATE TABLE tasks (id TEXT PRIMARY KEY NOT NULL, state_kind TEXT NOT NULL, accepted_at INTEGER NOT NULL, correlation_key TEXT, idempotency_key TEXT UNIQUE, request_json TEXT NOT NULL, record_json TEXT NOT NULL); CREATE INDEX tasks_state_accepted ON tasks(state_kind, accepted_at); CREATE INDEX tasks_accepted_id ON tasks(accepted_at, id); CREATE TABLE metadata (key TEXT PRIMARY KEY NOT NULL, value INTEGER NOT NULL);",
        )
        .expect("legacy schema is created");
    connection
        .execute(
            "INSERT INTO tasks (id,state_kind,accepted_at,correlation_key,idempotency_key,request_json,record_json) VALUES (?1,'Queued',?2,NULL,?3,?4,?5)",
            params![
                id.to_string(),
                i64::try_from(record.accepted_at_ms).expect("timestamp fits SQLite"),
                request.idempotency_key,
                json::to_string(&request).expect("request serializes"),
                {
                    let mut value = json::to_value(&record).expect("record serializes");
                    value.as_object_mut().unwrap().remove("retry_not_before_ms");
                    json::to_string(&value).expect("legacy record serializes")
                },
            ],
        )
        .expect("legacy task is inserted");
    (id, request)
}

#[tokio_test]
async fn test_sqlite_open_creates_version_three_schema() {
    let path = database_path("schema-fresh");
    let store = SqliteTaskStore::open(&path).expect("SQLite store opens");
    drop(store);
    let connection = Connection::open(&path).expect("database opens for schema inspection");
    let version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .expect("schema version is readable");
    assert_eq!(version, 3);
    let columns = connection
        .prepare("PRAGMA table_info(tasks)")
        .expect("table columns are readable")
        .query_map([], |row| row.get::<_, String>(1))
        .expect("column query succeeds")
        .map(|name| name.expect("column name reads"))
        .collect::<Vec<_>>();
    assert!(columns.iter().any(|name| name == "record_format_version"));
    assert!(columns.iter().any(|name| name == "lifecycle_json"));
    assert!(columns.iter().any(|name| name == "request_info_json"));
    assert!(columns.iter().any(|name| name == "payload"));
    assert!(!columns.iter().any(|name| name == "record_json"));
    drop(connection);
    remove_database(&path);
}

fn typed_task_id(value: u64) -> NumericTaskId {
    NumericTaskId::from_id(Id::new(value))
}

fn typed_request(key: Option<&str>) -> StoredTaskRequest {
    StoredTaskRequest {
        kind_id: "example.worker".into(),
        category: Some("reports".into()),
        payload: StoredPayload {
            type_id: "example.ReportRequest".try_into().expect("valid payload model ID"),
            schema_version: 3,
            codec_id: "json-v1".into(),
            bytes: b"{\"report\":1}".to_vec(),
        },
        metadata: qubit_metadata::Metadata::new(),
        resource_limit: ResourceRequest::default(),
        correlation_key: Some("corr-typed".into()),
        idempotency_key: key.map(str::to_owned),
    }
}

#[tokio_test]
async fn test_sqlite_typed_store_reports_missing_and_invalid_lifecycle_operations() {
    let path = database_path("typed-boundary-errors");
    let store = SqliteTaskStore::open_next(&path).expect("typed SQLite store opens");
    let missing_id = typed_task_id(201);

    assert!(
        TypedTaskStore::get_encoded_task(&store, missing_id)
            .await
            .expect("missing lookup succeeds")
            .is_none()
    );
    assert!(matches!(
        TypedTaskStore::start_encoded(
            &store,
            StartCommand {
                id: missing_id,
                expected_state_version: 0,
                started_at_ms: 1,
            }
        )
        .await,
        Err(StoreError::NotFound)
    ));
    assert!(matches!(
        TypedTaskStore::update_progress(&store, ProgressCommand::new(missing_id, 1, 1, None, Vec::new(), 2)).await,
        Err(StoreError::NotFound)
    ));

    let id = typed_task_id(202);
    let accepted = TypedTaskStore::accept_encoded(&store, id, typed_request(None))
        .await
        .expect("typed task is accepted");
    assert!(matches!(
        TypedTaskStore::update_progress(&store, ProgressCommand::new(id, 0, 1, None, Vec::new(), 3)).await,
        Err(StoreError::Conflict)
    ));
    assert!(matches!(
        TypedTaskStore::transition_encoded(
            &store,
            EncodedTransitionCommand {
                id,
                expected_state_version: accepted.summary.state_version,
                expected_attempt: accepted.summary.attempt,
                state: LegacyTaskState::Succeeded,
                cancel_requested: false,
                cancel_error: None,
                finished_at_ms: Some(4),
                output: None,
            }
        )
        .await,
        Err(StoreError::Conflict)
    ));
    assert!(matches!(
        TypedTaskStore::transition_encoded(
            &store,
            EncodedTransitionCommand {
                id,
                expected_state_version: accepted.summary.state_version,
                expected_attempt: accepted.summary.attempt,
                state: LegacyTaskState::Failed {
                    category: "x".repeat(crate::model::MAX_TASK_DIAGNOSTIC_CATEGORY_BYTES + 1),
                    message: "oversized failure category".into(),
                },
                cancel_requested: false,
                cancel_error: None,
                finished_at_ms: Some(5),
                output: None,
            }
        )
        .await,
        Err(StoreError::InvalidRequest(_))
    ));
    drop(store);
    remove_database(&path);
}

#[tokio_test]
async fn test_sqlite_typed_schema_accepts_fixed_width_max_id_and_reopens_progress() {
    let path = database_path("typed-schema");
    let id = typed_task_id(u64::MAX);
    let store = SqliteTaskStore::open_next(&path).expect("typed SQLite store opens");
    assert!(matches!(
        store
            .accept(TaskId::generate(), TaskRequest::new("legacy", "1", Vec::new()))
            .await,
        Err(StoreError::UnsupportedCapability)
    ));
    assert!(matches!(
        store.get(TaskId::generate()).await,
        Err(StoreError::UnsupportedCapability)
    ));
    assert!(matches!(
        store.list(TaskQuery::default()).await,
        Err(StoreError::UnsupportedCapability)
    ));
    assert!(matches!(
        store.scan_unfinished(None).await,
        Err(StoreError::UnsupportedCapability)
    ));
    let low_id = typed_task_id(1);
    TypedTaskStore::accept_encoded(&store, low_id, typed_request(None))
        .await
        .expect("low typed task is accepted");
    let accepted = TypedTaskStore::accept_encoded(&store, id, typed_request(Some("typed-idempotency")))
        .await
        .expect("typed request is accepted");
    assert!(accepted.created);
    assert_eq!(accepted.summary.id, id);
    assert_eq!(accepted.summary.category.as_deref(), Some("reports"));
    let existing = TypedTaskStore::accept_encoded(&store, id, typed_request(Some("typed-idempotency")))
        .await
        .expect("identical idempotent request is reused");
    assert!(!existing.created);
    assert_eq!(existing.summary.id, id);
    let mut conflicting_request = typed_request(Some("typed-idempotency"));
    conflicting_request.payload.bytes = b"{\"report\":2}".to_vec();
    assert!(matches!(
        TypedTaskStore::accept_encoded(&store, typed_task_id(u64::MAX - 1), conflicting_request).await,
        Err(StoreError::IdempotencyConflict)
    ));
    let loaded = TypedTaskStore::get_encoded_task(&store, id)
        .await
        .expect("typed task lookup succeeds")
        .expect("typed task exists");
    assert_eq!(loaded.request.payload.bytes, b"{\"report\":1}");
    assert_eq!(loaded.request.payload.schema_version, 3);
    let started = TypedTaskStore::start_encoded(
        &store,
        StartCommand {
            id,
            expected_state_version: 0,
            started_at_ms: 10,
        },
    )
    .await
    .expect("queued typed task starts");
    assert_eq!(started.state, LegacyTaskState::Running);
    assert_eq!(started.attempt, 1);
    assert_eq!(started.state_version, 1);
    assert!(matches!(
        TypedTaskStore::start_encoded(
            &store,
            StartCommand {
                id,
                expected_state_version: 0,
                started_at_ms: 12,
            }
        )
        .await,
        Err(StoreError::Conflict)
    ));
    let connection = Connection::open(&path).expect("database opens for key inspection");
    let stored_id: String = connection
        .query_row("SELECT id FROM tasks ORDER BY id LIMIT 1", [], |row| row.get(0))
        .expect("fixed width ID reads");
    assert_eq!(stored_id, "00000000000000000001");
    drop(connection);
    drop(store);

    let store = SqliteTaskStore::open_next(&path).expect("typed database reopens");
    let progress = TypedTaskStore::update_progress(&store, ProgressCommand::new(id, 1, 1, None, Vec::new(), 11))
        .await
        .expect("current attempt progress commits");
    assert_eq!(progress.progress.as_ref().unwrap().progress_version, 1);
    assert!(matches!(
        TypedTaskStore::update_progress(&store, ProgressCommand::new(id, 0, 2, None, Vec::new(), 12)).await,
        Err(StoreError::Conflict)
    ));
    assert!(matches!(
        TypedTaskStore::update_progress(&store, ProgressCommand::new(id, 1, 1, None, Vec::new(), 13)).await,
        Err(StoreError::Conflict)
    ));
    let connection = Connection::open(&path).expect("database opens for terminal setup");
    let lifecycle_json: String = connection
        .query_row(
            "SELECT lifecycle_json FROM tasks WHERE id=?1",
            [id.to_padded_decimal()],
            |row| row.get(0),
        )
        .expect("lifecycle is stored");
    let mut lifecycle: serde_json::Value = serde_json::from_str(&lifecycle_json).expect("lifecycle JSON decodes");
    lifecycle["state"] = serde_json::to_value(LegacyTaskState::Succeeded).expect("success state serializes");
    connection
        .execute(
            "UPDATE tasks SET state_kind='Succeeded',lifecycle_json=?2 WHERE id=?1",
            params![
                id.to_padded_decimal(),
                serde_json::to_string(&lifecycle).expect("lifecycle serializes")
            ],
        )
        .expect("task is marked terminal");
    drop(connection);
    assert!(matches!(
        TypedTaskStore::update_progress(&store, ProgressCommand::new(id, 1, 2, None, Vec::new(), 14)).await,
        Err(StoreError::Conflict)
    ));
    drop(store);

    let reopened = SqliteTaskStore::open_next(&path).expect("typed database remains readable");
    let recovered = TypedTaskStore::get_encoded_task(&reopened, id)
        .await
        .expect("typed task reopens")
        .expect("typed row remains present");
    assert_eq!(recovered.summary.progress.unwrap().progress_version, 1);
    drop(reopened);
    let connection = Connection::open(&path).expect("database opens for schema inspection");
    let schema_version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .expect("schema version reads");
    assert_eq!(schema_version, 4);
    let category_index: bool = connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='index' AND name='tasks_category_accepted_id')",
            [],
            |row| row.get(0),
        )
        .expect("category index lookup succeeds");
    assert!(category_index);
    let query_plan: String = connection
        .query_row(
            "EXPLAIN QUERY PLAN SELECT id FROM tasks WHERE category='reports' ORDER BY accepted_at,id LIMIT 10",
            [],
            |row| row.get(3),
        )
        .expect("category query plan is available");
    assert!(query_plan.contains("tasks_category_accepted_id"), "{query_plan}");
    drop(connection);
    remove_database(&path);
}

#[tokio_test]
async fn test_sqlite_new_attempt_clears_progress_and_restarts_progress_version() {
    let path = database_path("typed-progress-retry");
    let id = typed_task_id(120);
    let store = SqliteTaskStore::open_next(&path).expect("typed SQLite store opens");
    let accepted = TypedTaskStore::accept_encoded(&store, id, typed_request(None))
        .await
        .expect("typed task is accepted");
    let first_attempt = TypedTaskStore::start_encoded(
        &store,
        StartCommand {
            id,
            expected_state_version: accepted.summary.state_version,
            started_at_ms: 1,
        },
    )
    .await
    .expect("first attempt starts");
    TypedTaskStore::update_progress(
        &store,
        ProgressCommand::new(id, first_attempt.attempt, 1, None, Vec::new(), 2),
    )
    .await
    .expect("first attempt progress persists");
    let queued = TypedTaskStore::transition_encoded(
        &store,
        EncodedTransitionCommand {
            id,
            expected_state_version: first_attempt.state_version,
            expected_attempt: first_attempt.attempt,
            state: LegacyTaskState::Queued,
            cancel_requested: false,
            cancel_error: None,
            finished_at_ms: None,
            output: None,
        },
    )
    .await
    .expect("retryable task returns to queue");
    assert!(
        queued.progress.is_some(),
        "previous attempt remains visible until restart"
    );

    let second_attempt = TypedTaskStore::start_encoded(
        &store,
        StartCommand {
            id,
            expected_state_version: queued.state_version,
            started_at_ms: 3,
        },
    )
    .await
    .expect("second attempt starts");
    assert_eq!(second_attempt.attempt, 2);
    assert_eq!(second_attempt.progress, None);
    let next_progress = TypedTaskStore::update_progress(
        &store,
        ProgressCommand::new(id, second_attempt.attempt, 1, None, Vec::new(), 4),
    )
    .await
    .expect("progress version restarts for the new attempt");
    assert_eq!(
        next_progress
            .progress
            .as_ref()
            .map(|progress| progress.progress_version),
        Some(1)
    );
    let terminal = TypedTaskStore::transition_encoded(
        &store,
        EncodedTransitionCommand {
            id,
            expected_state_version: second_attempt.state_version,
            expected_attempt: second_attempt.attempt,
            state: LegacyTaskState::Succeeded,
            cancel_requested: false,
            cancel_error: None,
            finished_at_ms: Some(5),
            output: None,
        },
    )
    .await
    .expect("terminal state persists in lifecycle JSON");
    assert_eq!(terminal.state, LegacyTaskState::Succeeded);
    assert_eq!(terminal.finished_at_ms, Some(5));
    assert_eq!(terminal.progress.as_ref().map(|progress| progress.attempt), Some(2));

    drop(store);
    remove_database(&path);
}

#[tokio_test]
async fn test_sqlite_open_next_rejects_legacy_schema_without_rewriting_it() {
    let path = database_path("typed-legacy-reject");
    let (id, _) = seed_legacy_database(&path).await;
    let before = Connection::open(&path)
        .expect("legacy database opens")
        .query_row("SELECT record_json FROM tasks WHERE id=?1", [id.to_string()], |row| {
            row.get::<_, String>(0)
        })
        .expect("legacy record is readable before open");
    let error = match SqliteTaskStore::open_next(&path) {
        Ok(_) => panic!("typed API must reject a legacy UUID database"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("explicit task ID mapping"));
    let connection = Connection::open(&path).expect("legacy database remains readable");
    let after: String = connection
        .query_row("SELECT record_json FROM tasks WHERE id=?1", [id.to_string()], |row| {
            row.get(0)
        })
        .expect("legacy record remains present");
    assert_eq!(after, before);
    let version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .expect("legacy schema version remains readable");
    assert_eq!(version, 0);
    drop(connection);
    remove_database(&path);
}

#[tokio_test]
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
#[tokio_test]
async fn test_sqlite_open_migrates_schema_one_records_without_loss() {
    let path = database_path("schema-one-migrate");
    let (id, request) = seed_legacy_database(&path).await;
    let connection = Connection::open(&path).expect("legacy database opens");
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
    let connection = Connection::open(&path).expect("migrated database opens");
    let version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .unwrap();
    assert_eq!(version, 3);
    drop(connection);
    remove_database(&path);
}

/// Preserves queued retry, running, and diagnostic terminal lifecycle fields.
#[tokio_test]
async fn test_sqlite_schema_migration_preserves_lifecycle_variants() {
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
    let connection = Connection::open(&path).expect("legacy database opens");
    for record in &records {
        connection
            .execute(
                "INSERT INTO tasks (id,state_kind,accepted_at,correlation_key,idempotency_key,request_json,record_json) VALUES (?1,?2,?3,?4,?5,?6,?7)",
                params![
                    record.id.to_string(),
                    match record.state.kind() {
                        TaskStateKind::Queued => "Queued",
                        TaskStateKind::Running => "Running",
                        TaskStateKind::Blocked => "Blocked",
                        TaskStateKind::Succeeded => "Succeeded",
                        TaskStateKind::Failed => "Failed",
                        TaskStateKind::Panicked => "Panicked",
                        TaskStateKind::Cancelled => "Cancelled",
                    },
                    i64::try_from(record.accepted_at_ms).unwrap(),
                    record.request.correlation_key,
                    record.request.idempotency_key,
                    json::to_string(&record.request).unwrap(),
                    json::to_string(record).unwrap(),
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
#[tokio_test]
async fn test_sqlite_schema_migration_rolls_back_when_legacy_record_is_corrupt() {
    let path = database_path("schema-corrupt");
    let (id, _) = seed_legacy_database(&path).await;
    let connection = Connection::open(&path).expect("legacy database opens");
    connection
        .execute("UPDATE tasks SET record_json='not-json' WHERE id=?1", [id.to_string()])
        .expect("corruption is seeded");
    drop(connection);

    assert!(SqliteTaskStore::open(&path).is_err());
    let connection = Connection::open(&path).expect("database remains readable");
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
#[tokio_test]
async fn test_sqlite_transitions_do_not_rewrite_the_immutable_request() {
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
    let connection = Connection::open(&path).expect("database opens for inspection");
    let before: Vec<u8> = connection
        .query_row(
            "SELECT payload FROM tasks WHERE id=?1",
            [accepted.id.to_string()],
            |row| row.get(0),
        )
        .expect("request is read");
    drop(connection);
    let summary = store.get_summary(accepted.id).await.unwrap().unwrap();
    assert_eq!(summary.request.task_type, "large");
    assert_eq!(
        store
            .list(TaskQuery {
                limit: 1,
                ..TaskQuery::default()
            })
            .await
            .unwrap()
            .records[0],
        summary
    );
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
    let summary_sql = [
        "SELECT id,state_kind,accepted_at,correlation_key,idempotency_key,record_format_version,request_info_json,lifecycle_json FROM tasks WHERE id=?1",
        "SELECT id,state_kind,accepted_at,correlation_key,idempotency_key,record_format_version,request_info_json,lifecycle_json FROM tasks ORDER BY accepted_at,id",
    ];
    assert!(summary_sql.iter().all(|sql| !sql.contains("payload")));
    let connection = Connection::open(&path).expect("database opens for inspection");
    let (after, lifecycle): (Vec<u8>, String) = connection
        .query_row(
            "SELECT payload,lifecycle_json FROM tasks WHERE id=?1",
            [accepted.id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("stored fields are read");
    drop(connection);
    assert_eq!(before, after);
    assert_eq!(after, vec![b'x'; 1024 * 1024]);
    assert!(lifecycle.len() < 1024);
    assert!(!lifecycle.contains("payload"));
    drop(store);
    remove_database(&path);
}

#[tokio_test]
async fn test_sqlite_migrates_schema_two_without_changing_payload_or_lifecycle() {
    let path = database_path("schema-two-migrate");
    let store = SqliteTaskStore::open(&path).unwrap();
    let request = TaskRequest::new("schema-two", "v1", vec![7; 1024 * 1024]).with_idempotency_key("schema-two-key");
    let accepted = match store.accept(TaskId::generate(), request.clone()).await.unwrap() {
        AcceptOutcome::Accepted(record) => record,
        AcceptOutcome::Existing(_) => unreachable!(),
    };
    let running = store
        .transition(TransitionCommand {
            id: accepted.id,
            expected_version: accepted.state_version,
            expected_attempt: accepted.attempt,
            state: TaskState::Running,
            retry_not_before_ms: None,
            output: None,
            assigned_resources: vec!["cpu-0".into()],
            cancel_requested: false,
        })
        .await
        .unwrap();
    let blocked = store
        .transition(TransitionCommand {
            id: running.id,
            expected_version: running.state_version,
            expected_attempt: running.attempt,
            state: TaskState::Blocked {
                reason: "operator".into(),
            },
            retry_not_before_ms: None,
            output: None,
            assigned_resources: Vec::new(),
            cancel_requested: false,
        })
        .await
        .unwrap();
    drop(store);

    let connection = Connection::open(&path).unwrap();
    let info_json: String = connection
        .query_row(
            "SELECT request_info_json FROM tasks WHERE id=?1",
            [accepted.id.to_string()],
            |row| row.get(0),
        )
        .unwrap();
    let payload: Vec<u8> = connection
        .query_row(
            "SELECT payload FROM tasks WHERE id=?1",
            [accepted.id.to_string()],
            |row| row.get(0),
        )
        .unwrap();
    let lifecycle: String = connection
        .query_row(
            "SELECT lifecycle_json FROM tasks WHERE id=?1",
            [accepted.id.to_string()],
            |row| row.get(0),
        )
        .unwrap();
    let info: TaskRequestInfo = json::from_str(&info_json).unwrap();
    let full = TaskRequest {
        payload,
        ..TaskRequest::new(info.task_type, info.handler_version, vec![])
    };
    let full = TaskRequest {
        resources: info.resources,
        correlation_key: info.correlation_key,
        idempotency_key: info.idempotency_key,
        metadata: info.metadata,
        ..full
    };
    let request_json = json::to_string(&full).unwrap();
    connection.execute_batch("DROP TABLE tasks; CREATE TABLE tasks (id TEXT PRIMARY KEY NOT NULL, state_kind TEXT NOT NULL, accepted_at INTEGER NOT NULL, correlation_key TEXT, idempotency_key TEXT UNIQUE, request_json TEXT NOT NULL, record_format_version INTEGER NOT NULL DEFAULT 2, lifecycle_json TEXT NOT NULL); CREATE INDEX tasks_state_accepted ON tasks(state_kind,accepted_at); CREATE INDEX tasks_accepted_id ON tasks(accepted_at,id); PRAGMA user_version=2;").unwrap();
    connection.execute("INSERT INTO tasks (id,state_kind,accepted_at,correlation_key,idempotency_key,request_json,record_format_version,lifecycle_json) VALUES (?1,'Blocked',?2,NULL,?3,?4,2,?5)", params![accepted.id.to_string(), i64::try_from(blocked.accepted_at_ms).unwrap(), "schema-two-key", request_json, lifecycle]).unwrap();
    drop(connection);

    let migrated = SqliteTaskStore::open(&path).unwrap();
    let restored = migrated.get(accepted.id).await.unwrap().unwrap();
    assert_eq!(restored.request, request);
    assert_eq!(restored.state, blocked.state);
    assert_eq!(restored.state_version, blocked.state_version);
    assert_eq!(
        migrated
            .get_by_idempotency_key("schema-two-key")
            .await
            .unwrap()
            .unwrap()
            .id,
        accepted.id
    );
    drop(migrated);
    let connection = Connection::open(&path).unwrap();
    let version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .unwrap();
    assert_eq!(version, 3);
    let bytes: Vec<u8> = connection
        .query_row(
            "SELECT payload FROM tasks WHERE id=?1",
            [accepted.id.to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(bytes, vec![7; 1024 * 1024]);
    drop(connection);
    remove_database(&path);
}

#[tokio_test]
async fn test_sqlite_schema_two_corruption_rolls_back_migration() {
    let path = database_path("schema-two-corrupt");
    let id = TaskId::generate();
    let connection = Connection::open(&path).unwrap();
    connection.execute_batch("CREATE TABLE tasks (id TEXT PRIMARY KEY NOT NULL, state_kind TEXT NOT NULL, accepted_at INTEGER NOT NULL, correlation_key TEXT, idempotency_key TEXT UNIQUE, request_json TEXT NOT NULL, record_format_version INTEGER NOT NULL DEFAULT 2, lifecycle_json TEXT NOT NULL); PRAGMA user_version=2;").unwrap();
    connection.execute("INSERT INTO tasks (id,state_kind,accepted_at,request_json,record_format_version,lifecycle_json) VALUES (?1,'Queued',0,'broken',2,'{}')", [id.to_string()]).unwrap();
    drop(connection);

    assert!(SqliteTaskStore::open(&path).is_err());
    let connection = Connection::open(&path).unwrap();
    let version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .unwrap();
    let request: String = connection
        .query_row("SELECT request_json FROM tasks WHERE id=?1", [id.to_string()], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(version, 2);
    assert_eq!(request, "broken");
    let table_exists: bool = connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='tasks_v3')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(!table_exists);
    drop(connection);
    remove_database(&path);
}

#[tokio_test]
async fn test_sqlite_schema_two_migration_preserves_each_lifecycle_category() {
    let path = database_path("schema-two-states");
    let store = SqliteTaskStore::open(&path).unwrap();
    let mut cases = Vec::new();
    let queued_request = TaskRequest::new("schema-two-queued", "v1", vec![1]).with_idempotency_key("schema-two-queued");
    let queued = match store.accept(TaskId::generate(), queued_request.clone()).await.unwrap() {
        AcceptOutcome::Accepted(record) => record.summary(),
        _ => unreachable!(),
    };
    cases.push((queued_request, queued, "Queued"));

    for (name, target, key) in [
        ("running", TaskState::Running, "schema-two-running"),
        (
            "blocked",
            TaskState::Blocked {
                reason: "operator".into(),
            },
            "schema-two-blocked",
        ),
        ("succeeded", TaskState::Succeeded, "schema-two-succeeded"),
    ] {
        let request = TaskRequest::new(format!("schema-two-{name}"), "v1", vec![2, 3]).with_idempotency_key(key);
        let accepted = match store.accept(TaskId::generate(), request.clone()).await.unwrap() {
            AcceptOutcome::Accepted(record) => record.summary(),
            _ => unreachable!(),
        };
        let running = store
            .transition(TransitionCommand {
                id: accepted.id,
                expected_version: accepted.state_version,
                expected_attempt: accepted.attempt,
                state: TaskState::Running,
                retry_not_before_ms: None,
                output: None,
                assigned_resources: vec!["cpu-0".into()],
                cancel_requested: false,
            })
            .await
            .unwrap();
        let final_summary = if matches!(&target, TaskState::Running) {
            running
        } else {
            store
                .transition(TransitionCommand {
                    id: running.id,
                    expected_version: running.state_version,
                    expected_attempt: running.attempt,
                    state: target.clone(),
                    retry_not_before_ms: None,
                    output: None,
                    assigned_resources: Vec::new(),
                    cancel_requested: false,
                })
                .await
                .unwrap()
        };
        let state = match &target {
            TaskState::Running => "Running",
            TaskState::Blocked { .. } => "Blocked",
            TaskState::Succeeded => "Succeeded",
            _ => unreachable!(),
        };
        cases.push((request, final_summary, state));
    }
    drop(store);

    let connection = Connection::open(&path).unwrap();
    let mut rows = Vec::new();
    for (request, summary, state) in &cases {
        let lifecycle: String = connection
            .query_row(
                "SELECT lifecycle_json FROM tasks WHERE id=?1",
                [summary.id.to_string()],
                |row| row.get(0),
            )
            .unwrap();
        rows.push((request.clone(), summary.clone(), *state, lifecycle));
    }
    connection.execute_batch("DROP TABLE tasks; CREATE TABLE tasks (id TEXT PRIMARY KEY NOT NULL, state_kind TEXT NOT NULL, accepted_at INTEGER NOT NULL, correlation_key TEXT, idempotency_key TEXT UNIQUE, request_json TEXT NOT NULL, record_format_version INTEGER NOT NULL DEFAULT 2, lifecycle_json TEXT NOT NULL); PRAGMA user_version=2;").unwrap();
    for (request, summary, state, lifecycle) in &rows {
        connection.execute("INSERT INTO tasks (id,state_kind,accepted_at,correlation_key,idempotency_key,request_json,record_format_version,lifecycle_json) VALUES (?1,?2,?3,?4,?5,?6,2,?7)", params![summary.id.to_string(), state, i64::try_from(summary.accepted_at_ms).unwrap(), request.correlation_key, request.idempotency_key, json::to_string(request).unwrap(), lifecycle]).unwrap();
    }
    drop(connection);

    let migrated = SqliteTaskStore::open(&path).unwrap();
    for (request, expected, _, _) in rows {
        let restored = migrated.get(expected.id).await.unwrap().unwrap();
        assert_eq!(restored.request, request);
        assert_eq!(restored.state, expected.state);
        assert_eq!(restored.state_version, expected.state_version);
        assert_eq!(restored.attempt, expected.attempt);
        assert_eq!(restored.assigned_resources, expected.assigned_resources);
    }
    drop(migrated);
    remove_database(&path);
}

/// Rejects SQLite history pages larger than the shared query limit.
#[tokio_test]
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

#[tokio_test]
async fn test_sqlite_open_rejects_future_schema_without_changing_records() {
    let path = database_path("schema-future");
    let (id, _) = seed_legacy_database(&path).await;
    let connection = Connection::open(&path).expect("legacy database opens");
    connection
        .pragma_update(None, "user_version", 4)
        .expect("future version is set");
    drop(connection);

    let error = match SqliteTaskStore::open(&path) {
        Ok(store) => {
            drop(store);
            panic!("future schema is rejected")
        }
        Err(error) => error,
    };
    assert!(error.to_string().contains("4"));
    let connection = Connection::open(&path).expect("database remains readable");
    let rows: i64 = connection
        .query_row("SELECT COUNT(*) FROM tasks WHERE id=?1", [id.to_string()], |row| {
            row.get(0)
        })
        .expect("legacy row remains intact");
    assert_eq!(rows, 1);
    let version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .expect("schema version remains readable");
    assert_eq!(version, 4);
    drop(connection);
    remove_database(&path);
}

#[tokio_test]
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

    let connection = Connection::open(&path).expect("database opens");
    connection
        .execute("UPDATE tasks SET record_format_version=4 WHERE id=?1", [id.to_string()])
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

#[tokio_test]
async fn test_sqlite_store_accepts_retry_deadlines_only_while_queued() {
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

/// Recovery rejects cursors that cannot be represented by SQLite timestamps.
#[tokio_test]
async fn test_sqlite_recovery_rejects_cursor_timestamp_outside_integer_range() {
    let path = database_path("recovery-cursor-range");
    let store = SqliteTaskStore::open(&path).expect("SQLite store opens");
    assert!(matches!(
        store
            .scan_unfinished(Some(TaskCursor::new(u64::MAX, TaskId::generate())))
            .await,
        Err(StoreError::InvalidRequest(
            "recovery cursor timestamp exceeds the SQLite integer range"
        ))
    ));
    drop(store);
    remove_database(&path);
}
