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
use tokio::test as tokio_test;

use crate::model::TaskState;
use crate::model::typed::ProgressCommand;
use crate::model::typed::ResourceRequest;
use crate::model::typed::StartCommand;
use crate::model::typed::StoredPayload;
use crate::model::typed::StoredTaskRequest;
use crate::model::typed::TaskId as NumericTaskId;
use crate::model::typed::TransitionCommand as EncodedTransitionCommand;
use crate::store::SqliteTaskStore;
use crate::store::StoreError;
use crate::store::TaskStore as TypedTaskStore;

fn database_path(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("qubit-task-{label}-{}-{}.sqlite", std::process::id(), std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).expect("clock is after epoch").as_nanos()))
}

fn remove_database(path: &std::path::Path) {
    for candidate in [
        path.to_path_buf(),
        owner_lock_path(path),
        path.with_extension("sqlite-wal"),
        path.with_extension("sqlite-shm"),
    ] {
        let _ = std::fs::remove_file(candidate);
    }
}

fn owner_lock_path(path: &std::path::Path) -> std::path::PathBuf {
    let mut lock_path = path.as_os_str().to_owned();
    lock_path.push(".owner.lock");
    lock_path.into()
}

fn seed_legacy_database(path: &std::path::Path) -> String {
    let id = "550e8400-e29b-41d4-a716-446655440000".to_owned();
    let connection = Connection::open(path).expect("legacy database opens");
    connection
        .execute_batch(
            "CREATE TABLE tasks (id TEXT PRIMARY KEY NOT NULL, state_kind TEXT NOT NULL, accepted_at INTEGER NOT NULL, correlation_key TEXT, idempotency_key TEXT UNIQUE, request_json TEXT NOT NULL, record_json TEXT NOT NULL); CREATE INDEX tasks_state_accepted ON tasks(state_kind, accepted_at); CREATE INDEX tasks_accepted_id ON tasks(accepted_at, id); CREATE TABLE metadata (key TEXT PRIMARY KEY NOT NULL, value INTEGER NOT NULL); PRAGMA user_version=3;",
        )
        .expect("legacy schema is created");
    connection
        .execute(
            "INSERT INTO tasks (id,state_kind,accepted_at,request_json,record_json) VALUES (?1,'Queued',1,'legacy-request','legacy-record')",
            [&id],
        )
        .expect("legacy task is inserted");
    id
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
    let store = SqliteTaskStore::open(&path).expect("typed SQLite store opens");
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
                retry_not_before_ms: None,
                state: TaskState::Succeeded,
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
                retry_not_before_ms: None,
                state: TaskState::Failed {
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
async fn test_sqlite_typed_prune_rejects_limit_outside_sqlite_integer_range() {
    let path = database_path("typed-prune-limit-range");
    let store = SqliteTaskStore::open(&path).expect("typed SQLite store opens");
    let limit = std::num::NonZeroUsize::new(usize::MAX).expect("usize::MAX is nonzero");

    if i64::try_from(limit.get()).is_err() {
        assert!(matches!(
            TypedTaskStore::prune_terminal_before(&store, 0, limit).await,
            Err(StoreError::InvalidRequest(_))
        ));
    }

    drop(store);
    remove_database(&path);
}

#[tokio_test]
async fn test_sqlite_event_outbox_requires_active_owner() {
    let path = database_path("typed-outbox-owner");
    let store = SqliteTaskStore::open(&path).expect("typed SQLite store opens");
    let id = typed_task_id(203);

    assert!(matches!(
        TypedTaskStore::enable_event_outbox(&store).await,
        Err(StoreError::OwnerConflict)
    ));
    assert!(matches!(
        TypedTaskStore::list_event_outbox(&store, 1).await,
        Err(StoreError::OwnerConflict)
    ));
    assert!(matches!(
        TypedTaskStore::mark_event_published(&store, id, 1).await,
        Err(StoreError::OwnerConflict)
    ));

    let epoch = TypedTaskStore::acquire_owner(&store)
        .await
        .expect("typed service owner is acquired");
    assert!(matches!(
        TypedTaskStore::acquire_owner(&store).await,
        Err(StoreError::OwnerConflict)
    ));
    TypedTaskStore::enable_event_outbox(&store)
        .await
        .expect("outbox is enabled under active ownership");
    TypedTaskStore::accept_encoded(&store, id, typed_request(None))
        .await
        .expect("accepted task snapshot is committed");
    assert_eq!(
        TypedTaskStore::list_event_outbox(&store, 1)
            .await
            .expect("outbox is readable under active ownership")
            .len(),
        1
    );

    TypedTaskStore::release_owner(&store, epoch)
        .await
        .expect("owner release completes");
    assert!(matches!(
        TypedTaskStore::list_event_outbox(&store, 1).await,
        Err(StoreError::OwnerConflict)
    ));
    drop(store);
    remove_database(&path);
}

#[tokio_test]
async fn test_sqlite_event_outbox_validates_pages_and_marks_confirmed_events() {
    let path = database_path("typed-outbox-pages");
    let store = SqliteTaskStore::open(&path).expect("typed SQLite store opens");
    let epoch = TypedTaskStore::acquire_owner(&store)
        .await
        .expect("typed service owner is acquired");

    assert!(matches!(
        TypedTaskStore::list_event_outbox(&store, 0).await,
        Err(StoreError::InvalidRequest(_))
    ));
    assert!(matches!(
        TypedTaskStore::list_event_outbox(&store, 257).await,
        Err(StoreError::InvalidRequest(_))
    ));
    TypedTaskStore::enable_event_outbox(&store)
        .await
        .expect("outbox is enabled");

    let first_id = typed_task_id(204);
    let second_id = typed_task_id(205);
    TypedTaskStore::accept_encoded(&store, first_id, typed_request(None))
        .await
        .expect("first task is accepted");
    TypedTaskStore::accept_encoded(&store, second_id, typed_request(None))
        .await
        .expect("second task is accepted");
    let first_page = TypedTaskStore::list_event_outbox(&store, 1)
        .await
        .expect("bounded page is read");
    assert_eq!(first_page.len(), 1);
    let first_event = &first_page[0];
    assert_eq!(first_event.task_id, first_id);
    assert_eq!(first_event.state_version, 0);
    assert!(!first_event.event_id.is_empty());
    assert!(first_event.event_json.contains("Queued"));

    TypedTaskStore::mark_event_published(&store, first_event.task_id, first_event.state_version)
        .await
        .expect("confirmed outbox event is removed");
    TypedTaskStore::mark_event_published(&store, first_event.task_id, first_event.state_version)
        .await
        .expect("repeated confirmation is idempotent");
    let remaining = TypedTaskStore::list_event_outbox(&store, 256)
        .await
        .expect("remaining outbox entries are read");
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].task_id, second_id);

    TypedTaskStore::release_owner(&store, epoch)
        .await
        .expect("owner is released");
    drop(store);
    remove_database(&path);
}

#[tokio_test]
async fn test_sqlite_typed_history_ready_retry_and_terminal_prune_paths() {
    let path = database_path("typed-history-retry-prune");
    let store = SqliteTaskStore::open(&path).expect("typed SQLite store opens");
    let first_id = typed_task_id(207);
    let second_id = typed_task_id(208);
    for id in [first_id, second_id] {
        TypedTaskStore::accept_encoded(&store, id, typed_request(None))
            .await
            .expect("typed task is accepted");
    }

    let history = TypedTaskStore::list_encoded(
        &store,
        crate::model::typed::TaskQuery {
            category: Some("reports".into()),
            limit: 1,
            ..crate::model::typed::TaskQuery::default()
        },
    )
    .await
    .expect("typed history page is read");
    assert_eq!(history.records.len(), 1);
    assert_eq!(history.records[0].id, first_id);
    assert!(history.next.is_some());

    let ready = TypedTaskStore::list_ready_queued(&store, None, std::num::NonZeroUsize::new(10).unwrap(), 10)
        .await
        .expect("ready queued page is read");
    assert_eq!(ready.records.len(), 2);
    assert_eq!(TypedTaskStore::next_retry_deadline(&store, 10).await.unwrap(), None);

    let started = TypedTaskStore::start_encoded(
        &store,
        StartCommand {
            id: first_id,
            expected_state_version: 0,
            started_at_ms: 11,
        },
    )
    .await
    .expect("first task starts");
    TypedTaskStore::transition_encoded(
        &store,
        EncodedTransitionCommand {
            id: first_id,
            expected_state_version: started.state_version,
            expected_attempt: started.attempt,
            retry_not_before_ms: Some(30),
            state: TaskState::Queued,
            cancel_requested: false,
            cancel_error: None,
            finished_at_ms: None,
            output: None,
        },
    )
    .await
    .expect("first task is delayed for retry");
    assert_eq!(TypedTaskStore::next_retry_deadline(&store, 10).await.unwrap(), Some(30));
    let ready_before_deadline =
        TypedTaskStore::list_ready_queued(&store, None, std::num::NonZeroUsize::new(10).unwrap(), 29)
            .await
            .expect("not yet due task is omitted");
    assert_eq!(ready_before_deadline.records.len(), 1);
    assert_eq!(ready_before_deadline.records[0].id, second_id);

    let restarted = TypedTaskStore::start_encoded(
        &store,
        StartCommand {
            id: first_id,
            expected_state_version: 2,
            started_at_ms: 30,
        },
    )
    .await
    .expect("retry becomes ready at its deadline");
    TypedTaskStore::transition_encoded(
        &store,
        EncodedTransitionCommand {
            id: first_id,
            expected_state_version: restarted.state_version,
            expected_attempt: restarted.attempt,
            retry_not_before_ms: None,
            state: TaskState::Succeeded,
            cancel_requested: false,
            cancel_error: None,
            finished_at_ms: Some(40),
            output: None,
        },
    )
    .await
    .expect("retried task completes");
    assert_eq!(
        TypedTaskStore::prune_terminal_before(&store, 41, std::num::NonZeroUsize::MIN)
            .await
            .expect("terminal task is pruned by finish time"),
        1
    );
    assert!(matches!(
        TypedTaskStore::prune_terminal_before(&store, u64::MAX, std::num::NonZeroUsize::MIN).await,
        Err(StoreError::InvalidRequest(_))
    ));
    assert!(
        TypedTaskStore::get_encoded_task(&store, first_id)
            .await
            .expect("pruned typed task lookup succeeds")
            .is_none()
    );

    drop(store);
    remove_database(&path);
}

#[tokio_test]
async fn test_sqlite_typed_schema_accepts_fixed_width_max_id_and_reopens_progress() {
    let path = database_path("typed-schema");
    let id = typed_task_id(u64::MAX);
    let store = SqliteTaskStore::open(&path).expect("typed SQLite store opens");
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
    assert_eq!(started.state, TaskState::Running);
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
    assert_eq!(stored_id, id.to_padded_decimal());
    drop(connection);
    drop(store);

    let store = SqliteTaskStore::open(&path).expect("typed database reopens");
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
    lifecycle["state"] = serde_json::to_value(TaskState::Succeeded).expect("success state serializes");
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

    let reopened = SqliteTaskStore::open(&path).expect("typed database remains readable");
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
    assert_eq!(schema_version, 6);
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
    let store = SqliteTaskStore::open(&path).expect("typed SQLite store opens");
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
            retry_not_before_ms: None,
            state: TaskState::Queued,
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
            retry_not_before_ms: None,
            state: TaskState::Succeeded,
            cancel_requested: false,
            cancel_error: None,
            finished_at_ms: Some(5),
            output: None,
        },
    )
    .await
    .expect("terminal state persists in lifecycle JSON");
    assert_eq!(terminal.state, TaskState::Succeeded);
    assert_eq!(terminal.finished_at_ms, Some(5));
    assert_eq!(terminal.progress.as_ref().map(|progress| progress.attempt), Some(2));

    drop(store);
    remove_database(&path);
}

#[tokio_test]
async fn test_sqlite_open_rejects_legacy_schema_without_rewriting_it() {
    let path = database_path("typed-legacy-reject");
    let id = seed_legacy_database(&path);
    let before = Connection::open(&path)
        .expect("legacy database opens")
        .query_row("SELECT record_json FROM tasks WHERE id=?1", [&id], |row| {
            row.get::<_, String>(0)
        })
        .expect("legacy record is readable before open");
    let error = match SqliteTaskStore::open(&path) {
        Ok(_) => panic!("typed API must reject a legacy UUID database"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("explicit task ID mapping"));
    let connection = Connection::open(&path).expect("legacy database remains readable");
    let after: String = connection
        .query_row("SELECT record_json FROM tasks WHERE id=?1", [&id], |row| {
            row.get(0)
        })
        .expect("legacy record remains present");
    assert_eq!(after, before);
    let version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .expect("legacy schema version remains readable");
    assert_eq!(version, 3);
    drop(connection);
    remove_database(&path);
}

#[tokio_test]
async fn test_sqlite_owner_epoch_advances_after_release_and_reacquire() {
    let path = database_path("owner-epoch-reacquire");
    let store = SqliteTaskStore::open(&path).expect("SQLite store opens");
    let first_epoch = TypedTaskStore::acquire_owner(&store).await.expect("first owner is acquired");
    TypedTaskStore::release_owner(&store, first_epoch)
        .await
        .expect("first owner releases");
    let second_epoch = TypedTaskStore::acquire_owner(&store).await.expect("owner can be reacquired");
    assert!(second_epoch.0 > first_epoch.0);
    assert!(matches!(
        TypedTaskStore::release_owner(&store, first_epoch).await,
        Err(StoreError::OwnerConflict)
    ));
    TypedTaskStore::release_owner(&store, second_epoch)
        .await
        .expect("current owner releases");
    drop(store);
    remove_database(&path);
}
