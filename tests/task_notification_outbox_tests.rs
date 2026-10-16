// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Durable task notification storage and publisher behavior.
#![cfg(feature = "sqlite")]

mod support;

use std::env;
use std::path::PathBuf;

use qubit_id::Id;
use qubit_metadata::Metadata;
use qubit_task::model::ProgressCommand;
use qubit_task::model::ResourceRequest;
use qubit_task::model::StartCommand;
use qubit_task::model::StoredPayload;
use qubit_task::model::StoredTaskRequest;
use qubit_task::model::TaskId;
use qubit_task::model::TaskState;
use qubit_task::model::TransitionCommand;
use qubit_task::store::MemoryTaskStore;
use qubit_task::store::SqliteTaskStore;
use qubit_task::store::StoreError;
use qubit_task::store::TaskStore;
use rusqlite::Connection;
use uuid::Uuid;

/// Creates an isolated temporary database path.
fn database_path() -> PathBuf {
    env::temp_dir().join(format!("task-outbox-{}.sqlite", Uuid::new_v4()))
}

#[tokio::test]
async fn test_outbox_requires_owner_and_valid_page_size() {
    let path = database_path();
    let store = SqliteTaskStore::open(&path).expect("open database");
    assert!(matches!(
        store.enable_event_outbox().await,
        Err(StoreError::OwnerConflict)
    ));
    let epoch = store.acquire_owner().await.expect("acquire owner");
    store.enable_event_outbox().await.expect("enable outbox");
    assert!(store.list_event_outbox(128).await.expect("read page").is_empty());
    assert!(matches!(
        store.list_event_outbox(0).await,
        Err(StoreError::InvalidRequest(_))
    ));
    store.release_owner(epoch).await.expect("release owner");
    assert!(matches!(
        store.list_event_outbox(128).await,
        Err(StoreError::OwnerConflict)
    ));
}

#[tokio::test]
async fn test_memory_store_reports_unsupported_quarantine_capability() {
    let store = MemoryTaskStore::new(8);
    assert!(matches!(
        store
            .quarantine_event_outbox_head(
                TaskId::from_id(Id::new(40)),
                0,
                "task:40:0",
                "operator decision",
                "publish failed",
            )
            .await,
        Err(StoreError::UnsupportedCapability)
    ));
}

#[tokio::test]
async fn test_schema_five_migrates_to_eight_and_reopens_without_rewriting_tasks() {
    let path = database_path();
    let store = SqliteTaskStore::open(&path).expect("initialize database");
    let id = TaskId::from_id(Id::new(41));
    store
        .accept_encoded(id, request())
        .await
        .expect("persist existing task");
    drop(store);
    let connection = Connection::open(&path).expect("open fixture");
    let before: (String, Vec<u8>, String) = connection
        .query_row(
            "SELECT request_info_json,payload,lifecycle_json FROM tasks",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .expect("original row");
    connection
        .execute_batch(
            "DROP TABLE IF EXISTS task_event_outbox; DROP TABLE task_event_outbox_quarantine; DROP TABLE task_terminal_checkpoints; PRAGMA user_version=5;",
        )
        .expect("simulate v5");
    drop(connection);
    drop(SqliteTaskStore::open(&path).expect("migrate v5"));
    drop(SqliteTaskStore::open(&path).expect("reopen v8"));
    let connection = Connection::open(&path).expect("inspect schema");
    let version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .expect("version");
    assert_eq!(version, 8);
    let after: (String, Vec<u8>, String) = connection
        .query_row(
            "SELECT request_info_json,payload,lifecycle_json FROM tasks",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .expect("migrated row");
    assert_eq!(before, after);
    let index: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE name='task_event_outbox_created' AND type='index'",
            [],
            |row| row.get(0),
        )
        .expect("index");
    assert_eq!(index, 1);
    connection
        .prepare("SELECT task_id,state_version,event_id,event_json,created_at_ms FROM task_event_outbox")
        .expect("outbox schema");
}

#[tokio::test]
async fn test_schema_six_migrates_outbox_rows_to_eight_and_reopens() {
    let path = database_path();
    let store = SqliteTaskStore::open(&path).expect("initialize database");
    let owner = store.acquire_owner().await.expect("owner");
    store.enable_event_outbox().await.expect("enable outbox");
    let id = TaskId::from_id(Id::new(51));
    store.accept_encoded(id, request()).await.expect("accept");
    let running = store
        .start_encoded(StartCommand {
            id,
            expected_state_version: 0,
            started_at_ms: 10,
        })
        .await
        .expect("start");
    store
        .update_progress(ProgressCommand::new(id, running.attempt, 1, None, Vec::new(), 11))
        .await
        .expect("progress");
    let before = store.list_event_outbox(128).await.expect("outbox before migration");
    assert_eq!(before.len(), 2);
    let fresh_version: i64 = Connection::open(&path)
        .expect("inspect fresh schema")
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .expect("fresh schema version");
    assert_eq!(fresh_version, 8);
    store.release_owner(owner).await.expect("release owner");
    drop(store);

    let connection = Connection::open(&path).expect("open v6 fixture");
    connection
        .execute_batch("DROP TABLE task_event_outbox_quarantine; DROP TABLE task_terminal_checkpoints;")
        .expect("remove v7 quarantine table");
    connection
        .pragma_update(None, "user_version", 6)
        .expect("mark fixture as v6");
    drop(connection);
    drop(SqliteTaskStore::open(&path).expect("migrate v6"));
    drop(SqliteTaskStore::open(&path).expect("reopen v8"));

    let store = SqliteTaskStore::open(&path).expect("open migrated database");
    let owner = store.acquire_owner().await.expect("owner after migration");
    store.enable_event_outbox().await.expect("enable after migration");
    let after = store.list_event_outbox(128).await.expect("retained rows");
    assert_eq!(after.len(), 2);
    for (original, migrated) in before.iter().zip(&after) {
        assert_eq!(migrated.task_id, original.task_id);
        assert_eq!(migrated.state_version, original.state_version);
        assert_eq!(migrated.event_id, original.event_id);
        assert_eq!(migrated.event_json, original.event_json);
    }
    store.release_owner(owner).await.expect("release owner");
    let connection = Connection::open(&path).expect("inspect schema");
    let version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .expect("schema version");
    assert_eq!(version, 8);
    connection
        .prepare("SELECT task_id,state_version,event_id,event_json,created_at_ms,quarantined_at_ms,reason,last_error FROM task_event_outbox_quarantine")
        .expect("quarantine schema");
}

#[tokio::test]
async fn test_schema_four_and_five_migrate_tasks_to_eight() {
    for version in [4, 5] {
        let path = database_path();
        let store = SqliteTaskStore::open(&path).expect("initialize database");
        let id = TaskId::from_id(Id::new(52 + version as u64));
        store.accept_encoded(id, request()).await.expect("persist task");
        drop(store);
        let connection = Connection::open(&path).expect("open fixture");
        connection
            .execute_batch("DROP INDEX IF EXISTS task_event_outbox_created; DROP TABLE IF EXISTS task_event_outbox;")
            .expect("remove v6 outbox");
        connection
            .execute_batch("DROP TABLE task_event_outbox_quarantine; DROP TABLE task_terminal_checkpoints;")
            .expect("remove v7 quarantine table");
        if version == 4 {
            connection
                .execute_batch(
                    "DROP INDEX tasks_queued_retry_deadline; ALTER TABLE tasks DROP COLUMN retry_not_before_ms;",
                )
                .expect("restore v4 tasks schema");
        }
        connection
            .pragma_update(None, "user_version", version)
            .expect("set old schema version");
        drop(connection);

        drop(SqliteTaskStore::open(&path).expect("migrate old schema"));
        let connection = Connection::open(&path).expect("inspect migrated database");
        let migrated_version: i64 = connection
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .expect("schema version");
        assert_eq!(migrated_version, 8);
        let task_count: i64 = connection
            .query_row("SELECT COUNT(*) FROM tasks", [], |row| row.get(0))
            .expect("retained task count");
        assert_eq!(task_count, 1);
        connection
            .prepare("SELECT task_id,state_version,event_id,event_json,created_at_ms,quarantined_at_ms,reason,last_error FROM task_event_outbox_quarantine")
            .expect("quarantine schema");
    }
}

#[tokio::test]
async fn test_schema_seven_migrates_to_eight_preserving_outbox_and_quarantine() {
    let path = database_path();
    let store = SqliteTaskStore::open(&path).expect("initialize database");
    let id = TaskId::from_id(Id::new(59));
    store.accept_encoded(id, request()).await.expect("accept task");
    drop(store);
    let connection = Connection::open(&path).expect("open v7 fixture");
    connection
        .execute(
            "INSERT INTO task_event_outbox(task_id,state_version,event_id,event_json,created_at_ms) VALUES (?1,7,'task:59:7','{}',70)",
            [id.to_padded_decimal()],
        )
        .expect("seed active outbox");
    connection
        .execute(
            "INSERT INTO task_event_outbox_quarantine(task_id,state_version,event_id,event_json,created_at_ms,quarantined_at_ms,reason,last_error) VALUES (?1,8,'task:59:8','{}',80,81,'reason','error')",
            [id.to_padded_decimal()],
        )
        .expect("seed quarantined event");
    connection
        .execute_batch("DROP TABLE task_terminal_checkpoints; PRAGMA user_version=7;")
        .expect("mark fixture v7");
    drop(connection);
    drop(SqliteTaskStore::open(&path).expect("migrate v7"));
    drop(SqliteTaskStore::open(&path).expect("reopen v8"));
    let connection = Connection::open(&path).expect("inspect migrated v7 fixture");
    let version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .expect("schema version");
    assert_eq!(version, 8);
    for table in ["task_event_outbox", "task_event_outbox_quarantine"] {
        let count: i64 = connection
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| row.get(0))
            .expect("retained row count");
        assert_eq!(count, 1, "{table} data survives v7 migration");
    }
}

#[test]
fn test_schema_eight_reopen_rejects_invalid_checkpoint_table_without_repair() {
    let path = database_path();
    drop(SqliteTaskStore::open(&path).expect("initialize database"));
    let connection = Connection::open(&path).expect("open database");
    connection
        .execute_batch("ALTER TABLE task_terminal_checkpoints RENAME COLUMN state_json TO corrupt_json;")
        .expect("inject invalid checkpoint schema");
    assert!(matches!(SqliteTaskStore::open(&path), Err(StoreError::Failure(_))));
    connection
        .prepare("SELECT corrupt_json FROM task_terminal_checkpoints")
        .expect("invalid schema remains intact");
}

#[tokio::test]
async fn test_terminal_prune_skips_outbox_then_retains_checkpoint_atomically() {
    let path = database_path();
    let store = SqliteTaskStore::open(&path).expect("open database");
    let owner = store.acquire_owner().await.expect("owner");
    store.enable_event_outbox().await.expect("enable outbox");
    let id = TaskId::from_id(Id::new(60));
    let accepted = store.accept_encoded(id, request()).await.expect("accept task");
    let current_checkpoint = store
        .get_state_checkpoint(id)
        .await
        .expect("current checkpoint lookup")
        .expect("accepted task has a checkpoint");
    assert_eq!(current_checkpoint.id, id);
    assert_eq!(current_checkpoint.state_version, accepted.summary.state_version);
    assert_eq!(current_checkpoint.state, accepted.summary.state);
    let running = store
        .start_encoded(StartCommand {
            id,
            expected_state_version: accepted.summary.state_version,
            started_at_ms: 10,
        })
        .await
        .expect("start task");
    let terminal = store
        .transition_encoded(TransitionCommand {
            id,
            expected_state_version: running.state_version,
            expected_attempt: running.attempt,
            state: TaskState::Succeeded,
            cancel_requested: false,
            cancel_error: None,
            retry_not_before_ms: None,
            finished_at_ms: Some(20),
            output: None,
        })
        .await
        .expect("finish task");
    let limit = std::num::NonZeroUsize::new(1).expect("nonzero limit");
    assert_eq!(
        store
            .prune_terminal_before(21, limit)
            .await
            .expect("skip active outbox"),
        0
    );
    assert!(store.get_encoded_task(id).await.expect("task lookup").is_some());

    let events = store.list_event_outbox(128).await.expect("read outbox");
    for event in events {
        store
            .mark_event_published(id, event.state_version)
            .await
            .expect("mark event published");
    }
    let connection = Connection::open(&path).expect("fault injection connection");
    connection
        .execute_batch("CREATE TRIGGER fail_checkpoint BEFORE INSERT ON task_terminal_checkpoints BEGIN SELECT RAISE(ABORT, 'injected checkpoint failure'); END;")
        .expect("inject checkpoint failure");
    assert!(matches!(
        store.prune_terminal_before(21, limit).await,
        Err(StoreError::Failure(_))
    ));
    assert!(store.get_encoded_task(id).await.expect("task after rollback").is_some());
    let duplicate = store
        .accept_encoded(TaskId::from_id(Id::new(61)), request())
        .await
        .expect("idempotency key remains reserved after rollback");
    assert!(!duplicate.created);
    connection
        .execute_batch("DROP TRIGGER fail_checkpoint;")
        .expect("remove trigger");

    assert_eq!(
        store
            .prune_terminal_before(21, limit)
            .await
            .expect("prune published task"),
        1
    );
    assert!(store.get_encoded_task(id).await.expect("pruned task lookup").is_none());
    let checkpoint = store
        .get_state_checkpoint(id)
        .await
        .expect("checkpoint lookup")
        .expect("pruned task checkpoint");
    assert_eq!(checkpoint.id, id);
    assert_eq!(checkpoint.state_version, terminal.state_version);
    assert_eq!(checkpoint.state, terminal.state);
    assert!(
        store
            .accept_encoded(TaskId::from_id(Id::new(62)), request())
            .await
            .expect("pruned idempotency key is released")
            .created
    );
    store.release_owner(owner).await.expect("release owner");
}

#[tokio::test]
async fn test_quarantine_requires_current_owned_head_and_archives_atomically() {
    let path = database_path();
    let store = SqliteTaskStore::open(&path).expect("open database");
    let id = TaskId::from_id(Id::new(61));
    assert!(matches!(
        store
            .quarantine_event_outbox_head(id, 0, "task:61:0", "operator decision", "decode failed")
            .await,
        Err(StoreError::OwnerConflict)
    ));
    let owner = store.acquire_owner().await.expect("owner");
    store.enable_event_outbox().await.expect("enable outbox");
    store.accept_encoded(id, request()).await.expect("accept");
    let running = store
        .start_encoded(StartCommand {
            id,
            expected_state_version: 0,
            started_at_ms: 10,
        })
        .await
        .expect("start");
    store
        .update_progress(ProgressCommand::new(id, running.attempt, 1, None, Vec::new(), 11))
        .await
        .expect("progress");
    let events = store.list_event_outbox(128).await.expect("outbox");
    let head = &events[0];
    let next = &events[1];
    let connection = Connection::open(&path).expect("fault injection connection");
    let original_created_at_ms: i64 = connection
        .query_row(
            "SELECT created_at_ms FROM task_event_outbox WHERE task_id=?1 AND state_version=?2",
            rusqlite::params![head.task_id.to_padded_decimal(), head.state_version],
            |row| row.get(0),
        )
        .expect("original event timestamp");
    for (expected_id, expected_version, expected_event_id) in [
        (
            TaskId::from_id(Id::new(999)),
            head.state_version,
            head.event_id.as_str(),
        ),
        (head.task_id, head.state_version + 8, head.event_id.as_str()),
        (head.task_id, head.state_version, "wrong-event-id"),
        (next.task_id, next.state_version, next.event_id.as_str()),
    ] {
        assert!(matches!(
            store
                .quarantine_event_outbox_head(
                    expected_id,
                    expected_version,
                    expected_event_id,
                    "operator decision",
                    "decode failed",
                )
                .await,
            Err(StoreError::Conflict)
        ));
    }
    assert!(matches!(
        store
            .quarantine_event_outbox_head(head.task_id, head.state_version, &head.event_id, " \t", "decode failed")
            .await,
        Err(StoreError::InvalidRequest(_))
    ));
    assert!(matches!(
        store
            .quarantine_event_outbox_head(
                head.task_id,
                head.state_version,
                &head.event_id,
                "x".repeat(4097).as_str(),
                "decode failed",
            )
            .await,
        Err(StoreError::InvalidRequest(_))
    ));

    connection
        .execute_batch("CREATE TRIGGER fail_quarantine BEFORE INSERT ON task_event_outbox_quarantine BEGIN SELECT RAISE(ABORT, 'injected quarantine failure'); END;")
        .expect("inject archive failure");
    assert!(matches!(
        store
            .quarantine_event_outbox_head(
                head.task_id,
                head.state_version,
                &head.event_id,
                "operator decision",
                "decode failed"
            )
            .await,
        Err(StoreError::Failure(_))
    ));
    assert_eq!(
        store.list_event_outbox(128).await.expect("rows after rollback").len(),
        2
    );
    connection
        .execute_batch("DROP TRIGGER fail_quarantine;")
        .expect("remove trigger");
    store
        .quarantine_event_outbox_head(
            head.task_id,
            head.state_version,
            &head.event_id,
            "operator decision",
            "decode failed",
        )
        .await
        .expect("quarantine current head");
    assert_eq!(store.list_event_outbox(128).await.expect("remaining outbox").len(), 1);
    let archived: (String, u64, String, String, i64, i64, String, String) = connection
        .query_row(
            "SELECT task_id,state_version,event_id,event_json,created_at_ms,quarantined_at_ms,reason,last_error FROM task_event_outbox_quarantine",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?, row.get(6)?, row.get(7)?)),
        )
        .expect("archived event");
    assert_eq!(archived.0, head.task_id.to_padded_decimal());
    assert_eq!(archived.1, head.state_version);
    assert_eq!(archived.2, head.event_id);
    assert_eq!(archived.3, head.event_json);
    assert_eq!(archived.4, original_created_at_ms);
    assert!(archived.5 > 0);
    assert_eq!(archived.6, "operator decision");
    assert_eq!(archived.7, "decode failed");
    let transition = TransitionCommand {
        id,
        expected_state_version: 1,
        expected_attempt: running.attempt,
        state: TaskState::Succeeded,
        cancel_requested: false,
        cancel_error: None,
        retry_not_before_ms: None,
        finished_at_ms: Some(20),
        output: None,
    };
    store.transition_encoded(transition).await.expect("finish task");
    store
        .prune_terminal_before(21, std::num::NonZeroUsize::new(1).expect("nonzero batch"))
        .await
        .expect("prune terminal task");
    let archive_count: i64 = connection
        .query_row("SELECT COUNT(*) FROM task_event_outbox_quarantine", [], |row| {
            row.get(0)
        })
        .expect("archive survives cleanup");
    assert_eq!(archive_count, 1);
    store.release_owner(owner).await.expect("release owner");
}

#[test]
fn test_invalid_v6_outbox_is_rejected_without_repair() {
    let path = database_path();
    drop(SqliteTaskStore::open(&path).expect("initialize database"));
    let connection = Connection::open(&path).expect("open database");
    connection
        .execute_batch("ALTER TABLE task_event_outbox RENAME COLUMN event_json TO corrupt_json")
        .expect("inject invalid schema");
    assert!(matches!(SqliteTaskStore::open(&path), Err(StoreError::Failure(_))));
    connection
        .prepare("SELECT corrupt_json FROM task_event_outbox")
        .expect("invalid schema was preserved");
}

/// Encoded request independent of the event-bus feature.
fn request() -> StoredTaskRequest {
    StoredTaskRequest {
        kind_id: "outbox.test".into(),
        category: None,
        payload: StoredPayload {
            type_id: "qubit_task.tests.Payload".try_into().expect("type ID"),
            schema_version: 1,
            codec_id: "json".into(),
            bytes: vec![1],
        },
        metadata: Metadata::new(),
        resource_limit: ResourceRequest::default(),
        correlation_key: None,
        idempotency_key: Some("same-task".into()),
    }
}

#[tokio::test]
async fn test_lifecycle_outbox_snapshots_are_atomic_and_idempotent() {
    let path = database_path();
    let store = SqliteTaskStore::open(&path).expect("open database");
    let epoch = store.acquire_owner().await.expect("owner");
    store.enable_event_outbox().await.expect("enable");
    let id = TaskId::from_id(Id::new(42));
    store.accept_encoded(id, request()).await.expect("accept");
    assert!(!store.accept_encoded(id, request()).await.expect("repeat").created);
    let running = store
        .start_encoded(StartCommand {
            id,
            expected_state_version: 0,
            started_at_ms: 10,
        })
        .await
        .expect("start");
    store
        .update_progress(ProgressCommand::new(id, running.attempt, 1, None, Vec::new(), 10))
        .await
        .expect("progress");
    assert_eq!(store.list_event_outbox(128).await.expect("after progress").len(), 2);
    let transition = TransitionCommand {
        id,
        expected_state_version: 1,
        expected_attempt: running.attempt,
        state: TaskState::Succeeded,
        cancel_requested: false,
        cancel_error: None,
        retry_not_before_ms: None,
        finished_at_ms: Some(11),
        output: None,
    };
    let connection = Connection::open(&path).expect("fault injection connection");
    connection.execute_batch("CREATE TRIGGER fail_outbox BEFORE INSERT ON task_event_outbox BEGIN SELECT RAISE(ABORT, 'injected outbox failure'); END;").expect("inject failure");
    assert!(matches!(
        store.transition_encoded(transition.clone()).await,
        Err(StoreError::Failure(_))
    ));
    assert_eq!(
        store
            .get_encoded_task(id)
            .await
            .expect("get")
            .expect("task")
            .summary
            .state_version,
        1
    );
    connection
        .execute_batch("DROP TRIGGER fail_outbox;")
        .expect("restore outbox");
    store.transition_encoded(transition.clone()).await.expect("finish");
    assert!(matches!(
        store.transition_encoded(transition).await,
        Err(StoreError::Conflict)
    ));
    let events = store.list_event_outbox(128).await.expect("events");
    assert_eq!(
        events.iter().map(|entry| entry.state_version).collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    assert_eq!(events[0].event_id, "task:42:0");
    assert!(events[0].event_json.contains("Queued"));
    assert!(events[1].event_json.contains("Running"));
    assert!(events[2].event_json.contains("Succeeded"));
    for _ in 0..2 {
        store.mark_event_published(id, 0).await.expect("idempotent deletion");
    }
    assert_eq!(store.list_event_outbox(128).await.expect("remaining").len(), 2);
    store.release_owner(epoch).await.expect("release");
    assert!(matches!(
        store.mark_event_published(id, 1).await,
        Err(StoreError::OwnerConflict)
    ));
}

#[test]
fn test_v6_missing_outbox_index_is_rejected() {
    let path = database_path();
    drop(SqliteTaskStore::open(&path).expect("initialize database"));
    let connection = Connection::open(&path).expect("open database");
    connection
        .execute_batch("DROP INDEX task_event_outbox_created")
        .expect("remove index");
    assert!(matches!(SqliteTaskStore::open(&path), Err(StoreError::Failure(_))));
}

#[cfg(feature = "event-bus")]
mod publisher_tests;

#[tokio::test]
async fn test_oversized_event_rolls_back_acceptance() {
    let store = SqliteTaskStore::open(database_path()).expect("store");
    let owner = store.acquire_owner().await.expect("owner");
    store.enable_event_outbox().await.expect("enable");
    let id = TaskId::from_id(Id::new(43));
    let mut large = request();
    large.correlation_key = Some("x".repeat(128 * 1024));
    assert!(matches!(
        store.accept_encoded(id, large).await,
        Err(StoreError::InvalidRequest(_))
    ));
    assert!(store.get_encoded_task(id).await.expect("lookup").is_none());
    assert!(store.list_event_outbox(128).await.expect("outbox").is_empty());
    store.release_owner(owner).await.expect("release");
}

#[test]
fn test_v6_missing_outbox_primary_key_is_rejected() {
    let path = database_path();
    drop(SqliteTaskStore::open(&path).expect("initialize database"));
    let connection = Connection::open(&path).expect("open database");
    connection.execute_batch("DROP TABLE task_event_outbox; CREATE TABLE task_event_outbox(task_id TEXT NOT NULL,state_version INTEGER NOT NULL,event_id TEXT NOT NULL,event_json TEXT NOT NULL,created_at_ms INTEGER NOT NULL); CREATE INDEX task_event_outbox_created ON task_event_outbox(created_at_ms,task_id,state_version);").expect("remove primary key from fixture");
    assert!(matches!(SqliteTaskStore::open(&path), Err(StoreError::Failure(_))));
}

#[test]
fn test_v6_outbox_index_on_another_table_is_rejected() {
    let path = database_path();
    drop(SqliteTaskStore::open(&path).expect("initialize database"));
    let connection = Connection::open(&path).expect("open database");
    connection
        .execute_batch(
            "DROP INDEX task_event_outbox_created;
         CREATE TABLE unrelated_events(created_at_ms INTEGER,task_id TEXT,state_version INTEGER);
         CREATE INDEX task_event_outbox_created ON unrelated_events(created_at_ms,task_id,state_version);",
        )
        .expect("install a same-named index on another table");
    let error = match SqliteTaskStore::open(&path) {
        Ok(_) => panic!("an index on another table cannot satisfy the outbox schema"),
        Err(error) => error,
    };
    assert!(
        matches!(error, StoreError::Failure(message) if message == "SQLite event outbox is missing its ordered index")
    );
    let indexed_table: String = connection
        .query_row(
            "SELECT tbl_name FROM sqlite_master WHERE type='index' AND name='task_event_outbox_created'",
            [],
            |row| row.get(0),
        )
        .expect("inspect preserved invalid index");
    assert_eq!(
        indexed_table, "unrelated_events",
        "opening must not silently repair invalid v6 data"
    );
}
