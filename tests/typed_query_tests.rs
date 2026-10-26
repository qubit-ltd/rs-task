// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
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
use qubit_task::model::TaskQuery;
use qubit_task::model::TaskState;
use qubit_task::model::TaskStateKind;
use qubit_task::model::TransitionCommand;
use qubit_task::store::MemoryTaskStore;
#[cfg(feature = "sqlite")]
use qubit_task::store::SqliteTaskStore;
use qubit_task::store::TaskStore;
#[cfg(feature = "sqlite")]
use rusqlite::Connection;
use uuid::Uuid;

fn request(category: &str, correlation_key: &str) -> StoredTaskRequest {
    StoredTaskRequest {
        kind_id: "query.test".to_owned(),
        category: Some(category.to_owned()),
        payload: StoredPayload {
            type_id: ModelIdBuf::parse("qubit_task.tests.Payload").expect("valid model ID"),
            schema_version: 1,
            codec_id: "qubit.bytes.json".to_owned(),
            bytes: vec![1],
        },
        metadata: Metadata::new(),
        resource_limit: ResourceRequest::default(),
        correlation_key: Some(correlation_key.to_owned()),
        idempotency_key: None,
    }
}

fn id(value: u64) -> TaskId {
    TaskId::from_id(Id::new(value))
}

#[cfg(feature = "sqlite")]
fn remove_database(path: &Path) {
    let mut owner_lock = path.as_os_str().to_owned();
    owner_lock.push(".owner.lock");
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(owner_lock);
    let _ = fs::remove_file(path.with_extension("sqlite-wal"));
    let _ = fs::remove_file(path.with_extension("sqlite-shm"));
}

async fn ids_in_order(store: &dyn TaskStore) -> Vec<u64> {
    let page = store
        .list_encoded(TaskQuery {
            limit: 10,
            ..TaskQuery::default()
        })
        .await
        .expect("typed history query succeeds");
    page.records
        .iter()
        .map(|summary| summary.id.into_id().value())
        .collect()
}

async fn assert_category_filter_and_exclusive_numeric_cursor(store: &dyn TaskStore) {
    for task_id in [id(10), id(2)] {
        store
            .accept_encoded(task_id, request("image", "batch-a"))
            .await
            .expect("task is accepted");
    }
    store
        .accept_encoded(id(5), request("video", "batch-a"))
        .await
        .expect("other category is accepted");
    store
        .accept_encoded(id(6), request("image", "batch-b"))
        .await
        .expect("other correlation is accepted");

    let first = store
        .list_encoded(TaskQuery {
            states: vec![TaskStateKind::Queued],
            category: Some("image".to_owned()),
            correlation_key: Some("batch-a".to_owned()),
            limit: 1,
            ..TaskQuery::default()
        })
        .await
        .expect("filtered first page succeeds");
    assert_eq!(first.records.len(), 1);
    assert!(first.records.iter().all(|record| {
        record.category.as_deref() == Some("image")
            && record.correlation_key.as_deref() == Some("batch-a")
            && record.state == TaskState::Queued
    }));
    assert!(first.next.is_some());

    store
        .accept_encoded(id(u64::MAX), request("image", "batch-a"))
        .await
        .expect("a later matching task is accepted");

    let second = store
        .list_encoded(TaskQuery {
            states: vec![TaskStateKind::Queued],
            category: Some("image".to_owned()),
            correlation_key: Some("batch-a".to_owned()),
            after: first.next,
            limit: 2,
        })
        .await
        .expect("exclusive continuation page succeeds");
    assert_eq!(first.records.len() + second.records.len(), 3);
    assert!(second.next.is_none());
    let prior = first.records.last().expect("first page is nonempty");
    let next = second.records.first().expect("second page is nonempty");
    assert!((next.accepted_at_ms, next.id) > (prior.accepted_at_ms, prior.id));
    assert!(second.records.iter().any(|record| record.id == id(u64::MAX)));
    let mut combined = first.records.iter().chain(&second.records).collect::<Vec<_>>();
    if combined
        .iter()
        .all(|record| record.accepted_at_ms == combined[0].accepted_at_ms)
    {
        combined.sort_by_key(|record| record.id);
        assert_eq!(
            combined
                .iter()
                .map(|record| record.id.into_id().value())
                .collect::<Vec<_>>(),
            [2, 10, u64::MAX]
        );
    }
}

async fn assert_ready_queue_contract(store: &dyn TaskStore) {
    assert!(
        store
            .list_ready_queued(None, NonZeroUsize::new(257).expect("nonzero page limit"), 0,)
            .await
            .is_err()
    );
    for value in [20, 10, 30, 40] {
        let task_id = id(value);
        let accepted = store
            .accept_encoded(task_id, request("ready", &format!("{value}")))
            .await
            .expect("task is accepted");
        if value != 40 {
            let running = store
                .start_encoded(StartCommand {
                    id: task_id,
                    expected_state_version: accepted.summary.state_version,
                    started_at_ms: 10,
                })
                .await
                .expect("queued task starts");
            store
                .transition_encoded(TransitionCommand {
                    id: task_id,
                    expected_state_version: running.state_version,
                    expected_attempt: running.attempt,
                    retry_not_before_ms: Some(match value {
                        20 => 100,
                        10 => 200,
                        _ => 300,
                    }),
                    state: TaskState::Queued,
                    cancel_requested: false,
                    cancel_error: None,
                    finished_at_ms: None,
                    output: None,
                })
                .await
                .expect("task is rescheduled");
        }
    }
    let running_id = id(50);
    let running = store
        .accept_encoded(running_id, request("ready", "running"))
        .await
        .expect("running task is accepted");
    store
        .start_encoded(StartCommand {
            id: running_id,
            expected_state_version: running.summary.state_version,
            started_at_ms: 10,
        })
        .await
        .expect("task starts");
    let blocked_id = id(60);
    let blocked = store
        .accept_encoded(blocked_id, request("ready", "blocked"))
        .await
        .expect("blocked task is accepted");
    store
        .transition_encoded(TransitionCommand {
            id: blocked_id,
            expected_state_version: blocked.summary.state_version,
            expected_attempt: 0,
            retry_not_before_ms: None,
            state: TaskState::Blocked { reason: "test".into() },
            cancel_requested: false,
            cancel_error: None,
            finished_at_ms: None,
            output: None,
        })
        .await
        .expect("task is blocked");
    let first = store
        .list_ready_queued(None, NonZeroUsize::new(2).expect("nonzero page limit"), 200)
        .await
        .expect("first ready page succeeds");
    assert_eq!(first.records.len(), 2);
    assert!(first.next.is_some());
    store
        .accept_encoded(id(70), request("ready", "submitted-between-pages"))
        .await
        .expect("task submitted between pages is accepted");
    let second = store
        .list_ready_queued(first.next, NonZeroUsize::new(2).expect("nonzero page limit"), 200)
        .await
        .expect("second ready page succeeds");
    assert_eq!(second.records.len(), 2);
    assert!(second.next.is_none());
    let mut ready = first
        .records
        .iter()
        .chain(&second.records)
        .map(|row| row.id)
        .collect::<Vec<_>>();
    ready.sort();
    assert_eq!(ready, [id(10), id(20), id(40), id(70)]);
    assert_eq!(
        store
            .next_retry_deadline(200)
            .await
            .expect("next retry deadline query succeeds"),
        Some(300)
    );
    assert_eq!(
        store
            .next_retry_deadline(300)
            .await
            .expect("next retry deadline query succeeds"),
        None
    );
}

#[tokio::test]
async fn test_ready_queue_filters_due_retries_and_pages_in_memory() {
    assert_ready_queue_contract(&MemoryTaskStore::new(16)).await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_ready_queue_filters_due_retries_and_pages_in_sqlite() {
    let db_path = env::temp_dir().join(format!("qubit-task-ready-{}.sqlite", Uuid::new_v4()));
    let store = SqliteTaskStore::open(&db_path).expect("SQLite store opens");
    assert_ready_queue_contract(&store).await;
    drop(store);
    remove_database(&db_path);
}

#[cfg(feature = "sqlite")]
#[test]
fn test_ready_queue_and_retry_deadline_queries_use_partial_indexes() {
    let db_path = env::temp_dir().join(format!("qubit-task-ready-plan-{}.sqlite", Uuid::new_v4()));
    let store = SqliteTaskStore::open(&db_path).expect("SQLite store opens");
    drop(store);
    let connection = Connection::open(&db_path).expect("SQLite database opens");
    let ready_plan = {
        let mut ready = connection
            .prepare(concat!(
                "EXPLAIN QUERY PLAN SELECT id FROM tasks INDEXED BY tasks_queued_accepted_id ",
                "WHERE state_kind='Queued' AND (retry_not_before_ms IS NULL OR retry_not_before_ms<=?1) ",
                "ORDER BY accepted_at,id LIMIT ?2",
            ))
            .expect("ready queue query plan is prepared");
        ready
            .query_map([200_i64, 10_i64], |row| row.get::<_, String>(3))
            .expect("ready queue query plan is read")
            .map(|row| row.expect("ready queue query plan row is valid"))
            .collect::<Vec<_>>()
            .join(" ")
    };
    assert!(ready_plan.contains("tasks_queued_accepted_id"), "{ready_plan}");
    let retry_plan = {
        let mut statement = connection
            .prepare(concat!(
                "EXPLAIN QUERY PLAN SELECT MIN(retry_not_before_ms) FROM tasks ",
                "INDEXED BY tasks_queued_retry_deadline WHERE state_kind='Queued' ",
                "AND retry_not_before_ms>?1",
            ))
            .expect("retry deadline query plan is prepared");
        statement
            .query_map([200_i64], |row| row.get::<_, String>(3))
            .expect("retry deadline query plan is read")
            .map(|row| row.expect("retry deadline query plan row is valid"))
            .collect::<Vec<_>>()
            .join(" ")
    };
    assert!(retry_plan.contains("tasks_queued_retry_deadline"), "{retry_plan}");
    drop(connection);
    remove_database(&db_path);
}

#[tokio::test]
async fn test_typed_history_filters_category_and_uses_exclusive_numeric_cursor() {
    let memory = MemoryTaskStore::new(16);
    assert_category_filter_and_exclusive_numeric_cursor(&memory).await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_typed_history_filters_category_and_uses_exclusive_numeric_cursor_in_sqlite() {
    let db_path = env::temp_dir().join(format!("qubit-task-typed-query-{}.sqlite", Uuid::new_v4()));
    let memory = MemoryTaskStore::new(16);
    let sqlite = SqliteTaskStore::open(&db_path).expect("typed SQLite store opens");
    assert_category_filter_and_exclusive_numeric_cursor(&memory).await;
    assert_category_filter_and_exclusive_numeric_cursor(&sqlite).await;

    drop(sqlite);
    remove_database(&db_path);
}

#[tokio::test]
async fn test_typed_history_uses_stable_numeric_order_in_memory() {
    let memory = MemoryTaskStore::new(16);
    for task_id in [id(2), id(10), id(u64::MAX)] {
        memory
            .accept_encoded(task_id, request("order", "same"))
            .await
            .expect("task is accepted");
    }
    assert_eq!(ids_in_order(&memory).await, vec![2, 10, u64::MAX]);
}

#[tokio::test]
async fn test_typed_history_pages_a_queue_larger_than_the_maximum_page() {
    let memory = MemoryTaskStore::new(300);
    for value in 1..=257 {
        memory
            .accept_encoded(id(value), request("page", "257"))
            .await
            .expect("task is accepted");
    }
    let first = memory
        .list_encoded(TaskQuery {
            limit: 256,
            ..TaskQuery::default()
        })
        .await
        .expect("first history page succeeds");
    assert_eq!(first.records.len(), 256);
    let second = memory
        .list_encoded(TaskQuery {
            limit: 256,
            after: first.next,
            ..TaskQuery::default()
        })
        .await
        .expect("second history page succeeds");
    assert_eq!(second.records.len(), 1);
    assert!(second.next.is_none());
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_typed_history_has_the_same_order_in_memory_and_sqlite() {
    let db_path = env::temp_dir().join(format!("qubit-task-typed-order-{}.sqlite", Uuid::new_v4()));
    let memory = MemoryTaskStore::new(16);
    let sqlite = SqliteTaskStore::open(&db_path).expect("typed SQLite store opens");
    let ids = [id(2), id(10), id(u64::MAX)];
    for store in [&memory as &dyn TaskStore, &sqlite] {
        for task_id in ids {
            store
                .accept_encoded(task_id, request("order", "same"))
                .await
                .expect("task is accepted");
        }
    }
    let memory_order = ids_in_order(&memory).await;
    let sqlite_order = ids_in_order(&sqlite).await;
    assert_eq!(memory_order, sqlite_order);
    assert_eq!(memory_order, vec![2, 10, u64::MAX]);

    drop(sqlite);
    remove_database(&db_path);
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_typed_schema_four_migrates_retry_deadline_column_without_losing_rows() {
    let db_path = env::temp_dir().join(format!("qubit-task-typed-schema-migration-{}.sqlite", Uuid::new_v4()));
    let sqlite = SqliteTaskStore::open(&db_path).expect("schema 8 opens");
    let stored_id = id(801);
    sqlite
        .accept_encoded(stored_id, request("migration", "preserve"))
        .await
        .expect("task is accepted");
    drop(sqlite);

    let connection = Connection::open(&db_path).expect("SQLite database opens");
    connection
        .execute_batch(concat!(
            "DROP TABLE task_event_outbox; DROP TABLE task_event_outbox_quarantine; ",
            "DROP TABLE task_terminal_checkpoints; ",
            "DROP INDEX tasks_queued_retry_deadline; ",
            "ALTER TABLE tasks DROP COLUMN retry_not_before_ms; PRAGMA user_version=4;",
        ))
        .expect("schema 4 fixture is prepared");
    drop(connection);

    let migrated = SqliteTaskStore::open(&db_path).expect("typed schema 4 migrates to 8");
    let loaded = migrated
        .get_encoded_task(stored_id)
        .await
        .expect("migrated task query succeeds")
        .expect("migrated task is present");
    assert_eq!(loaded.summary.id, stored_id);
    assert_eq!(loaded.summary.category.as_deref(), Some("migration"));
    assert_eq!(loaded.request.payload.bytes, [1]);
    assert_eq!(loaded.summary.retry_not_before_ms, None);
    drop(migrated);
    remove_database(&db_path);
}
