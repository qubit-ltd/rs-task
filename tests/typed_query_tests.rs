// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use qubit_task::model::ResourceRequest;
use qubit_task::model::StoredPayload;
use qubit_task::model::StoredTaskRequest;
use qubit_task::model::TaskId;
use qubit_task::model::TaskQuery;
use qubit_task::model::TaskState;
use qubit_task::model::TaskStateKind;
use qubit_task::store::MemoryTaskStore;
#[cfg(feature = "sqlite")]
use qubit_task::store::SqliteTaskStore;
use qubit_task::store::TaskStore;

fn request(category: &str, correlation_key: &str) -> StoredTaskRequest {
    StoredTaskRequest {
        kind_id: "query.test".to_owned(),
        category: Some(category.to_owned()),
        payload: StoredPayload {
            type_id: qubit_model_metadata::metadata::ModelIdBuf::parse("qubit_task.tests.Payload")
                .expect("valid model ID"),
            schema_version: 1,
            codec_id: "qubit.bytes.json".to_owned(),
            bytes: vec![1],
        },
        metadata: qubit_metadata::Metadata::new(),
        resource_limit: ResourceRequest::default(),
        correlation_key: Some(correlation_key.to_owned()),
        idempotency_key: None,
    }
}

fn id(value: u64) -> TaskId {
    TaskId::from_id(qubit_id::Id::new(value))
}

#[cfg(feature = "sqlite")]
fn remove_database(path: &std::path::Path) {
    let mut owner_lock = path.as_os_str().to_owned();
    owner_lock.push(".owner.lock");
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(owner_lock);
    let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
    let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
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

#[tokio::test]
async fn typed_history_filters_category_and_uses_exclusive_numeric_cursor() {
    let memory = MemoryTaskStore::new(16);
    assert_category_filter_and_exclusive_numeric_cursor(&memory).await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn typed_history_filters_category_and_uses_exclusive_numeric_cursor_in_sqlite() {
    let db_path = std::env::temp_dir().join(format!("qubit-task-typed-query-{}.sqlite", uuid::Uuid::new_v4()));
    let memory = MemoryTaskStore::new(16);
    let sqlite = SqliteTaskStore::open_next(&db_path).expect("typed SQLite store opens");
    assert_category_filter_and_exclusive_numeric_cursor(&memory).await;
    assert_category_filter_and_exclusive_numeric_cursor(&sqlite).await;

    drop(sqlite);
    remove_database(&db_path);
}

#[tokio::test]
async fn typed_history_uses_stable_numeric_order_in_memory() {
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
async fn typed_history_pages_a_queue_larger_than_the_maximum_page() {
    let memory = MemoryTaskStore::new(300);
    for value in 1..=257 {
        memory.accept_encoded(id(value), request("page", "257")).await.unwrap();
    }
    let first = memory
        .list_encoded(TaskQuery {
            limit: 256,
            ..TaskQuery::default()
        })
        .await
        .unwrap();
    assert_eq!(first.records.len(), 256);
    let second = memory
        .list_encoded(TaskQuery {
            limit: 256,
            after: first.next,
            ..TaskQuery::default()
        })
        .await
        .unwrap();
    assert_eq!(second.records.len(), 1);
    assert!(second.next.is_none());
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn typed_history_has_the_same_order_in_memory_and_sqlite() {
    let db_path = std::env::temp_dir().join(format!("qubit-task-typed-order-{}.sqlite", uuid::Uuid::new_v4()));
    let memory = MemoryTaskStore::new(16);
    let sqlite = SqliteTaskStore::open_next(&db_path).expect("typed SQLite store opens");
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
async fn typed_schema_four_migrates_retry_deadline_column_without_losing_rows() {
    let db_path = std::env::temp_dir().join(format!(
        "qubit-task-typed-schema-migration-{}.sqlite",
        uuid::Uuid::new_v4()
    ));
    let sqlite = SqliteTaskStore::open_next(&db_path).expect("schema 5 opens");
    let stored_id = id(801);
    sqlite
        .accept_encoded(stored_id, request("migration", "preserve"))
        .await
        .expect("task is accepted");
    drop(sqlite);

    let connection = rusqlite::Connection::open(&db_path).unwrap();
    connection
        .execute_batch("ALTER TABLE tasks DROP COLUMN retry_not_before_ms; PRAGMA user_version=4;")
        .unwrap();
    drop(connection);

    let migrated = SqliteTaskStore::open_next(&db_path).expect("typed schema 4 migrates to 5");
    let loaded = migrated.get_encoded_task(stored_id).await.unwrap().unwrap();
    assert_eq!(loaded.summary.id, stored_id);
    assert_eq!(loaded.summary.category.as_deref(), Some("migration"));
    assert_eq!(loaded.request.payload.bytes, [1]);
    assert_eq!(loaded.summary.retry_not_before_ms, None);
    drop(migrated);
    remove_database(&db_path);
}
