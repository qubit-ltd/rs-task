// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
#![cfg(feature = "sqlite")]

use std::num::NonZeroUsize;
use std::sync::Arc;

use common::sqlite_paths;
use rusqlite::Connection;
use rusqlite::params;
use tokio::sync::Semaphore;
use tokio::sync::mpsc;
use tokio::test as tokio_test;
use tokio::time;

use super::common;
use crate::handler::TaskContext;
use crate::handler::TaskHandler;
use crate::handler::TaskHandlerDescriptor;
use crate::handler::TaskRunOutcome;
use crate::handler::TaskRunResult;
use crate::model::AcceptOutcome;
use crate::model::ResourceCapacity;
use crate::model::ResourceRequest;
use crate::model::TaskCursor;
use crate::model::TaskId;
use crate::model::TaskOutput;
use crate::model::TaskRequest;
use crate::scheduling::FairFifoPolicy;
use crate::service::TaskExecutionServiceBuilder;
use crate::store::LegacyTaskStore as TaskStore;
use crate::store::SqliteTaskStore;
use crate::store::TaskFuture;

/// Owns the disposable database and removes its files after connections close.
struct TestDatabase {
    path: std::path::PathBuf,
}

impl TestDatabase {
    /// Creates a unique path for a recovery ordering scenario.
    fn new() -> Self {
        Self {
            path: std::env::temp_dir().join(format!("qubit-task-recovery-order-{}.sqlite", TaskId::generate())),
        }
    }
}

impl Drop for TestDatabase {
    fn drop(&mut self) {
        for path in [
            self.path.clone(),
            sqlite_paths::owner_lock_path(&self.path),
            self.path.with_extension("sqlite-wal"),
            self.path.with_extension("sqlite-shm"),
        ] {
            let _ = std::fs::remove_file(path);
        }
    }
}

/// Constructs reproducible IDs independent of random acceptance UUIDs.
fn task_id(number: usize) -> TaskId {
    serde_json::from_str(&format!("\"00000000-0000-0000-0000-{number:012x}\"")).expect("fixed UUID decodes")
}

/// Persists real acceptances, then aligns only their acceptance timestamp.
///
/// The indexed column and its lifecycle copy must agree for the row codec;
/// all other lifecycle data and payload bytes remain unchanged.
async fn seed(database: &TestDatabase, keys: &[TaskCursor]) {
    let store = SqliteTaskStore::open(&database.path).expect("seed store opens");
    for key in keys {
        let mut request = TaskRequest::new("ordered", "1", Vec::new());
        request.resources = ResourceRequest {
            cpu_slots: 1,
            ..ResourceRequest::default()
        };
        assert!(matches!(
            store.accept(key.id, request).await.expect("acceptance commits"),
            AcceptOutcome::Accepted(_)
        ));
    }
    drop(store);
    let mut connection = Connection::open(&database.path).expect("seed database opens");
    let transaction = connection.transaction().expect("timestamp transaction starts");
    for key in keys {
        transaction
            .execute(
                "UPDATE tasks SET accepted_at=?1, lifecycle_json=json_set(lifecycle_json, '$.accepted_at_ms', ?1) WHERE id=?2",
                params![key.accepted_at_ms, key.id.to_string()],
            )
            .expect("acceptance timestamp is aligned");
    }
    transaction.commit().expect("timestamps commit");
}

/// Checks every page, next cursor, terminal page, and exclusion boundary.
async fn assert_pages(keys: Vec<TaskCursor>, expected_sizes: &[usize]) {
    let database = TestDatabase::new();
    seed(&database, &keys).await;
    let store = SqliteTaskStore::open(&database.path).expect("recovery store reopens");
    let mut expected = keys;
    expected.sort();
    let mut actual = Vec::new();
    let mut cursor = None;
    for (index, size) in expected_sizes.iter().enumerate() {
        let page = store.scan_unfinished(cursor).await.expect("recovery page reads");
        assert_eq!(page.tasks.len(), *size, "page {index} has the expected bounded size");
        actual.extend(page.tasks.iter().map(TaskCursor::from));
        if index + 1 < expected_sizes.len() {
            assert_eq!(page.next, page.tasks.last().map(TaskCursor::from));
        } else {
            assert_eq!(page.next, None, "an exact full terminal page has no next cursor");
        }
        cursor = page.next;
    }
    assert_eq!(
        actual, expected,
        "recovery uses acceptance time before UUID and breaks ties by UUID"
    );
    let tail = store
        .scan_unfinished(actual.last().copied())
        .await
        .expect("exclusive tail reads");
    assert!(tail.tasks.is_empty());
    assert!(tail.next.is_none());
}

#[tokio_test]
async fn test_recovery_acceptance_time_precedes_uuid() {
    assert_pages(
        vec![TaskCursor::new(1, task_id(2)), TaskCursor::new(2, task_id(1))],
        &[2],
    )
    .await;
}

#[tokio_test]
async fn test_recovery_equal_timestamps_break_ties_by_uuid() {
    assert_pages(
        vec![
            TaskCursor::new(42, task_id(3)),
            TaskCursor::new(42, task_id(1)),
            TaskCursor::new(42, task_id(2)),
        ],
        &[3],
    )
    .await;
}

#[tokio_test]
async fn test_recovery_513_rows_cross_three_pages_without_gaps() {
    assert_pages(
        (0..513)
            .map(|index| TaskCursor::new(index as u64 / 3, task_id(513 - index)))
            .collect(),
        &[256, 256, 1],
    )
    .await;
}

#[tokio_test]
async fn test_recovery_256_rows_finish_on_full_page() {
    assert_pages(
        (0..256)
            .map(|index| TaskCursor::new(42, task_id(256 - index)))
            .collect(),
        &[256],
    )
    .await;
}

#[tokio_test]
async fn test_recovery_512_rows_finish_on_second_full_page() {
    assert_pages(
        (0..512)
            .map(|index| TaskCursor::new(42, task_id(512 - index)))
            .collect(),
        &[256, 256],
    )
    .await;
}

struct OrderedHandler {
    started: mpsc::UnboundedSender<TaskId>,
    permits: Arc<Semaphore>,
}

impl TaskHandler for OrderedHandler {
    fn descriptor(&self) -> TaskHandlerDescriptor {
        TaskHandlerDescriptor {
            task_type: "ordered".into(),
            version: "1".into(),
        }
    }

    fn run<'a>(&'a self, _payload: &'a [u8], context: TaskContext) -> TaskFuture<'a, TaskRunResult> {
        Box::pin(async move {
            self.started
                .send(context.task_id())
                .expect("start observer remains open");
            self.permits
                .acquire()
                .await
                .expect("execution gate remains open")
                .forget();
            Ok(TaskRunOutcome::Succeeded(TaskOutput::default()))
        })
    }
}

#[tokio_test]
async fn test_builder_restarts_persisted_acceptances_in_cursor_order_with_one_slot() {
    let database = TestDatabase::new();
    let keys = (0..6)
        .map(|index| TaskCursor::new(index as u64 / 2, task_id(6 - index)))
        .collect::<Vec<_>>();
    seed(&database, &keys).await;
    let store = SqliteTaskStore::open(&database.path).expect("persisted store reopens");
    let mut expected = Vec::new();
    for key in &keys {
        expected.push(TaskCursor::from(
            &store
                .get_summary(key.id)
                .await
                .expect("summary reads")
                .expect("accepted task exists"),
        ));
    }
    expected.sort();
    drop(store);
    let (started, mut starts) = mpsc::unbounded_channel();
    let permits = Arc::new(Semaphore::new(0));
    let service = TaskExecutionServiceBuilder::recoverable_sqlite(&database.path)
        .expect("restart builder opens")
        .capacity(ResourceCapacity {
            cpu_slots: 1,
            ..ResourceCapacity::default()
        })
        .max_running_tasks(NonZeroUsize::MIN)
        .scan_budget(1)
        .policy(Arc::new(FairFifoPolicy::new(0)))
        .register_handler(Arc::new(OrderedHandler {
            started,
            permits: Arc::clone(&permits),
        }))
        .expect("handler registers")
        .build()
        .await
        .expect("service recovers");
    let mut actual = Vec::new();
    for _ in &expected {
        actual.push(
            time::timeout(std::time::Duration::from_secs(3), starts.recv())
                .await
                .expect("next recovered task starts")
                .expect("handler reports start"),
        );
        permits.add_permits(1);
    }
    for key in &expected {
        time::timeout(std::time::Duration::from_secs(3), service.wait(key.id))
            .await
            .expect("task finishes")
            .expect("terminal state persists");
    }
    service.shutdown().await.expect("service closes");
    assert_eq!(actual, expected.iter().map(|key| key.id).collect::<Vec<_>>());
}
