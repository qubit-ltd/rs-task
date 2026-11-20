// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;

use qubit_task::model::TaskQuery;
use qubit_task::model::typed::TaskId;
use qubit_task::store::MemoryStoreOptions;
use qubit_task::store::MemoryTaskStore;
use qubit_task::store::StoreError;
use qubit_task::store::TaskFuture;
use qubit_task::store::TaskStore;
use qubit_task::store::delegate_task_store;

#[test]
fn test_query_uses_private_builder_and_validates_limits() {
    let query = TaskQuery::default();
    assert_eq!(query.limit(), 50);
    assert!(TaskQuery::default().page_size(0).is_err());
    assert_eq!(TaskQuery::default().page_size(50).expect("valid").limit(), 50);
    assert_eq!(TaskQuery::default().page_size(256).expect("maximum").limit(), 256);
    assert!(TaskQuery::default().page_size(257).is_err());
}

#[test]
fn test_memory_options_validate_nonzero_limits() {
    let options = MemoryStoreOptions::new(0)
        .max_payload_bytes(1024)
        .expect("positive payload budget")
        .max_unfinished_records(8)
        .expect("positive unfinished limit");
    let _store = MemoryTaskStore::with_options(options);
    assert!(MemoryStoreOptions::new(1).max_payload_bytes(0).is_err());
    assert!(MemoryStoreOptions::new(1).max_unfinished_records(0).is_err());
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_sqlite_async_open_observes_owner_conflict() {
    let directory = std::env::temp_dir().join(format!("qubit-task-api-store-{}", std::process::id()));
    std::fs::create_dir(&directory).expect("create unique test directory");
    let path = directory.join("tasks.sqlite");
    let first = qubit_task::store::SqliteTaskStore::open_async(&path)
        .await
        .expect("open first");
    assert!(matches!(
        qubit_task::store::SqliteTaskStore::open_async(&path).await,
        Err(StoreError::OwnerConflict)
    ));
    drop(first);
    let _second = qubit_task::store::SqliteTaskStore::open_async(&path)
        .await
        .expect("reopen after drop");
    drop(_second);
    let _ = std::fs::remove_file(&path);
    let mut lock_path = path.as_os_str().to_os_string();
    lock_path.push(".owner.lock");
    let _ = std::fs::remove_file(std::path::PathBuf::from(lock_path));
    let _ = std::fs::remove_dir(directory);
}

struct Forwarder {
    inner: Arc<dyn TaskStore>,
}

struct PlainForwarder {
    inner: Arc<dyn TaskStore>,
}

impl TaskStore for PlainForwarder {
    delegate_task_store!(inner; except []);
}

struct CustomDefault {
    inner: Arc<dyn TaskStore>,
}

impl TaskStore for CustomDefault {
    delegate_task_store!(inner; except [enable_event_outbox, get_summary]);

    fn enable_event_outbox<'a>(&'a self) -> TaskFuture<'a, Result<(), StoreError>> {
        Box::pin(async { Ok(()) })
    }

    fn get_summary<'a>(
        &'a self,
        id: TaskId,
    ) -> TaskFuture<'a, Result<Option<qubit_task::model::typed::TaskSummary>, StoreError>> {
        self.inner.get_summary(id)
    }
}

impl TaskStore for Forwarder {
    delegate_task_store!(inner; except [accept_encoded]);

    fn accept_encoded<'a>(
        &'a self,
        id: TaskId,
        request: qubit_task::model::typed::StoredTaskRequest,
    ) -> TaskFuture<'a, Result<qubit_task::model::typed::AcceptOutcome, StoreError>> {
        self.inner.accept_encoded(id, request)
    }
}

#[test]
fn test_delegate_macro_is_available_to_external_crates() {
    let wrapped = Forwarder {
        inner: Arc::new(CustomDefault {
            inner: Arc::new(MemoryTaskStore::new(4)),
        }),
    };
    assert_eq!(wrapped.capabilities(), wrapped.inner.capabilities());
    assert!(futures::executor::block_on(wrapped.enable_event_outbox()).is_ok());
    let plain = PlainForwarder {
        inner: Arc::new(MemoryTaskStore::new(4)),
    };
    assert_eq!(plain.capabilities(), plain.inner.capabilities());
}
