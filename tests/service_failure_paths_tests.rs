// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::handler::TaskRunOutcome;
use qubit_task::model::AcceptOutcome;
use qubit_task::model::OwnerEpoch;
use qubit_task::model::StoreCapabilities;
use qubit_task::model::StoredTaskPage;
use qubit_task::model::TaskId;
use qubit_task::model::TaskOutput;
use qubit_task::model::TaskPage;
use qubit_task::model::TaskQuery;
use qubit_task::model::TaskRecord;
use qubit_task::model::TaskRequest;
use qubit_task::model::TaskStateCounts;
use qubit_task::model::TransitionCommand;
use qubit_task::service::TaskServiceError;
use qubit_task::store::MemoryTaskStore;
use qubit_task::store::StoreError;
use qubit_task::store::TaskFuture;
use qubit_task::store::TaskStore;

struct FailFirstGetStore {
    inner: MemoryTaskStore,
    should_fail_get: AtomicBool,
}

impl FailFirstGetStore {
    fn new() -> Self {
        Self {
            inner: MemoryTaskStore::new(16),
            should_fail_get: AtomicBool::new(true),
        }
    }
}

impl TaskStore for FailFirstGetStore {
    fn capabilities(&self) -> StoreCapabilities {
        self.inner.capabilities()
    }

    fn accept<'a>(&'a self, id: TaskId, request: TaskRequest) -> TaskFuture<'a, Result<AcceptOutcome, StoreError>> {
        self.inner.accept(id, request)
    }

    fn find_idempotent<'a>(&'a self, request: TaskRequest) -> TaskFuture<'a, Result<Option<TaskRecord>, StoreError>> {
        self.inner.find_idempotent(request)
    }

    fn transition<'a>(&'a self, command: TransitionCommand) -> TaskFuture<'a, Result<TaskRecord, StoreError>> {
        self.inner.transition(command)
    }

    fn get<'a>(&'a self, id: TaskId) -> TaskFuture<'a, Result<Option<TaskRecord>, StoreError>> {
        if self.should_fail_get.swap(false, Ordering::AcqRel) {
            Box::pin(async { Err(StoreError::Failure("injected get failure".into())) })
        } else {
            self.inner.get(id)
        }
    }

    fn list<'a>(&'a self, query: TaskQuery) -> TaskFuture<'a, Result<TaskPage, StoreError>> {
        self.inner.list(query)
    }

    fn count_states<'a>(&'a self) -> TaskFuture<'a, Result<TaskStateCounts, StoreError>> {
        self.inner.count_states()
    }

    fn acquire_owner<'a>(&'a self) -> TaskFuture<'a, Result<OwnerEpoch, StoreError>> {
        self.inner.acquire_owner()
    }

    fn scan_unfinished<'a>(&'a self, cursor: Option<TaskId>) -> TaskFuture<'a, Result<StoredTaskPage, StoreError>> {
        self.inner.scan_unfinished(cursor)
    }

    fn release_owner<'a>(&'a self, epoch: OwnerEpoch) -> TaskFuture<'a, Result<(), StoreError>> {
        self.inner.release_owner(epoch)
    }
}

#[tokio::test]
async fn test_scheduler_store_failure_pauses_service_and_prevents_execution() {
    let service = TaskExecutionServiceBuilder::default()
        .store(Arc::new(FailFirstGetStore::new()))
        .build()
        .await
        .expect("service builds");
    let ran = Arc::new(AtomicBool::new(false));
    let handler_ran = Arc::clone(&ran);
    service
        .submit_local(move |_| {
            handler_ran.store(true, Ordering::Release);
            Ok(TaskRunOutcome::Succeeded(TaskOutput::default()))
        })
        .await
        .expect("task is accepted before scheduler reads it");

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if service.last_store_error().is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("scheduler records the storage failure");

    let error = service
        .submit_local(|_| Ok(TaskRunOutcome::Succeeded(TaskOutput::default())))
        .await
        .expect_err("service rejects submissions after a store failure");
    assert!(matches!(error, TaskServiceError::StoreUnavailable(_)));
    assert!(!ran.load(Ordering::Acquire), "failed task handler must not run");
    assert!(matches!(
        service.shutdown().await,
        Err(TaskServiceError::StoreUnavailable(_))
    ));
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_recoverable_sqlite_store_rejects_local_closure_without_accepting_it() {
    let path = std::env::temp_dir().join(format!("qubit-task-local-submit-{}.sqlite", TaskId::generate()));
    let service = TaskExecutionServiceBuilder::recoverable_sqlite(&path)
        .expect("SQLite service builds")
        .build()
        .await
        .expect("service builds");

    let error = service
        .submit_local(|_| Ok(TaskRunOutcome::Succeeded(TaskOutput::default())))
        .await
        .expect_err("recoverable stores cannot retain process-local closures");
    assert!(matches!(error, TaskServiceError::UnsupportedCapability));
    let page = service
        .list(TaskQuery {
            limit: 10,
            ..TaskQuery::default()
        })
        .await
        .expect("history query succeeds");
    assert!(page.records.is_empty(), "rejected closure must not be stored");
    service.shutdown().await.expect("empty service shuts down");

    for suffix in ["", "-wal", "-shm", ".owner.lock"] {
        let file = if suffix == ".owner.lock" {
            path.with_extension("owner.lock")
        } else {
            std::path::PathBuf::from(format!("{}{suffix}", path.display()))
        };
        let _ = std::fs::remove_file(file);
    }
}
