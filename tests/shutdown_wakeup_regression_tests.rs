// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::time::Duration;

use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::model::AcceptOutcome;
use qubit_task::model::OwnerEpoch;
use qubit_task::model::RecoveryPage;
use qubit_task::model::StoreCapabilities;
use qubit_task::model::TaskId;
use qubit_task::model::TaskOutput;
use qubit_task::model::TaskPage;
use qubit_task::model::TaskQuery;
use qubit_task::model::TaskRecord;
use qubit_task::model::TaskRequest;
use qubit_task::model::TaskState;
use qubit_task::model::TaskStateCounts;
use qubit_task::model::TaskSummary;
use qubit_task::model::TransitionCommand;
use qubit_task::service::LocalTaskOutcome;
use qubit_task::store::MemoryTaskStore;
use qubit_task::store::StoreError;
use qubit_task::store::TaskFuture;
use qubit_task::store::TaskStore;
use tokio::spawn;
use tokio::sync::Semaphore;
use tokio::task::yield_now;
use tokio::test as tokio_test;
use tokio::time::Instant;
use tokio::time::timeout;

struct DelayedCountsStore {
    inner: Arc<dyn TaskStore>,
    armed: AtomicBool,
    snapshots: AtomicUsize,
    observed: Semaphore,
    resume: Semaphore,
}
impl TaskStore for DelayedCountsStore {
    fn capabilities(&self) -> StoreCapabilities {
        self.inner.capabilities()
    }

    fn accept<'a>(&'a self, id: TaskId, request: TaskRequest) -> TaskFuture<'a, Result<AcceptOutcome, StoreError>> {
        self.inner.accept(id, request)
    }

    fn get_by_idempotency_key<'a>(&'a self, key: &'a str) -> TaskFuture<'a, Result<Option<TaskRecord>, StoreError>> {
        self.inner.get_by_idempotency_key(key)
    }

    fn get_summary_by_idempotency_key<'a>(
        &'a self,
        key: &'a str,
    ) -> TaskFuture<'a, Result<Option<TaskSummary>, StoreError>> {
        self.inner.get_summary_by_idempotency_key(key)
    }

    fn transition<'a>(&'a self, command: TransitionCommand) -> TaskFuture<'a, Result<TaskSummary, StoreError>> {
        self.inner.transition(command)
    }

    fn get_summary<'a>(&'a self, id: TaskId) -> TaskFuture<'a, Result<Option<TaskSummary>, StoreError>> {
        self.inner.get_summary(id)
    }

    fn get<'a>(&'a self, id: TaskId) -> TaskFuture<'a, Result<Option<TaskRecord>, StoreError>> {
        self.inner.get(id)
    }

    fn list<'a>(&'a self, _query: TaskQuery) -> TaskFuture<'a, Result<TaskPage, StoreError>> {
        self.inner.list(_query)
    }

    fn count_states<'a>(&'a self) -> TaskFuture<'a, Result<TaskStateCounts, StoreError>> {
        Box::pin(async move {
            let snapshot = self.inner.count_states().await?;
            if snapshot.running > 0 && self.armed.load(Ordering::SeqCst) {
                let ticket = self.snapshots.fetch_add(1, Ordering::SeqCst);
                println!("shutdown count snapshot {ticket}: {snapshot:?}");
                self.observed.add_permits(1);
                self.resume.acquire().await.unwrap().forget();
            }
            Ok(snapshot)
        })
    }

    fn prune_terminal_before<'a>(
        &'a self,
        _accepted_before_ms: u64,
        _max_rows: NonZeroUsize,
    ) -> TaskFuture<'a, Result<usize, StoreError>> {
        self.inner.prune_terminal_before(_accepted_before_ms, _max_rows)
    }

    fn acquire_owner<'a>(&'a self) -> TaskFuture<'a, Result<OwnerEpoch, StoreError>> {
        self.inner.acquire_owner()
    }

    fn has_unfinished_over_limit<'a>(&'a self, limit: usize) -> TaskFuture<'a, Result<bool, StoreError>> {
        self.inner.has_unfinished_over_limit(limit)
    }

    fn scan_unfinished<'a>(&'a self, cursor: Option<TaskId>) -> TaskFuture<'a, Result<RecoveryPage, StoreError>> {
        self.inner.scan_unfinished(cursor)
    }

    fn release_owner<'a>(&'a self, epoch: OwnerEpoch) -> TaskFuture<'a, Result<(), StoreError>> {
        self.inner.release_owner(epoch)
    }
}

#[tokio_test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_drains_when_terminal_notification_precedes_count_return() {
    let (send, recv) = mpsc::channel();
    let store = Arc::new(DelayedCountsStore {
        inner: Arc::new(MemoryTaskStore::new(8)),
        armed: AtomicBool::new(false),
        snapshots: AtomicUsize::new(0),
        observed: Semaphore::new(0),
        resume: Semaphore::new(0),
    });
    let service = TaskExecutionServiceBuilder::in_memory()
        .store(store.clone())
        .build()
        .await
        .unwrap();
    let handle = service
        .submit_local(move |_| {
            recv.recv().unwrap();
            LocalTaskOutcome::<u32, String>::Succeeded {
                value: 42,
                summary: TaskOutput::default(),
            }
        })
        .await
        .unwrap();
    let id = handle.task_id();
    timeout(Duration::from_secs(5), async {
        loop {
            if matches!(
                service.get_summary(id).await.unwrap().unwrap().state,
                TaskState::Running
            ) {
                break;
            }
            yield_now().await;
        }
    })
    .await
    .expect("handler did not reach Running");
    store.armed.store(true, Ordering::SeqCst);
    let closing = service.clone();
    let shutdown = spawn(async move { closing.shutdown_until(Instant::now() + Duration::from_secs(5)).await });
    timeout(Duration::from_secs(5), async {
        store.observed.acquire_many(2).await.unwrap().forget();
    })
    .await
    .expect("scheduler and shutdown did not both acquire running snapshots");
    send.send(()).unwrap();
    let value = timeout(Duration::from_secs(5), handle.result())
        .await
        .expect("task did not finalize")
        .unwrap()
        .unwrap();
    assert_eq!(value, 42);
    // finish_attempt notifies changed before finalizing this public local handle.
    // Returning stale-but-consistent snapshots now deterministically exercises
    // the notification registration window, without timing sleeps.
    store.resume.add_permits(2);
    assert!(shutdown.await.unwrap().is_ok(), "shutdown lost the final notification");
    let counts = store.inner.count_states().await.unwrap();
    assert_eq!(counts.running, 0);
    assert_eq!(counts.queued, 0);
    service
        .shutdown_until(Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
}
