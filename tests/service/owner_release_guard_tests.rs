// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::sync::Notify;
use tokio::sync::Semaphore;

use crate::model::OwnerEpoch;
use crate::model::StoreCapabilities;
use crate::model::next::AcceptOutcome;
use crate::model::next::ProgressCommand;
use crate::model::next::StartCommand;
use crate::model::next::StoredTask;
use crate::model::next::StoredTaskRequest;
use crate::model::next::TaskCursor;
use crate::model::next::TaskId;
use crate::model::next::TaskPage;
use crate::model::next::TaskQuery;
use crate::model::next::TaskSummary;
use crate::model::next::TransitionCommand;
use crate::service::owner_release_guard::OwnerReleaseGuard;
use crate::service::owner_release_guard::OwnerReleaseWorker;
use crate::store::MemoryTaskStore;
use crate::store::StoreError;
use crate::store::TaskFuture;
use crate::store::TaskStore;

struct GatedReleaseStore {
    inner: MemoryTaskStore,
    entered: Notify,
    resume: Semaphore,
    first_release: AtomicBool,
    fail_next_release: AtomicBool,
}

impl GatedReleaseStore {
    /// Creates a real memory-backed owner with one blocked release attempt.
    fn new() -> Self {
        Self {
            inner: MemoryTaskStore::new(32),
            entered: Notify::new(),
            resume: Semaphore::new(0),
            first_release: AtomicBool::new(true),
            fail_next_release: AtomicBool::new(false),
        }
    }
}

impl TaskStore for GatedReleaseStore {
    fn capabilities(&self) -> StoreCapabilities {
        self.inner.capabilities()
    }
    fn accept_encoded<'a>(
        &'a self,
        id: TaskId,
        request: StoredTaskRequest,
    ) -> TaskFuture<'a, Result<AcceptOutcome, StoreError>> {
        self.inner.accept_encoded(id, request)
    }
    fn get_encoded_task<'a>(&'a self, id: TaskId) -> TaskFuture<'a, Result<Option<StoredTask>, StoreError>> {
        self.inner.get_encoded_task(id)
    }
    fn start_encoded<'a>(&'a self, command: StartCommand) -> TaskFuture<'a, Result<TaskSummary, StoreError>> {
        self.inner.start_encoded(command)
    }
    fn transition_encoded<'a>(&'a self, command: TransitionCommand) -> TaskFuture<'a, Result<TaskSummary, StoreError>> {
        self.inner.transition_encoded(command)
    }
    fn update_progress<'a>(&'a self, command: ProgressCommand) -> TaskFuture<'a, Result<TaskSummary, StoreError>> {
        self.inner.update_progress(command)
    }
    fn list_encoded<'a>(&'a self, query: TaskQuery) -> TaskFuture<'a, Result<TaskPage, StoreError>> {
        self.inner.list_encoded(query)
    }
    fn list_ready_queued<'a>(
        &'a self,
        after: Option<TaskCursor>,
        limit: NonZeroUsize,
        now_ms: u64,
    ) -> TaskFuture<'a, Result<TaskPage, StoreError>> {
        self.inner.list_ready_queued(after, limit, now_ms)
    }
    fn next_retry_deadline<'a>(&'a self, now_ms: u64) -> TaskFuture<'a, Result<Option<u64>, StoreError>> {
        self.inner.next_retry_deadline(now_ms)
    }
    fn prune_terminal_before<'a>(
        &'a self,
        finished_before_ms: u64,
        max_rows: NonZeroUsize,
    ) -> TaskFuture<'a, Result<usize, StoreError>> {
        self.inner.prune_terminal_before(finished_before_ms, max_rows)
    }
    fn acquire_owner<'a>(&'a self) -> TaskFuture<'a, Result<OwnerEpoch, StoreError>> {
        self.inner.acquire_owner()
    }
    fn release_owner<'a>(&'a self, epoch: OwnerEpoch) -> TaskFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            if self.first_release.swap(false, Ordering::AcqRel) {
                self.entered.notify_one();
                let permit = self
                    .resume
                    .acquire()
                    .await
                    .map_err(|error| StoreError::Failure(error.to_string()))?;
                permit.forget();
            }
            if self.fail_next_release.swap(false, Ordering::AcqRel) {
                return Err(StoreError::Failure("transient release failure".into()));
            }
            self.inner.release_owner(epoch).await
        })
    }
}

/// Cancelling `release()` leaves the guard armed for Drop cleanup.
#[tokio::test]
async fn test_cancelled_release_future_still_releases_owner() {
    let store = Arc::new(GatedReleaseStore::new());
    let epoch = store.acquire_owner().await.expect("first owner acquired");
    let dyn_store: Arc<dyn TaskStore> = store.clone();
    let mut guard = OwnerReleaseGuard::new(
        dyn_store,
        epoch,
        OwnerReleaseWorker::shared().expect("cleanup worker starts"),
    );
    let entered = store.entered.notified();
    tokio::pin!(entered);
    entered.as_mut().enable();
    {
        let release = guard.release();
        tokio::pin!(release);
        tokio::select! {
            result = &mut release => panic!("release unexpectedly completed: {result:?}"),
            () = &mut entered => {},
        }
    }
    drop(guard);
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match store.acquire_owner().await {
                Ok(next_epoch) => {
                    store.release_owner(next_epoch).await.expect("second owner released");
                    break;
                }
                Err(StoreError::OwnerConflict) => tokio::task::yield_now().await,
                Err(error) => panic!("unexpected owner acquisition error: {error}"),
            }
        }
    })
    .await
    .expect("Drop eventually releases owner after release future cancellation");
}

/// A failed release leaves the same epoch available for a later retry.
#[tokio::test]
async fn test_failed_release_stays_armed_for_retry() {
    let store = Arc::new(GatedReleaseStore::new());
    store.first_release.store(false, Ordering::Release);
    store.fail_next_release.store(true, Ordering::Release);
    let epoch = store.acquire_owner().await.expect("first owner acquired");
    let dyn_store: Arc<dyn TaskStore> = store.clone();
    let mut guard = OwnerReleaseGuard::new(
        dyn_store,
        epoch,
        OwnerReleaseWorker::shared().expect("cleanup worker starts"),
    );

    assert!(matches!(
        guard.release().await,
        Err(StoreError::Failure(message)) if message == "transient release failure"
    ));
    assert!(matches!(store.acquire_owner().await, Err(StoreError::OwnerConflict)));
    guard.release().await.expect("retry releases the same owner epoch");
    let next_epoch = store.acquire_owner().await.expect("owner can be reacquired");
    store.release_owner(next_epoch).await.expect("second owner released");
}

/// Releasing an already disarmed guard succeeds without releasing a newer
/// owner.
#[tokio::test]
async fn test_successful_release_disarms_guard_for_later_calls_and_drop() {
    let store = Arc::new(GatedReleaseStore::new());
    store.first_release.store(false, Ordering::Release);
    let epoch = store.acquire_owner().await.expect("first owner acquired");
    let dyn_store: Arc<dyn TaskStore> = store.clone();
    let mut guard = OwnerReleaseGuard::new(
        dyn_store,
        epoch,
        OwnerReleaseWorker::shared().expect("cleanup worker starts"),
    );

    guard.release().await.expect("first owner released");
    guard.release().await.expect("disarmed release is idempotent");
    let next_epoch = store.acquire_owner().await.expect("new owner acquired");
    drop(guard);

    assert!(matches!(store.acquire_owner().await, Err(StoreError::OwnerConflict)));
    store.release_owner(next_epoch).await.expect("new owner remains valid");
}

/// A blocked cleanup must not prevent an unrelated owner from draining.
#[tokio::test]
async fn test_pending_release_does_not_block_other_cleanup() {
    let worker = OwnerReleaseWorker::shared().expect("cleanup worker starts");
    let blocked = Arc::new(GatedReleaseStore::new());
    let blocked_epoch = blocked.acquire_owner().await.expect("blocked owner acquired");
    let blocked_store: Arc<dyn TaskStore> = blocked.clone();
    let blocked_guard = OwnerReleaseGuard::new(blocked_store, blocked_epoch, worker.clone());
    let entered = blocked.entered.notified();
    tokio::pin!(entered);
    entered.as_mut().enable();
    drop(blocked_guard);
    tokio::time::timeout(Duration::from_secs(2), entered)
        .await
        .expect("first release enters its gate");

    let other = Arc::new(MemoryTaskStore::new(16));
    let other_epoch = other.acquire_owner().await.expect("other owner acquired");
    let other_store: Arc<dyn TaskStore> = other.clone();
    drop(OwnerReleaseGuard::new(other_store, other_epoch, worker));
    let other_released = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match other.acquire_owner().await {
                Ok(epoch) => {
                    other.release_owner(epoch).await.expect("other owner released");
                    break;
                }
                Err(StoreError::OwnerConflict) => tokio::task::yield_now().await,
                Err(error) => panic!("unexpected owner acquisition error: {error}"),
            }
        }
    })
    .await
    .is_ok();
    blocked.resume.add_permits(1);
    assert!(
        other_released,
        "pending cleanup held the worker behind an unrelated owner"
    );
}
