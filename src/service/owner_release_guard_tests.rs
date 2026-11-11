// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::sync::Notify;
use tokio::sync::Semaphore;

use crate::model::OwnerEpoch;
use crate::model::StoreCapabilities;
use crate::model::TaskStateCheckpoint;
use crate::model::typed::AcceptOutcome;
use crate::model::typed::ProgressCommand;
use crate::model::typed::StartCommand;
use crate::model::typed::StoredTask;
use crate::model::typed::StoredTaskRequest;
use crate::model::typed::TaskCursor;
use crate::model::typed::TaskId;
use crate::model::typed::TaskPage;
use crate::model::typed::TaskQuery;
use crate::model::typed::TaskSummary;
use crate::model::typed::TransitionCommand;
use crate::service::owner_release_guard::OwnerReleaseGuard;
use crate::service::owner_release_guard::OwnerReleaseWorker;
use crate::store::MemoryTaskStore;
use crate::store::StoreError;
use crate::store::TaskFuture;
use crate::store::TaskStore;

struct GatedReleaseStore {
    inner: MemoryTaskStore,
    entered: Notify,
    release_finished: Notify,
    resume: Semaphore,
    first_release: AtomicBool,
    failures_remaining: AtomicUsize,
    panic_next_release: AtomicBool,
    panic_on_construction: AtomicBool,
    unsupported_next_release: AtomicBool,
    gate_retry: AtomicBool,
    retry_entered: Notify,
    retry_resume: Semaphore,
    failure_observed: Notify,
    panic_observed: Notify,
    terminal_observed: Notify,
    attempts: Mutex<Vec<OwnerEpoch>>,
    attempt_observed: Notify,
}

impl GatedReleaseStore {
    /// Creates a real memory-backed owner with one blocked release attempt.
    fn new() -> Self {
        Self {
            inner: MemoryTaskStore::new(32),
            entered: Notify::new(),
            release_finished: Notify::new(),
            resume: Semaphore::new(0),
            first_release: AtomicBool::new(true),
            failures_remaining: AtomicUsize::new(0),
            panic_next_release: AtomicBool::new(false),
            panic_on_construction: AtomicBool::new(false),
            unsupported_next_release: AtomicBool::new(false),
            gate_retry: AtomicBool::new(false),
            retry_entered: Notify::new(),
            retry_resume: Semaphore::new(0),
            failure_observed: Notify::new(),
            panic_observed: Notify::new(),
            terminal_observed: Notify::new(),
            attempts: Mutex::new(Vec::new()),
            attempt_observed: Notify::new(),
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
    fn get_state_checkpoint<'a>(
        &'a self,
        id: TaskId,
    ) -> TaskFuture<'a, Result<Option<TaskStateCheckpoint>, StoreError>> {
        self.inner.get_state_checkpoint(id)
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
        if self.panic_on_construction.swap(false, Ordering::AcqRel) {
            self.panic_observed.notify_one();
            panic!("injected cleanup future construction panic");
        }
        Box::pin(async move {
            self.attempts.lock().expect("attempts lock").push(epoch);
            self.attempt_observed.notify_one();
            if self.first_release.swap(false, Ordering::AcqRel) {
                self.entered.notify_one();
                let permit = self
                    .resume
                    .acquire()
                    .await
                    .map_err(|error| StoreError::Failure(error.to_string()))?;
                permit.forget();
            }
            if self
                .failures_remaining
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                self.failure_observed.notify_one();
                return Err(StoreError::Failure("transient release failure".into()));
            }
            if self.gate_retry.swap(false, Ordering::AcqRel) {
                self.retry_entered.notify_one();
                let permit = self
                    .retry_resume
                    .acquire()
                    .await
                    .map_err(|error| StoreError::Failure(error.to_string()))?;
                permit.forget();
            }
            if self.panic_next_release.swap(false, Ordering::AcqRel) {
                self.panic_observed.notify_one();
                panic!("injected cleanup panic");
            }
            if self.unsupported_next_release.swap(false, Ordering::AcqRel) {
                self.terminal_observed.notify_one();
                return Err(StoreError::UnsupportedCapability);
            }
            let result = self.inner.release_owner(epoch).await;
            self.release_finished.notify_one();
            result
        })
    }
}

/// A stale retry must stop at OwnerConflict without releasing a newer owner.
#[tokio::test]
async fn test_drop_cleanup_retry_keeps_epoch_and_preserves_new_owner() {
    let store = Arc::new(GatedReleaseStore::new());
    store.first_release.store(false, Ordering::Release);
    store.failures_remaining.store(1, Ordering::Release);
    store.gate_retry.store(true, Ordering::Release);
    let old_epoch = store.acquire_owner().await.expect("old owner acquired");
    let dyn_store: Arc<dyn TaskStore> = store.clone();
    drop(OwnerReleaseGuard::new(
        dyn_store,
        old_epoch,
        OwnerReleaseWorker::shared().expect("cleanup worker starts"),
    ));

    tokio::time::timeout(Duration::from_secs(2), store.failure_observed.notified())
        .await
        .expect("old epoch fails once before retry");
    tokio::time::timeout(Duration::from_secs(2), store.attempt_observed.notified())
        .await
        .expect("first cleanup attempt is recorded before its failure");
    let retry_entered = tokio::time::timeout(Duration::from_secs(2), store.retry_entered.notified())
        .await
        .is_ok();
    if !retry_entered {
        store.retry_resume.add_permits(1);
        assert!(retry_entered, "old epoch retry entered its gate");
    }
    store
        .inner
        .release_owner(old_epoch)
        .await
        .expect("simulate old owner release");
    let new_epoch = store.acquire_owner().await.expect("new owner acquired");
    store.retry_resume.add_permits(1);
    tokio::time::timeout(Duration::from_secs(2), store.attempt_observed.notified())
        .await
        .expect("old cleanup retries and observes the inactive epoch");
    tokio::time::timeout(Duration::from_secs(2), store.release_finished.notified())
        .await
        .expect("old epoch release finishes with a conflict");
    assert!(matches!(store.acquire_owner().await, Err(StoreError::OwnerConflict)));
    let attempts = store.attempts.lock().expect("attempts lock").clone();
    assert_eq!(attempts, vec![old_epoch, old_epoch]);
    store
        .inner
        .release_owner(new_epoch)
        .await
        .expect("new owner remains releasable");
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
    tokio::time::timeout(Duration::from_secs(2), store.release_finished.notified())
        .await
        .expect("Drop cleanup finishes after release future cancellation");
    let next_epoch = store.acquire_owner().await.expect("second owner acquired");
    store.release_owner(next_epoch).await.expect("second owner released");
}

/// A failed release leaves the same epoch available for a later retry.
#[tokio::test]
async fn test_failed_release_stays_armed_for_retry() {
    let store = Arc::new(GatedReleaseStore::new());
    store.first_release.store(false, Ordering::Release);
    store.failures_remaining.store(1, Ordering::Release);
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

/// A cleanup retry delay must not prevent an unrelated owner from draining.
#[tokio::test]
async fn test_failure_backoff_does_not_block_other_cleanup() {
    let worker = OwnerReleaseWorker::shared().expect("cleanup worker starts");
    let blocked = Arc::new(GatedReleaseStore::new());
    blocked.first_release.store(false, Ordering::Release);
    blocked.failures_remaining.store(1, Ordering::Release);
    blocked.gate_retry.store(true, Ordering::Release);
    let blocked_epoch = blocked.acquire_owner().await.expect("blocked owner acquired");
    let blocked_store: Arc<dyn TaskStore> = blocked.clone();
    let blocked_guard = OwnerReleaseGuard::new(blocked_store, blocked_epoch, worker.clone());
    drop(blocked_guard);
    tokio::time::timeout(Duration::from_secs(2), blocked.failure_observed.notified())
        .await
        .expect("failed release enters its retry delay");
    let retry_entered = tokio::time::timeout(Duration::from_secs(2), blocked.retry_entered.notified())
        .await
        .is_ok();

    let other = Arc::new(GatedReleaseStore::new());
    other.first_release.store(false, Ordering::Release);
    let other_epoch = other.acquire_owner().await.expect("other owner acquired");
    let other_store: Arc<dyn TaskStore> = other.clone();
    drop(OwnerReleaseGuard::new(other_store, other_epoch, worker));
    let other_released = tokio::time::timeout(Duration::from_secs(2), other.release_finished.notified())
        .await
        .is_ok();
    blocked.retry_resume.add_permits(1);
    assert!(retry_entered, "failed cleanup entered its separately gated retry");
    assert!(
        other_released,
        "a gated retry held the worker behind an unrelated owner"
    );
    let next_epoch = other.acquire_owner().await.expect("other owner released");
    other
        .release_owner(next_epoch)
        .await
        .expect("replacement owner released");
    tokio::time::timeout(Duration::from_secs(2), blocked.release_finished.notified())
        .await
        .expect("failed job eventually retries and releases its owner");
}

/// Drop cleanup retries repeated store failures until it releases ownership.
#[tokio::test]
async fn test_drop_cleanup_retries_store_failure_until_owner_is_released() {
    let store = Arc::new(GatedReleaseStore::new());
    store.first_release.store(false, Ordering::Release);
    store.failures_remaining.store(2, Ordering::Release);
    let epoch = store.acquire_owner().await.expect("owner acquired");
    let dyn_store: Arc<dyn TaskStore> = store.clone();
    drop(OwnerReleaseGuard::new(
        dyn_store,
        epoch,
        OwnerReleaseWorker::shared().expect("cleanup worker starts"),
    ));

    for attempt in 1..=2 {
        tokio::time::timeout(Duration::from_secs(2), store.failure_observed.notified())
            .await
            .expect("cleanup worker attempts the injected failure");
        assert!(matches!(store.acquire_owner().await, Err(StoreError::OwnerConflict)));
        assert_eq!(store.failures_remaining.load(Ordering::Acquire), 2 - attempt);
    }
    tokio::time::timeout(Duration::from_secs(2), store.release_finished.notified())
        .await
        .expect("cleanup worker retries and releases the owner");
    assert_eq!(
        store.attempts.lock().expect("attempts lock").as_slice(),
        &[epoch, epoch, epoch],
        "successful cleanup stops after its third attempt"
    );
    let next_epoch = store
        .acquire_owner()
        .await
        .expect("owner can be acquired after cleanup recovery");
    store
        .release_owner(next_epoch)
        .await
        .expect("replacement owner released");

    let other = Arc::new(GatedReleaseStore::new());
    other.first_release.store(false, Ordering::Release);
    let other_epoch = other.acquire_owner().await.expect("worker remains available");
    let other_store: Arc<dyn TaskStore> = other.clone();
    drop(OwnerReleaseGuard::new(
        other_store,
        other_epoch,
        OwnerReleaseWorker::shared().expect("shared cleanup worker remains available"),
    ));
    tokio::time::timeout(Duration::from_secs(2), other.release_finished.notified())
        .await
        .expect("failed cleanup did not stop the worker");
    let next_epoch = other.acquire_owner().await.expect("replacement owner acquired");
    other
        .release_owner(next_epoch)
        .await
        .expect("replacement owner released");
}

/// A panicking drop cleanup is contained and does not terminate the worker.
#[tokio::test]
async fn test_drop_cleanup_contains_store_panic() {
    let store = Arc::new(GatedReleaseStore::new());
    store.first_release.store(false, Ordering::Release);
    store.panic_next_release.store(true, Ordering::Release);
    let epoch = store.acquire_owner().await.expect("owner acquired");
    let dyn_store: Arc<dyn TaskStore> = store.clone();
    drop(OwnerReleaseGuard::new(
        dyn_store,
        epoch,
        OwnerReleaseWorker::shared().expect("cleanup worker starts"),
    ));

    tokio::time::timeout(Duration::from_secs(2), async {
        store.panic_observed.notified().await;
    })
    .await
    .expect("cleanup worker reaches the injected panic");
    assert_eq!(store.attempts.lock().expect("attempts lock").as_slice(), &[epoch]);
    assert!(matches!(store.acquire_owner().await, Err(StoreError::OwnerConflict)));
    store
        .inner
        .release_owner(epoch)
        .await
        .expect("test releases the owner left after the injected panic");

    let other = Arc::new(GatedReleaseStore::new());
    other.first_release.store(false, Ordering::Release);
    let other_epoch = other.acquire_owner().await.expect("worker remains available");
    let other_store: Arc<dyn TaskStore> = other.clone();
    drop(OwnerReleaseGuard::new(
        other_store,
        other_epoch,
        OwnerReleaseWorker::shared().expect("shared cleanup worker remains available"),
    ));
    tokio::time::timeout(Duration::from_secs(2), other.release_finished.notified())
        .await
        .expect("panicking cleanup did not stop the worker");
    let next_epoch = other.acquire_owner().await.expect("replacement owner acquired");
    other
        .release_owner(next_epoch)
        .await
        .expect("replacement owner released");
}

/// Errors outside `Failure` are terminal and are attempted only once.
#[tokio::test]
async fn test_drop_cleanup_stops_after_unsupported_capability() {
    let store = Arc::new(GatedReleaseStore::new());
    store.first_release.store(false, Ordering::Release);
    store.unsupported_next_release.store(true, Ordering::Release);
    let epoch = store.acquire_owner().await.expect("owner acquired");
    let dyn_store: Arc<dyn TaskStore> = store.clone();
    drop(OwnerReleaseGuard::new(
        dyn_store,
        epoch,
        OwnerReleaseWorker::shared().expect("cleanup worker starts"),
    ));
    tokio::time::timeout(Duration::from_secs(2), store.terminal_observed.notified())
        .await
        .expect("terminal error is observed");
    assert_eq!(store.attempts.lock().expect("attempts lock").as_slice(), &[epoch]);
    assert!(matches!(store.acquire_owner().await, Err(StoreError::OwnerConflict)));
    store
        .inner
        .release_owner(epoch)
        .await
        .expect("test releases retained owner");
}

/// A panic while constructing the release future is also contained once.
#[tokio::test]
async fn test_drop_cleanup_contains_store_future_construction_panic() {
    let store = Arc::new(GatedReleaseStore::new());
    store.first_release.store(false, Ordering::Release);
    store.panic_on_construction.store(true, Ordering::Release);
    let epoch = store.acquire_owner().await.expect("owner acquired");
    let dyn_store: Arc<dyn TaskStore> = store.clone();
    drop(OwnerReleaseGuard::new(
        dyn_store,
        epoch,
        OwnerReleaseWorker::shared().expect("cleanup worker starts"),
    ));
    tokio::time::timeout(Duration::from_secs(2), store.panic_observed.notified())
        .await
        .expect("cleanup worker catches panic while constructing release future");
    assert!(store.attempts.lock().expect("attempts lock").is_empty());
    assert!(matches!(store.acquire_owner().await, Err(StoreError::OwnerConflict)));
    store
        .inner
        .release_owner(epoch)
        .await
        .expect("test releases retained owner");
}
