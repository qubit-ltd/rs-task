// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use parking_lot::Mutex;
use qubit_task::model::AcceptOutcome;
use qubit_task::model::OwnerEpoch;
use qubit_task::model::RecoveryPage;
use qubit_task::model::StoreCapabilities;
use qubit_task::model::TaskId;
use qubit_task::model::TaskPage;
use qubit_task::model::TaskQuery;
use qubit_task::model::TaskRecord;
use qubit_task::model::TaskRequest;
use qubit_task::model::TaskStateCounts;
use qubit_task::model::TaskSummary;
use qubit_task::model::TransitionCommand;
use qubit_task::store::SqliteTaskStore;
use qubit_task::store::StoreError;
use qubit_task::store::TaskFuture;
use qubit_task::store::TaskStore;
use tokio::sync::Notify;
use tokio::sync::Semaphore;
use tokio::sync::oneshot;

/// Test provider that can hold one accepted write before it reaches SQLite.
pub struct DelayedWriteStore {
    /// SQLite provider used for the real storage contract.
    inner: SqliteTaskStore,
    /// Permit released by the test to finish the held acceptance.
    accept_gate: Arc<Semaphore>,
    /// Signals when the held write has entered the provider.
    accept_started: Mutex<Option<oneshot::Sender<()>>>,
    /// Number of writes not yet completed by the inner store.
    active_writes: AtomicUsize,
    /// Wakes ownership release when the final accepted write completes.
    writes_idle: Notify,
}

impl DelayedWriteStore {
    /// Creates a wrapper whose next acceptance waits for a test permit.
    pub fn new(inner: SqliteTaskStore) -> (Self, oneshot::Receiver<()>) {
        let (sender, receiver) = oneshot::channel();
        (
            Self {
                inner,
                accept_gate: Arc::new(Semaphore::new(0)),
                accept_started: Mutex::new(Some(sender)),
                active_writes: AtomicUsize::new(0),
                writes_idle: Notify::new(),
            },
            receiver,
        )
    }

    /// Allows the held acceptance to continue.
    pub fn release_accept(&self) {
        self.accept_gate.add_permits(1);
    }
}

impl TaskStore for DelayedWriteStore {
    fn capabilities(&self) -> StoreCapabilities {
        self.inner.capabilities()
    }

    fn accept<'a>(&'a self, id: TaskId, request: TaskRequest) -> TaskFuture<'a, Result<AcceptOutcome, StoreError>> {
        Box::pin(async move {
            self.active_writes.fetch_add(1, Ordering::AcqRel);
            let _active_write = ActiveWrite(self);
            if let Some(sender) = self.accept_started.lock().take() {
                let _ = sender.send(());
            }
            let _permit = self
                .accept_gate
                .acquire()
                .await
                .map_err(|_| StoreError::Failure("accept gate closed".into()))?;
            self.inner.accept(id, request).await
        })
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

    fn list<'a>(&'a self, query: TaskQuery) -> TaskFuture<'a, Result<TaskPage, StoreError>> {
        self.inner.list(query)
    }

    fn count_states<'a>(&'a self) -> TaskFuture<'a, Result<TaskStateCounts, StoreError>> {
        self.inner.count_states()
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
        Box::pin(async move {
            loop {
                let notified = self.writes_idle.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self.active_writes.load(Ordering::Acquire) == 0 {
                    break;
                }
                notified.await;
            }
            self.inner.release_owner(epoch).await
        })
    }
}

/// Decrements the controlled store's in-flight write count when acceptance
/// exits.
struct ActiveWrite<'a>(&'a DelayedWriteStore);

impl Drop for ActiveWrite<'_> {
    fn drop(&mut self) {
        if self.0.active_writes.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.writes_idle.notify_waiters();
        }
    }
}
