// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Releases store ownership when construction or shutdown is cancelled.

use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::mpsc;

use futures::FutureExt;

use crate::model::OwnerEpoch;
use crate::store::StoreError;
use crate::store::TaskStore;

/// Holds an acquired owner epoch until release has finished.
pub(super) struct OwnerReleaseGuard {
    owner: Option<(Arc<dyn TaskStore>, OwnerEpoch)>,
    cleanup: OwnerReleaseWorker,
}

struct ReleaseJob {
    store: Arc<dyn TaskStore>,
    epoch: OwnerEpoch,
}

/// A process-wide, pre-started cleanup thread for guards dropped while armed.
#[derive(Clone)]
pub(super) struct OwnerReleaseWorker {
    sender: tokio::sync::mpsc::UnboundedSender<ReleaseJob>,
}

impl OwnerReleaseWorker {
    /// Starts the cleanup runtime before owner acquisition and returns its
    /// sender. A failed thread or runtime startup leaves the caller free to
    /// fail the build.
    pub(super) fn shared() -> Result<Self, String> {
        static WORKER: OnceLock<Mutex<Option<OwnerReleaseWorker>>> = OnceLock::new();
        let slot = WORKER.get_or_init(|| Mutex::new(None));
        let mut slot = slot
            .lock()
            .map_err(|error| format!("owner cleanup worker lock poisoned: {error}"))?;
        if let Some(worker) = slot.as_ref() {
            return Ok(worker.clone());
        }
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let (ready_sender, ready_receiver) = mpsc::sync_channel(0);
        std::thread::Builder::new()
            .name("task-owner-release".into())
            .spawn(move || run_cleanup_worker(receiver, ready_sender))
            .map_err(|error| format!("failed to start owner cleanup worker: {error}"))?;
        ready_receiver
            .recv()
            .map_err(|error| format!("owner cleanup worker stopped before ready: {error}"))??;
        let worker = Self { sender };
        *slot = Some(worker.clone());
        Ok(worker)
    }
}

/// Creates the cleanup runtime, acknowledges readiness, then processes jobs.
fn run_cleanup_worker(
    mut receiver: tokio::sync::mpsc::UnboundedReceiver<ReleaseJob>,
    ready: mpsc::SyncSender<Result<(), String>>,
) {
    let runtime = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
        Ok(runtime) => runtime,
        Err(error) => {
            let _ = ready.send(Err(format!("failed to create owner cleanup runtime: {error}")));
            return;
        }
    };
    if ready.send(Ok(())).is_err() {
        return;
    }
    runtime.block_on(async move {
        while let Some(ReleaseJob { store, epoch }) = receiver.recv().await {
            tokio::spawn(async move {
                let release = async { store.release_owner(epoch).await };
                match AssertUnwindSafe(release).catch_unwind().await {
                    Ok(result) => log_release_error(epoch, result),
                    Err(_) => {
                        eprintln!("owner cleanup worker panicked while releasing epoch {epoch:?}")
                    }
                }
            });
        }
    });
}

impl OwnerReleaseGuard {
    /// Arms the guard for `epoch` acquired from `store`.
    pub(super) fn new(store: Arc<dyn TaskStore>, epoch: OwnerEpoch, cleanup: OwnerReleaseWorker) -> Self {
        Self {
            owner: Some((store, epoch)),
            cleanup,
        }
    }

    /// Releases ownership and disarms the guard after the store future returns.
    ///
    /// If this future is cancelled while waiting, dropping the still-armed
    /// guard queues the same store release on the pre-started cleanup thread.
    /// Store errors are returned to callers; a failed attempt leaves the
    /// guard armed for retry.
    pub(super) async fn release(&mut self) -> Result<(), StoreError> {
        let Some((store, epoch)) = self.owner.as_ref() else {
            return Ok(());
        };
        let store = Arc::clone(store);
        let epoch = *epoch;
        let result = store.release_owner(epoch).await;
        if result.is_ok() {
            self.owner = None;
        }
        result
    }
}

impl Drop for OwnerReleaseGuard {
    fn drop(&mut self) {
        let Some((store, epoch)) = self.owner.take() else {
            return;
        };
        if let Err(error) = self.cleanup.sender.send(ReleaseJob { store, epoch }) {
            eprintln!("failed to queue owner cleanup for epoch {epoch:?}; ownership may remain held: {error}");
        }
    }
}

/// Reports a store release failure with its owner generation.
fn log_release_error(epoch: OwnerEpoch, result: Result<(), StoreError>) {
    if let Err(error) = result {
        eprintln!("failed to release task store owner epoch {epoch:?}: {error}");
    }
}
