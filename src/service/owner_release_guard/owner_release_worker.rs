// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Runs deferred store ownership releases on a dedicated thread.

use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::mpsc;

use futures::FutureExt;

use super::internal::ReleaseJob;
use crate::model::OwnerEpoch;
use crate::store::StoreError;
use crate::store::TaskStore;

/// A process-wide, pre-started cleanup thread for guards dropped while armed.
#[derive(Clone)]
pub(in crate::service) struct OwnerReleaseWorker {
    /// Sends release requests to the process-wide cleanup thread.
    sender: tokio::sync::mpsc::UnboundedSender<ReleaseJob>,
}

impl OwnerReleaseWorker {
    /// Starts the cleanup runtime before owner acquisition and returns its
    /// shared worker. A failed thread or runtime startup leaves the caller
    /// free to fail the build.
    ///
    /// # Returns
    ///
    /// The shared cleanup worker after its thread and runtime are ready.
    ///
    /// # Errors
    ///
    /// Returns a diagnostic if locking the singleton or starting the thread or
    /// runtime fails.
    pub(in crate::service) fn shared() -> Result<Self, String> {
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

    /// Queues a store release for cleanup after the guard is dropped.
    ///
    /// # Parameters
    ///
    /// * `store` is the store whose ownership must be released.
    /// * `epoch` identifies that ownership.
    ///
    /// # Returns
    ///
    /// `Ok(())` when the cleanup channel accepted the request.
    ///
    /// # Errors
    ///
    /// Returns the channel error if the cleanup receiver has stopped.
    pub(super) fn queue_release(&self, store: Arc<dyn TaskStore>, epoch: OwnerEpoch) -> Result<(), String> {
        self.sender
            .send(ReleaseJob { store, epoch })
            .map_err(|error| error.to_string())
    }
}

/// Creates the cleanup runtime, acknowledges readiness, then processes jobs.
///
/// # Parameters
///
/// * `receiver` supplies release requests until all senders are dropped.
/// * `ready` reports whether runtime initialization succeeded.
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

/// Reports a store release failure with its owner generation.
///
/// # Parameters
///
/// * `epoch` identifies the ownership whose release was attempted.
/// * `result` is the outcome to report; successful releases produce no log.
fn log_release_error(epoch: OwnerEpoch, result: Result<(), StoreError>) {
    if let Err(error) = result {
        eprintln!("failed to release task store owner epoch {epoch:?}: {error}");
    }
}
