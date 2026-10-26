// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Holds acquired ownership until release has completed.

use std::sync::Arc;

use super::owner_release_worker::OwnerReleaseWorker;
use crate::model::OwnerEpoch;
use crate::store::StoreError;
use crate::store::TaskStore;

/// Holds an acquired owner epoch until release has finished.
pub(super) struct OwnerReleaseGuard {
    /// Holds the store and epoch until release succeeds or drop queues cleanup.
    owner: Option<(Arc<dyn TaskStore>, OwnerEpoch)>,
    /// Keeps the pre-started worker available for cancellation cleanup.
    cleanup: OwnerReleaseWorker,
}

impl OwnerReleaseGuard {
    /// Arms the guard for `epoch` acquired from `store`.
    ///
    /// # Parameters
    ///
    /// * `store` is the store that issued the ownership epoch.
    /// * `epoch` identifies the acquired ownership.
    /// * `cleanup` is the process-wide worker used if this guard is dropped
    ///   while still armed.
    ///
    /// # Returns
    ///
    /// A guard that releases `epoch` directly or queues it during drop.
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
    ///
    /// # Returns
    ///
    /// `Ok(())` when the owner was released or the guard was already disarmed.
    ///
    /// # Errors
    ///
    /// Returns the store error when the release attempt fails; the guard stays
    /// armed so callers can retry.
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
    /// Queues unfinished ownership cleanup when the guard is dropped armed.
    ///
    /// If the cleanup worker has stopped and cannot accept the request, logs
    /// the failure; the ownership may then remain held by the store.
    fn drop(&mut self) {
        let Some((store, epoch)) = self.owner.take() else {
            return;
        };
        if let Err(error) = self.cleanup.queue_release(store, epoch) {
            eprintln!("failed to queue owner cleanup for epoch {epoch:?}; ownership may remain held: {error}");
        }
    }
}
