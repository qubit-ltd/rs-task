// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;

use crate::model::OwnerEpoch;
use crate::store::StoreError;
use crate::store::TaskStore;

/// Owns a recovery lease until it is released or transferred to the service.
pub(in crate::service::task_execution_service_builder) struct OwnerGuard {
    /// Store whose ownership lease is managed by this guard.
    store: Arc<dyn TaskStore>,
    /// Lease not yet released or transferred to the running service.
    epoch: Option<OwnerEpoch>,
}

impl OwnerGuard {
    /// Creates a guard for an optional store ownership lease.
    ///
    /// # Parameters
    ///
    /// * `store` - Store that issued the lease.
    /// * `epoch` - Acquired lease, or `None` for a volatile store.
    ///
    /// # Returns
    ///
    /// A guard that releases an untransferred lease.
    pub(in crate::service::task_execution_service_builder) fn new(
        store: Arc<dyn TaskStore>,
        epoch: Option<OwnerEpoch>,
    ) -> Self {
        Self { store, epoch }
    }

    /// Transfers the lease to the constructed service.
    ///
    /// # Returns
    ///
    /// The lease epoch, if this guard still owns one.
    #[must_use]
    pub(in crate::service::task_execution_service_builder) fn transfer(&mut self) -> Option<OwnerEpoch> {
        self.epoch.take()
    }

    /// Releases an owned lease and retains no ownership after completion.
    ///
    /// # Returns
    ///
    /// Success when no lease remains or release succeeds.
    ///
    /// # Errors
    ///
    /// Returns the store error if the lease cannot be released.
    pub(in crate::service::task_execution_service_builder) async fn release(&mut self) -> Result<(), StoreError> {
        if let Some(epoch) = self.epoch.take() {
            self.store.release_owner(epoch).await
        } else {
            Ok(())
        }
    }
}

impl Drop for OwnerGuard {
    /// Schedules best-effort asynchronous release if the caller drops the
    /// guard before explicit cleanup.
    fn drop(&mut self) {
        if let Some(epoch) = self.epoch.take() {
            let store = Arc::clone(&self.store);
            crate::service::task_execution_service::runtime()
                .handle()
                .spawn(async move {
                    if let Err(error) = store.release_owner(epoch).await {
                        eprintln!("task service owner guard could not release ownership: {error}");
                    }
                });
        }
    }
}
