// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use tokio::sync::watch;

use super::internal::CancellationState;

/// Read-only cancellation state shared with a task handler.
///
/// The service supplies this handle through the typed task context. It can
/// report persisted cancellation requests and wait for cooperative requests.
///
/// # Examples
///
/// ```
/// use qubit_task::TaskContext;
///
/// fn inspect_cancellation(context: &TaskContext) {
///     let cancellation = context.cancellation();
///     let _requested = cancellation.is_requested();
/// }
/// ```
#[derive(Clone)]
pub struct TaskCancellation {
    /// Receives service-published state changes for one active attempt.
    pub(super) state: watch::Receiver<CancellationState>,
}

impl TaskCancellation {
    /// Returns whether cancellation has been durably requested for this
    /// attempt.
    ///
    /// # Returns
    ///
    /// `true` after either cancellation request mode has been published, and
    /// `false` while the attempt has no request.
    #[must_use]
    pub fn is_requested(&self) -> bool {
        *self.state.borrow() != CancellationState::Clear
    }

    /// Waits until the service publishes a cooperative cancellation request.
    ///
    /// This future remains pending if the service-side sender closes before a
    /// cooperative request is published.
    pub async fn wait_requested(&self) {
        let mut state = self.state.clone();
        loop {
            if *state.borrow() == CancellationState::CooperativeRequested {
                return;
            }
            if state.changed().await.is_err() {
                std::future::pending::<()>().await;
            }
        }
    }
}
