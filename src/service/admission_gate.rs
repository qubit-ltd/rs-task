// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
// qubit-style: allow multiple-public-types
//! Coordinates admission with one service-wide shutdown result.

use parking_lot::Mutex;
use tokio::pin;
use tokio::sync::Notify;

use super::task_execution_service::TaskServiceError;

/// Copyable shutdown failure retained for every caller awaiting close.
#[derive(Clone)]
#[must_use]
enum CloseFailure {
    /// A shutdown failure without a dedicated service error category.
    Other(String),
    /// A task store failure that must be returned to every shutdown caller.
    Store(String),
    /// A scheduler worker failure that must be returned to every shutdown
    /// caller.
    Scheduler(String),
    /// An event notification worker failed while closing.
    NotificationClose(String),
}

/// Admission and shutdown coordination lifecycle.
enum Phase {
    /// New operations may enter.
    Open,
    /// No new operations may enter; existing permits are draining.
    Closing,
    /// Shutdown has published its final result.
    Closed,
}

/// State protected by the admission gate mutex.
struct GateState {
    /// Current admission lifecycle phase.
    phase: Phase,
    /// Operations admitted before shutdown that have not finished.
    active: usize,
    /// Shared final shutdown outcome, once published.
    close_result: Option<Result<(), CloseFailure>>,
}

/// Serializes admission against shutdown and tracks operations already inside.
pub(super) struct AdmissionGate {
    /// Active count, phase, and shared shutdown outcome.
    state: Mutex<GateState>,
    /// Notifies admissions and shutdown waiters when gate state changes.
    changed: Notify,
}

/// Keeps one accepted operation in the gate until all its side effects finish.
pub(super) struct AdmissionPermit<'a> {
    /// Gate whose active-operation count this permit holds.
    gate: &'a AdmissionGate,
}

impl AdmissionGate {
    /// Creates an open gate with no in-flight operations.
    ///
    /// # Returns
    ///
    /// A gate ready to admit operations.
    pub(super) fn new() -> Self {
        Self {
            state: Mutex::new(GateState {
                phase: Phase::Open,
                active: 0,
                close_result: None,
            }),
            changed: Notify::new(),
        }
    }

    /// Reports whether shutdown has started.
    ///
    /// # Returns
    ///
    /// Whether the gate has left its open phase.
    #[must_use]
    pub(super) fn is_closing(&self) -> bool {
        !matches!(self.state.lock().phase, Phase::Open)
    }

    /// Reports whether every operation admitted before close has finished.
    ///
    /// # Returns
    ///
    /// Whether the active permit count is zero.
    #[must_use]
    pub(super) fn is_idle(&self) -> bool {
        self.state.lock().active == 0
    }

    /// Enters before the first storage operation, or rejects a closed gate.
    ///
    /// # Returns
    ///
    /// A permit held through the operation's side effects.
    ///
    /// # Errors
    ///
    /// Returns `ShuttingDown` after shutdown has started.
    pub(super) fn enter(&self) -> Result<AdmissionPermit<'_>, TaskServiceError> {
        let mut state = self.state.lock();
        if !matches!(state.phase, Phase::Open) {
            return Err(TaskServiceError::ShuttingDown);
        }
        state.active += 1;
        Ok(AdmissionPermit { gate: self })
    }

    /// Starts closing and returns whether this call owns shutdown coordination.
    ///
    /// # Returns
    ///
    /// `true` only for the call that transitions the gate from open to closing.
    #[must_use]
    pub(super) fn close(&self) -> bool {
        let mut state = self.state.lock();
        if !matches!(state.phase, Phase::Open) {
            return false;
        }
        state.phase = Phase::Closing;
        self.changed.notify_waiters();
        true
    }

    /// Waits until permits granted before closing have all been dropped.
    ///
    /// # Returns
    ///
    /// Completes after every active permit has been released.
    pub(super) async fn wait_idle(&self) {
        loop {
            let notified = self.changed.notified();
            pin!(notified);
            notified.as_mut().enable();
            if self.state.lock().active == 0 {
                return;
            }
            notified.await;
        }
    }

    /// Publishes the coordinator result exactly once and wakes all waiters.
    ///
    /// # Parameters
    ///
    /// * `result` - Final outcome shared by all shutdown callers.
    pub(super) fn finish_close(&self, result: Result<(), TaskServiceError>) {
        let mut state = self.state.lock();
        if matches!(state.phase, Phase::Closed) {
            return;
        }
        state.close_result = Some(result.map_err(|error| match error {
            TaskServiceError::NotificationClose(message) => CloseFailure::NotificationClose(message),
            TaskServiceError::StoreUnavailable(message) => CloseFailure::Store(message),
            TaskServiceError::SchedulerUnavailable(message) => CloseFailure::Scheduler(message),
            other => CloseFailure::Other(other.to_string()),
        }));
        state.phase = Phase::Closed;
        self.changed.notify_waiters();
    }

    /// Returns the same completed shutdown diagnosis to each caller.
    ///
    /// # Returns
    ///
    /// The published shutdown result, or waits until one is available.
    ///
    /// # Errors
    ///
    /// Returns the retained service error if shutdown failed.
    pub(super) async fn wait_closed(&self) -> Result<(), TaskServiceError> {
        loop {
            let notified = self.changed.notified();
            pin!(notified);
            notified.as_mut().enable();
            if let Some(result) = self.state.lock().close_result.clone() {
                return result.map_err(|error| match error {
                    CloseFailure::Other(message) | CloseFailure::Store(message) => {
                        TaskServiceError::StoreUnavailable(message)
                    }
                    CloseFailure::Scheduler(message) => TaskServiceError::SchedulerUnavailable(message),
                    CloseFailure::NotificationClose(message) => TaskServiceError::NotificationClose(message),
                });
            }
            notified.await;
        }
    }
}

impl Drop for AdmissionPermit<'_> {
    /// Decrements the active count and wakes tasks waiting for idle state.
    fn drop(&mut self) {
        let mut state = self.gate.state.lock();
        state.active -= 1;
        self.gate.changed.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use tokio::test as tokio_test;

    use super::AdmissionGate;
    use crate::service::TaskServiceError;

    #[tokio_test]
    async fn test_wait_closed_preserves_notification_close_failure_for_all_callers() {
        let gate = AdmissionGate::new();
        assert!(gate.close());
        gate.finish_close(Err(TaskServiceError::NotificationClose("worker panicked".into())));

        for _ in 0..2 {
            let error = gate.wait_closed().await.expect_err("close failure is retained");
            assert!(matches!(error, TaskServiceError::NotificationClose(message) if message == "worker panicked"));
        }
    }

    #[tokio_test]
    async fn test_wait_closed_keeps_other_failures_as_store_unavailable() {
        let gate = AdmissionGate::new();
        assert!(gate.close());
        gate.finish_close(Err(TaskServiceError::StoreUnavailable("store failed".into())));

        let error = gate.wait_closed().await.expect_err("close failure is retained");
        assert!(matches!(error, TaskServiceError::StoreUnavailable(message) if message == "store failed"));
    }
}
