// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Coordinates admission with one service-wide shutdown result.

use parking_lot::Mutex;
use tokio::sync::Notify;

use super::task_execution_service::TaskServiceError;

#[derive(Clone)]
enum CloseFailure {
    Other(String),
    Store(String),
    Scheduler(String),
    NotificationClose(String),
}

enum Phase {
    Open,
    Closing,
    Closed,
}

struct GateState {
    phase: Phase,
    active: usize,
    close_result: Option<Result<(), CloseFailure>>,
}

/// Serializes admission against shutdown and tracks operations already inside.
pub(super) struct AdmissionGate {
    state: Mutex<GateState>,
    changed: Notify,
}

/// Keeps one accepted operation in the gate until all its side effects finish.
pub(super) struct AdmissionPermit<'a> {
    gate: &'a AdmissionGate,
}

impl AdmissionGate {
    /// Creates an open gate with no in-flight operations.
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

    /// Enters before the first storage operation, or rejects a closed gate.
    pub(super) fn enter(&self) -> Result<AdmissionPermit<'_>, TaskServiceError> {
        let mut state = self.state.lock();
        if !matches!(state.phase, Phase::Open) {
            return Err(TaskServiceError::ShuttingDown);
        }
        state.active += 1;
        Ok(AdmissionPermit { gate: self })
    }

    /// Starts closing and returns whether this call owns shutdown coordination.
    pub(super) fn close(&self) -> bool {
        let mut state = self.state.lock();
        if !matches!(state.phase, Phase::Open) {
            return false;
        }
        state.phase = Phase::Closing;
        self.changed.notify_waiters();
        true
    }

    /// Reports whether shutdown has published its final result.
    pub(super) fn is_closed(&self) -> bool {
        matches!(self.state.lock().phase, Phase::Closed)
    }

    /// Waits until permits granted before closing have all been dropped.
    pub(super) async fn wait_idle(&self) {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.state.lock().active == 0 {
                return;
            }
            notified.await;
        }
    }

    /// Publishes the coordinator result exactly once and wakes all waiters.
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
    pub(super) async fn wait_closed(&self) -> Result<(), TaskServiceError> {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
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
    fn drop(&mut self) {
        let mut state = self.gate.state.lock();
        state.active -= 1;
        self.gate.changed.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::AdmissionGate;
    use crate::service::TaskServiceError;

    #[tokio::test]
    async fn test_wait_closed_preserves_notification_close_failure_for_all_callers() {
        let gate = AdmissionGate::new();
        assert!(gate.close());
        gate.finish_close(Err(TaskServiceError::NotificationClose("worker panicked".into())));

        for _ in 0..2 {
            let error = gate.wait_closed().await.expect_err("close failure is retained");
            assert!(matches!(error, TaskServiceError::NotificationClose(message) if message == "worker panicked"));
        }
    }

    #[tokio::test]
    async fn test_wait_closed_keeps_other_failures_as_store_unavailable() {
        let gate = AdmissionGate::new();
        assert!(gate.close());
        gate.finish_close(Err(TaskServiceError::StoreUnavailable("store failed".into())));

        let error = gate.wait_closed().await.expect_err("close failure is retained");
        assert!(matches!(error, TaskServiceError::StoreUnavailable(message) if message == "store failed"));
    }
}
