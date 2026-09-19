// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::MutexGuard;

use super::task_execution_service_error::TaskExecutionServiceError;
use super::task_execution_stats::TaskExecutionStats;
use super::task_id::TaskId;
use super::task_status::TaskStatus;

/// Erased cancellation endpoint for a single unstarted task.
pub(super) type CancelFn = Arc<dyn Fn() -> bool + Send + Sync>;

/// Identity of one submission, distinct even when its business ID is reused.
#[derive(Clone)]
pub(super) struct SubmissionToken(Arc<()>);

impl SubmissionToken {
    /// Returns whether two tokens belong to the same submission.
    fn same_as(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

/// Registry state shared by the service and accepted pool jobs.
pub(super) struct TaskExecutionServiceState {
    inner: Mutex<Inner>,
    idle: Condvar,
}

impl TaskExecutionServiceState {
    /// Creates an empty registry with the specified terminal history limit.
    pub(super) fn new(history_capacity: usize) -> Self {
        Self {
            inner: Mutex::new(Inner::new(history_capacity)),
            idle: Condvar::new(),
        }
    }

    /// Locks the registry; poisoning indicates an internal invariant failure.
    fn lock_inner(&self) -> MutexGuard<'_, Inner> {
        self.inner
            .lock()
            .expect("task execution service state lock should not be poisoned")
    }

    /// Reserves an ID before asking the pool to accept the task.
    ///
    /// Returns `Suspended` or `DuplicateTask` without retaining the supplied
    /// endpoint when the reservation cannot be made.
    pub(super) fn reserve(
        &self,
        task_id: TaskId,
        cancel: CancelFn,
    ) -> Result<SubmissionToken, TaskExecutionServiceError> {
        let mut inner = self.lock_inner();
        if inner.suspended {
            return Err(TaskExecutionServiceError::Suspended);
        }
        if inner.active.contains_key(&task_id) {
            return Err(TaskExecutionServiceError::DuplicateTask(task_id));
        }
        inner.completed.remove(&task_id);
        let token = SubmissionToken(Arc::new(()));
        inner.active.insert(
            task_id,
            ActiveRecord {
                token: token.clone(),
                phase: ActivePhase::Submitting,
                cancel,
            },
        );
        Ok(token)
    }

    /// Publishes pool acceptance for the matching reservation.
    pub(super) fn accept(&self, task_id: TaskId, token: &SubmissionToken) -> bool {
        let mut inner = self.lock_inner();
        let Some(record) = inner.active.get_mut(&task_id) else {
            return false;
        };
        if !record.token.same_as(token) || record.phase != ActivePhase::Submitting {
            return false;
        }
        record.phase = ActivePhase::Submitted;
        true
    }

    /// Marks the matching accepted task as running.
    pub(super) fn start(&self, task_id: TaskId, token: &SubmissionToken) -> bool {
        let mut inner = self.lock_inner();
        let Some(record) = inner.active.get_mut(&task_id) else {
            return false;
        };
        if !record.token.same_as(token) || record.phase != ActivePhase::Submitted {
            return false;
        }
        record.phase = ActivePhase::Running;
        true
    }

    /// Finishes a matching active task and retains only its bounded status.
    ///
    /// Returns `false` for a stale token or an already finished task. The
    /// caller must have won the underlying task slot before reporting a result.
    pub(super) fn finish(&self, task_id: TaskId, token: &SubmissionToken, status: TaskStatus) -> bool {
        debug_assert!(!status.is_active(), "finish requires a terminal status");
        let mut inner = self.lock_inner();
        if !inner.matches_active(task_id, token) {
            return false;
        }
        inner.active.remove(&task_id);
        inner.remember(task_id, token.clone(), status);
        self.idle.notify_all();
        true
    }

    /// Removes only the matching submission after pool rejection.
    ///
    /// A rare rejection after the accept callback may find this submission in
    /// the completed history. It is removed there as well, without touching a
    /// newer submission of the same business ID.
    pub(super) fn discard(&self, task_id: TaskId, token: &SubmissionToken) -> bool {
        let mut inner = self.lock_inner();
        if inner.matches_active(task_id, token) {
            inner.active.remove(&task_id);
            self.idle.notify_all();
            return true;
        }
        if inner
            .completed
            .get(&task_id)
            .is_some_and(|record| record.token.same_as(token))
        {
            inner.completed.remove(&task_id);
            return true;
        }
        false
    }

    /// Returns the cancellation endpoint of an accepted, unstarted task.
    ///
    /// The caller must invoke it after releasing the registry lock and pass
    /// the returned token to `finish` if cancellation wins.
    pub(super) fn cancel_candidate(&self, task_id: TaskId) -> Option<(SubmissionToken, CancelFn)> {
        let inner = self.lock_inner();
        let record = inner.active.get(&task_id)?;
        (record.phase == ActivePhase::Submitted).then(|| (record.token.clone(), Arc::clone(&record.cancel)))
    }

    /// Returns the visible status of the latest retained submission.
    ///
    /// Unaccepted reservations and evicted terminal records return `None`.
    pub(super) fn status(&self, task_id: TaskId) -> Option<TaskStatus> {
        let inner = self.lock_inner();
        if let Some(record) = inner.active.get(&task_id) {
            return match record.phase {
                ActivePhase::Submitting => None,
                ActivePhase::Submitted => Some(TaskStatus::Submitted),
                ActivePhase::Running => Some(TaskStatus::Running),
            };
        }
        inner.completed.get(&task_id).map(|record| record.status)
    }

    /// Computes counts for visible active and retained terminal records.
    pub(super) fn stats(&self) -> TaskExecutionStats {
        let inner = self.lock_inner();
        let mut stats = TaskExecutionStats::default();
        for record in inner.active.values() {
            match record.phase {
                ActivePhase::Submitting => {}
                ActivePhase::Submitted => stats.add_status(TaskStatus::Submitted),
                ActivePhase::Running => stats.add_status(TaskStatus::Running),
            }
        }
        for record in inner.completed.values() {
            stats.add_status(record.status);
        }
        stats
    }

    /// Enables or disables admission of new reservations.
    pub(super) fn set_suspended(&self, suspended: bool) {
        self.lock_inner().suspended = suspended;
    }

    /// Returns whether admission is suspended.
    pub(super) fn is_suspended(&self) -> bool {
        self.lock_inner().suspended
    }

    /// Blocks until submissions active at the snapshot have left the registry.
    pub(super) fn await_in_flight_tasks_completion(&self) {
        let mut inner = self.lock_inner();
        let snapshot = inner
            .active
            .iter()
            .map(|(&task_id, record)| (task_id, record.token.clone()))
            .collect::<Vec<_>>();
        while snapshot
            .iter()
            .any(|(task_id, token)| inner.matches_active(*task_id, token))
        {
            inner = self.wait_for_idle_notification(inner);
        }
    }

    /// Blocks until no reservation or accepted task remains active.
    pub(super) fn await_idle(&self) {
        let mut inner = self.lock_inner();
        while !inner.active.is_empty() {
            inner = self.wait_for_idle_notification(inner);
        }
    }

    /// Waits for a registry transition and reacquires its lock.
    fn wait_for_idle_notification<'a>(&self, inner: MutexGuard<'a, Inner>) -> MutexGuard<'a, Inner> {
        self.idle
            .wait(inner)
            .expect("task execution service state lock should not be poisoned")
    }
}

/// Mutable state guarded by the registry mutex.
struct Inner {
    suspended: bool,
    active: HashMap<TaskId, ActiveRecord>,
    completed: HashMap<TaskId, CompletedRecord>,
    completed_order: VecDeque<(TaskId, SubmissionToken)>,
    history_capacity: usize,
}

impl Inner {
    /// Creates the empty registry maps and completion order.
    fn new(history_capacity: usize) -> Self {
        Self {
            suspended: false,
            active: HashMap::new(),
            completed: HashMap::new(),
            completed_order: VecDeque::new(),
            history_capacity,
        }
    }

    /// Checks that an active ID still denotes this exact submission.
    fn matches_active(&self, task_id: TaskId, token: &SubmissionToken) -> bool {
        self.active
            .get(&task_id)
            .is_some_and(|record| record.token.same_as(token))
    }

    /// Keeps one terminal status, evicting the oldest completion if needed.
    fn remember(&mut self, task_id: TaskId, token: SubmissionToken, status: TaskStatus) {
        if self.history_capacity == 0 {
            return;
        }
        self.completed.insert(
            task_id,
            CompletedRecord {
                token: token.clone(),
                status,
            },
        );
        self.completed_order.push_back((task_id, token));
        while self.completed_order.len() > self.history_capacity {
            if let Some((old_id, old_token)) = self.completed_order.pop_front()
                && self
                    .completed
                    .get(&old_id)
                    .is_some_and(|record| record.token.same_as(&old_token))
            {
                self.completed.remove(&old_id);
            }
        }
    }
}

/// Lifecycle phase before a task reaches a terminal status.
#[derive(Clone, Copy, PartialEq, Eq)]
enum ActivePhase {
    Submitting,
    Submitted,
    Running,
}

/// One active submission and its type-erased cancellation endpoint.
struct ActiveRecord {
    token: SubmissionToken,
    phase: ActivePhase,
    cancel: CancelFn,
}

/// Lightweight terminal status retained for a bounded interval.
struct CompletedRecord {
    token: SubmissionToken,
    status: TaskStatus,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    use super::CancelFn;
    use super::TaskExecutionServiceState;
    use crate::service::TaskStatus;

    /// Creates a cancellation endpoint without a registry reference.
    fn inert_cancel() -> CancelFn {
        Arc::new(|| false)
    }

    #[test]
    fn test_state_releases_terminal_records_without_reference_cycle() {
        let state = Arc::new(TaskExecutionServiceState::new(1));
        let weak = Arc::downgrade(&state);
        let token = state.reserve(7, inert_cancel()).expect("ID should be free");
        assert!(state.accept(7, &token));
        assert!(state.finish(7, &token, TaskStatus::Succeeded));
        drop(state);
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn test_history_capacity_and_reused_id_do_not_retain_stale_entries() {
        let state = TaskExecutionServiceState::new(1);
        let first = state.reserve(7, inert_cancel()).expect("ID should be free");
        assert!(state.accept(7, &first));
        assert!(state.finish(7, &first, TaskStatus::Succeeded));
        assert_eq!(state.status(7), Some(TaskStatus::Succeeded));
        let second = state.reserve(7, inert_cancel()).expect("ID should be reusable");
        assert_eq!(state.status(7), None);
        assert!(state.accept(7, &second));
        assert!(state.finish(7, &second, TaskStatus::Failed));
        assert_eq!(state.status(7), Some(TaskStatus::Failed));
        let third = state.reserve(8, inert_cancel()).expect("ID should be free");
        assert!(state.accept(8, &third));
        assert!(state.finish(8, &third, TaskStatus::Cancelled));
        assert_eq!(state.status(7), None);
        assert_eq!(state.status(8), Some(TaskStatus::Cancelled));
        let inner = state.lock_inner();
        assert_eq!(inner.completed.len(), 1);
        assert_eq!(inner.completed_order.len(), 1);
    }

    #[test]
    fn test_history_capacity_two_keeps_two_newest_completions() {
        let state = TaskExecutionServiceState::new(2);
        for id in 1..=3 {
            let token = state.reserve(id, inert_cancel()).expect("ID should be free");
            assert!(state.accept(id, &token));
            assert!(state.finish(id, &token, TaskStatus::Succeeded));
        }
        assert_eq!(state.status(1), None);
        assert_eq!(state.status(2), Some(TaskStatus::Succeeded));
        assert_eq!(state.status(3), Some(TaskStatus::Succeeded));
        assert_eq!(state.stats().total, 2);
        let inner = state.lock_inner();
        assert_eq!(inner.completed.len(), 2);
        assert_eq!(inner.completed_order.len(), 2);
    }

    #[test]
    fn test_zero_history_and_stale_callbacks_do_not_change_new_submission() {
        let state = TaskExecutionServiceState::new(0);
        let first = state.reserve(7, Arc::new(|| true)).expect("ID should be free");
        assert!(state.accept(7, &first));
        let (stale_token, stale_cancel) = state.cancel_candidate(7).expect("accepted task");
        assert!(state.discard(7, &first));
        let second = state.reserve(7, inert_cancel()).expect("ID should be reusable");
        assert!(stale_cancel());
        assert!(!state.finish(7, &stale_token, TaskStatus::Cancelled));
        assert!(!state.discard(7, &first));
        assert!(state.accept(7, &second));
        assert_eq!(state.status(7), Some(TaskStatus::Submitted));
        assert!(state.finish(7, &second, TaskStatus::Succeeded));
        assert_eq!(state.status(7), None);
        assert_eq!(state.stats().total, 0);
    }

    #[test]
    fn test_reservation_is_hidden_but_waited_for() {
        let state = Arc::new(TaskExecutionServiceState::new(2));
        let token = state.reserve(7, inert_cancel()).expect("ID should be free");
        assert_eq!(state.status(7), None);
        assert_eq!(state.stats().total, 0);
        assert!(state.cancel_candidate(7).is_none());
        let (started_tx, started_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let wait_state = Arc::clone(&state);
        let waiter = thread::spawn(move || {
            started_tx.send(()).expect("start signal should send");
            wait_state.await_idle();
            done_tx.send(()).expect("completion signal should send");
        });
        started_rx.recv().expect("waiter should start");
        assert!(done_rx.recv_timeout(Duration::from_millis(20)).is_err());
        assert!(state.discard(7, &token));
        done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("waiter should finish");
        waiter.join().expect("waiter should not panic");
    }
}
