// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
// qubit-style: allow multiple-public-types
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::MutexGuard;

use qubit_id::Id;

use super::task_execution_service_error::TaskExecutionServiceError;
use super::task_execution_stats::TaskExecutionStats;
use super::task_status::TaskStatus;

/// Erased cancellation endpoint for a single unstarted task.
pub(super) type CancelFn = Arc<dyn Fn() -> bool + Send + Sync>;

/// Identity of one submission, distinct even when its business ID is reused.
#[derive(Clone)]
pub(super) struct SubmissionToken(
    /// Reference-counted identity used for pointer equality between callbacks.
    Arc<()>,
);

impl SubmissionToken {
    /// Returns whether two tokens belong to the same submission.
    #[must_use]
    fn same_as(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

/// Registry state shared by the service and accepted pool jobs.
pub(super) struct TaskExecutionServiceState {
    /// Mutable lifecycle registry protected against concurrent callbacks.
    inner: Mutex<Inner>,
    /// Wakes waiters after a task leaves the active registry.
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
    ///
    /// # Returns
    ///
    /// A submission token on success, or the specific rejection error.
    pub(super) fn reserve(&self, task_id: Id, cancel: CancelFn) -> Result<SubmissionToken, TaskExecutionServiceError> {
        let mut inner = self.lock_inner();
        if inner.suspended {
            return Err(TaskExecutionServiceError::Suspended);
        }
        if inner.active.contains_key(&task_id) {
            return Err(TaskExecutionServiceError::DuplicateTask(task_id));
        }
        let previous_completed = inner.completed.remove(&task_id);
        let token = SubmissionToken(Arc::new(()));
        inner.active.insert(
            task_id,
            ActiveRecord {
                token: token.clone(),
                phase: ActivePhase::Submitting,
                cancel,
                previous_completed,
            },
        );
        Ok(token)
    }

    /// Publishes pool acceptance for the matching reservation.
    ///
    /// # Returns
    ///
    /// `true` when this callback transitions the matching reservation;
    /// otherwise `false` for a stale or already transitioned token.
    #[must_use]
    pub(super) fn accept(&self, task_id: Id, token: &SubmissionToken) -> bool {
        let mut inner = self.lock_inner();
        let Some(record) = inner.active.get_mut(&task_id) else {
            return false;
        };
        if !record.token.same_as(token) || record.phase != ActivePhase::Submitting {
            return false;
        }
        record.phase = ActivePhase::Submitted;
        let previous_completed = record.previous_completed.take();
        if let Some(previous_completed) = previous_completed {
            inner.remove_order_marker(task_id, &previous_completed.token);
        }
        true
    }

    /// Marks the matching accepted task as running.
    ///
    /// # Returns
    ///
    /// `true` when this callback transitions the matching accepted task;
    /// otherwise `false` for a stale or already transitioned token.
    #[must_use]
    pub(super) fn start(&self, task_id: Id, token: &SubmissionToken) -> bool {
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
    ///
    /// # Returns
    ///
    /// `true` when the matching active task was finished; otherwise `false`.
    #[must_use]
    pub(super) fn finish(&self, task_id: Id, token: &SubmissionToken, status: TaskStatus) -> bool {
        debug_assert!(!status.is_active(), "finish requires a terminal status");
        let mut inner = self.lock_inner();
        if !inner.matches_active(task_id, token) {
            return false;
        }
        let record = inner
            .active
            .remove(&task_id)
            .expect("matching active submission should exist");
        if let Some(previous_completed) = record.previous_completed {
            inner.remove_order_marker(task_id, &previous_completed.token);
        }
        inner.remember(task_id, token.clone(), status);
        self.idle.notify_all();
        true
    }

    /// Removes only the matching submission after pool rejection.
    ///
    /// A rare rejection after the accept callback may find this submission in
    /// the completed history. It is removed there as well, without touching a
    /// newer submission of the same business ID.
    ///
    /// # Returns
    ///
    /// `true` when the matching submission was removed; otherwise `false`.
    #[must_use]
    pub(super) fn discard(&self, task_id: Id, token: &SubmissionToken) -> bool {
        let mut inner = self.lock_inner();
        if inner.matches_active(task_id, token) {
            let record = inner
                .active
                .remove(&task_id)
                .expect("matching active submission should exist");
            if let Some(previous_completed) = record.previous_completed {
                let previous_is_retained = inner
                    .completed_order
                    .iter()
                    .any(|(completed_id, token)| *completed_id == task_id && token.same_as(&previous_completed.token));
                if previous_is_retained {
                    inner.completed.insert(task_id, previous_completed);
                    inner.evict_to_capacity();
                }
            }
            self.idle.notify_all();
            return true;
        }
        if inner
            .completed
            .get(&task_id)
            .is_some_and(|record| record.token.same_as(token))
        {
            inner.completed.remove(&task_id);
            inner.remove_order_marker(task_id, token);
            return true;
        }
        false
    }

    /// Returns the cancellation endpoint of an accepted, unstarted task.
    ///
    /// The caller must invoke it after releasing the registry lock and pass
    /// the returned token to `finish` if cancellation wins.
    ///
    /// # Returns
    ///
    /// The matching submission token and cancellation endpoint, or `None` if
    /// the task is unknown or has already started.
    #[must_use]
    pub(super) fn cancel_candidate(&self, task_id: Id) -> Option<(SubmissionToken, CancelFn)> {
        let inner = self.lock_inner();
        let record = inner.active.get(&task_id)?;
        (record.phase == ActivePhase::Submitted).then(|| (record.token.clone(), Arc::clone(&record.cancel)))
    }

    /// Returns the visible status of the latest retained submission.
    ///
    /// Unaccepted reservations and evicted terminal records return `None`.
    ///
    /// # Returns
    ///
    /// The latest visible status, or `None` when no visible status exists.
    #[must_use]
    pub(super) fn status(&self, task_id: Id) -> Option<TaskStatus> {
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
    ///
    /// # Returns
    ///
    /// A snapshot of all visible registry records.
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
    #[must_use]
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
    /// Whether new reservations are currently rejected.
    suspended: bool,
    /// Submissions that have not reached a terminal state.
    active: HashMap<Id, ActiveRecord>,
    /// Most recently retained terminal status for each task ID.
    completed: HashMap<Id, CompletedRecord>,
    /// Completion order used to evict the oldest retained records.
    completed_order: VecDeque<(Id, SubmissionToken)>,
    /// Maximum number of terminal records retained.
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
    #[must_use]
    fn matches_active(&self, task_id: Id, token: &SubmissionToken) -> bool {
        self.active
            .get(&task_id)
            .is_some_and(|record| record.token.same_as(token))
    }

    /// Removes the order marker owned by one submission.
    fn remove_order_marker(&mut self, task_id: Id, token: &SubmissionToken) {
        self.completed_order
            .retain(|(id, marker)| *id != task_id || !marker.same_as(token));
    }

    /// Drops oldest effective records until the configured capacity is met.
    fn evict_to_capacity(&mut self) {
        while self.completed.len() > self.history_capacity {
            let Some((old_id, old_token)) = self.completed_order.pop_front() else {
                break;
            };
            if self
                .completed
                .get(&old_id)
                .is_some_and(|record| record.token.same_as(&old_token))
            {
                self.completed.remove(&old_id);
            } else if let Some(active) = self.active.get_mut(&old_id)
                && active
                    .previous_completed
                    .as_ref()
                    .is_some_and(|record| record.token.same_as(&old_token))
            {
                active.previous_completed = None;
            }
        }
    }

    /// Keeps one terminal status, evicting the oldest completion if needed.
    fn remember(&mut self, task_id: Id, token: SubmissionToken, status: TaskStatus) {
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
        self.evict_to_capacity();
    }
}

/// Lifecycle phase before a task reaches a terminal status.
#[derive(Clone, Copy, PartialEq, Eq)]
enum ActivePhase {
    /// The submission is reserved locally but not yet accepted by the pool.
    Submitting,
    /// The pool accepted the submission but has not started it.
    Submitted,
    /// A worker is executing the task.
    Running,
}

/// One active submission and its type-erased cancellation endpoint.
struct ActiveRecord {
    /// Identity of the submission owning this record.
    token: SubmissionToken,
    /// Current lifecycle phase.
    phase: ActivePhase,
    /// Endpoint used to cancel the task before execution starts.
    cancel: CancelFn,
    /// Previous terminal status restored if the pool rejects this submission.
    previous_completed: Option<CompletedRecord>,
}

/// Lightweight terminal status retained for a bounded interval.
struct CompletedRecord {
    /// Identity of the completed submission.
    token: SubmissionToken,
    /// Terminal status observed for the submission.
    status: TaskStatus,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    use qubit_id::Id;

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
        let token = state.reserve(Id::new(7), inert_cancel()).expect("ID should be free");
        assert!(state.accept(Id::new(7), &token));
        assert!(state.finish(Id::new(7), &token, TaskStatus::Succeeded));
        drop(state);
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn test_history_capacity_and_reused_id_do_not_retain_stale_entries() {
        let state = TaskExecutionServiceState::new(1);
        let first = state.reserve(Id::new(7), inert_cancel()).expect("ID should be free");
        assert!(state.accept(Id::new(7), &first));
        assert!(state.finish(Id::new(7), &first, TaskStatus::Succeeded));
        assert_eq!(state.status(Id::new(7)), Some(TaskStatus::Succeeded));
        let second = state
            .reserve(Id::new(7), inert_cancel())
            .expect("ID should be reusable");
        assert_eq!(state.status(Id::new(7)), None);
        assert!(state.accept(Id::new(7), &second));
        assert!(state.finish(Id::new(7), &second, TaskStatus::Failed));
        assert_eq!(state.status(Id::new(7)), Some(TaskStatus::Failed));
        let third = state.reserve(Id::new(8), inert_cancel()).expect("ID should be free");
        assert!(state.accept(Id::new(8), &third));
        assert!(state.finish(Id::new(8), &third, TaskStatus::Cancelled));
        assert_eq!(state.status(Id::new(7)), None);
        assert_eq!(state.status(Id::new(8)), Some(TaskStatus::Cancelled));
        let inner = state.lock_inner();
        assert_eq!(inner.completed.len(), 1);
        assert_eq!(inner.completed_order.len(), 1);
    }

    #[test]
    fn test_history_capacity_two_keeps_two_newest_completions() {
        let state = TaskExecutionServiceState::new(2);
        for id in 1..=3 {
            let token = state.reserve(Id::new(id), inert_cancel()).expect("ID should be free");
            assert!(state.accept(Id::new(id), &token));
            assert!(state.finish(Id::new(id), &token, TaskStatus::Succeeded));
        }
        assert_eq!(state.status(Id::new(1)), None);
        assert_eq!(state.status(Id::new(2)), Some(TaskStatus::Succeeded));
        assert_eq!(state.status(Id::new(3)), Some(TaskStatus::Succeeded));
        assert_eq!(state.stats().total, 2);
        let inner = state.lock_inner();
        assert_eq!(inner.completed.len(), 2);
        assert_eq!(inner.completed_order.len(), 2);
    }

    #[test]
    fn test_history_capacity_two_counts_reused_completion_once() {
        let state = TaskExecutionServiceState::new(2);
        let first = state.reserve(Id::new(1), inert_cancel()).expect("ID should be free");
        assert!(state.accept(Id::new(1), &first));
        assert!(state.finish(Id::new(1), &first, TaskStatus::Succeeded));
        let second = state.reserve(Id::new(2), inert_cancel()).expect("ID should be free");
        assert!(state.accept(Id::new(2), &second));
        assert!(state.finish(Id::new(2), &second, TaskStatus::Succeeded));

        let reused = state.reserve(Id::new(2), inert_cancel()).expect("ID should be reusable");
        assert!(state.accept(Id::new(2), &reused));
        assert!(state.finish(Id::new(2), &reused, TaskStatus::Failed));

        assert_eq!(state.status(Id::new(1)), Some(TaskStatus::Succeeded));
        assert_eq!(state.status(Id::new(2)), Some(TaskStatus::Failed));
        assert_eq!(state.stats().total, 2);
        let inner = state.lock_inner();
        assert_eq!(inner.completed_order.len(), inner.completed.len());
        assert_eq!(inner.completed.len(), 2);
    }

    #[test]
    fn test_finishing_reused_id_while_submitting_cleans_previous_marker() {
        let state = TaskExecutionServiceState::new(2);
        for id in 1..=2 {
            let token = state.reserve(Id::new(id), inert_cancel()).expect("ID should be free");
            assert!(state.accept(Id::new(id), &token));
            assert!(state.finish(Id::new(id), &token, TaskStatus::Succeeded));
        }

        let replacement = state.reserve(Id::new(2), inert_cancel()).expect("ID should be reusable");
        assert!(state.finish(Id::new(2), &replacement, TaskStatus::Failed));

        assert_eq!(state.status(Id::new(1)), Some(TaskStatus::Succeeded));
        assert_eq!(state.status(Id::new(2)), Some(TaskStatus::Failed));
        assert_eq!(state.stats().total, 2);
        let inner = state.lock_inner();
        assert_eq!(inner.completed_order.len(), inner.completed.len());
        assert_eq!(inner.completed.len(), 2);
    }

    #[test]
    fn test_rejected_reused_id_restores_original_history_position() {
        let state = TaskExecutionServiceState::new(2);
        for (id, status) in [(1, TaskStatus::Succeeded), (2, TaskStatus::Succeeded)] {
            let token = state.reserve(Id::new(id), inert_cancel()).expect("ID should be free");
            assert!(state.accept(Id::new(id), &token));
            assert!(state.finish(Id::new(id), &token, status));
        }
        let retry = state.reserve(Id::new(2), inert_cancel()).expect("ID should be reusable");
        assert_eq!(state.status(Id::new(2)), None);
        assert!(state.discard(Id::new(2), &retry));
        assert_eq!(state.status(Id::new(1)), Some(TaskStatus::Succeeded));
        assert_eq!(state.status(Id::new(2)), Some(TaskStatus::Succeeded));
        {
            let inner = state.lock_inner();
            assert_eq!(
                inner.completed_order.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
                [Id::new(1), Id::new(2)]
            );
            assert_eq!(inner.completed_order.len(), inner.completed.len());
        }

        let third = state.reserve(Id::new(3), inert_cancel()).expect("ID should be free");
        assert!(state.accept(Id::new(3), &third));
        assert!(state.finish(Id::new(3), &third, TaskStatus::Succeeded));
        assert_eq!(state.status(Id::new(1)), None);
        assert_eq!(state.status(Id::new(2)), Some(TaskStatus::Succeeded));
        assert_eq!(state.status(Id::new(3)), Some(TaskStatus::Succeeded));
    }

    #[test]
    fn test_rejected_reused_id_is_not_restored_after_history_eviction() {
        let state = TaskExecutionServiceState::new(2);
        for id in 1..=2 {
            let token = state.reserve(Id::new(id), inert_cancel()).expect("ID should be free");
            assert!(state.accept(Id::new(id), &token));
            assert!(state.finish(Id::new(id), &token, TaskStatus::Succeeded));
        }
        let retry = state.reserve(Id::new(2), inert_cancel()).expect("ID should be reusable");

        for id in 3..=4 {
            let token = state.reserve(Id::new(id), inert_cancel()).expect("ID should be free");
            assert!(state.accept(Id::new(id), &token));
            assert!(state.finish(Id::new(id), &token, TaskStatus::Succeeded));
        }
        assert!(state.discard(Id::new(2), &retry));
        assert_eq!(state.status(Id::new(1)), None);
        assert_eq!(state.status(Id::new(2)), None);
        assert_eq!(state.status(Id::new(3)), Some(TaskStatus::Succeeded));
        assert_eq!(state.status(Id::new(4)), Some(TaskStatus::Succeeded));
        assert_eq!(state.stats().total, 2);
        let inner = state.lock_inner();
        assert_eq!(inner.completed_order.len(), inner.completed.len());
    }

    #[test]
    fn test_rejected_reused_id_does_not_restore_evicted_terminal_record() {
        let state = TaskExecutionServiceState::new(1);
        let first = state.reserve(Id::new(1), inert_cancel()).expect("ID should be free");
        assert!(state.accept(Id::new(1), &first));
        assert!(state.finish(Id::new(1), &first, TaskStatus::Succeeded));

        let retry = state
            .reserve(Id::new(1), inert_cancel())
            .expect("terminal ID should be reusable");
        let second = state
            .reserve(Id::new(2), inert_cancel())
            .expect("second ID should be free");
        assert!(state.accept(Id::new(2), &second));
        assert!(state.finish(Id::new(2), &second, TaskStatus::Failed));

        assert!(state.discard(Id::new(1), &retry));
        assert_eq!(state.status(Id::new(1)), None);
        assert_eq!(state.status(Id::new(2)), Some(TaskStatus::Failed));
        assert_eq!(state.stats().total, 1);
    }

    #[test]
    fn test_zero_history_and_stale_callbacks_do_not_change_new_submission() {
        let state = TaskExecutionServiceState::new(0);
        let first = state.reserve(Id::new(7), Arc::new(|| true)).expect("ID should be free");
        assert!(state.accept(Id::new(7), &first));
        let (stale_token, stale_cancel) = state.cancel_candidate(Id::new(7)).expect("accepted task");
        assert!(state.discard(Id::new(7), &first));
        let second = state
            .reserve(Id::new(7), inert_cancel())
            .expect("ID should be reusable");
        assert!(stale_cancel());
        assert!(!state.finish(Id::new(7), &stale_token, TaskStatus::Cancelled));
        assert!(!state.discard(Id::new(7), &first));
        assert!(state.accept(Id::new(7), &second));
        assert_eq!(state.status(Id::new(7)), Some(TaskStatus::Submitted));
        assert!(state.finish(Id::new(7), &second, TaskStatus::Succeeded));
        assert_eq!(state.status(Id::new(7)), None);
        assert_eq!(state.stats().total, 0);
    }

    #[test]
    fn test_reservation_is_hidden_but_waited_for() {
        let state = Arc::new(TaskExecutionServiceState::new(2));
        let token = state.reserve(Id::new(7), inert_cancel()).expect("ID should be free");
        assert_eq!(state.status(Id::new(7)), None);
        assert_eq!(state.stats().total, 0);
        assert!(state.cancel_candidate(Id::new(7)).is_none());
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
        assert!(state.discard(Id::new(7), &token));
        done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("waiter should finish");
        waiter.join().expect("waiter should not panic");
    }
}
