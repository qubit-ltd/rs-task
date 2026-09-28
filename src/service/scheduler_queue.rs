// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::collections::BTreeMap;
use std::collections::VecDeque;

use crate::model::TaskId;
use crate::scheduling::QueuedTask;

/// Holds ready work separately from tasks waiting for retry deadlines.
pub(crate) struct SchedulerQueue {
    /// Tasks eligible for immediate FIFO scheduling.
    ready: VecDeque<QueuedTask>,
    /// Tasks grouped by retry deadline in ascending deadline order.
    delayed: BTreeMap<u64, VecDeque<QueuedTask>>,
    /// Total number of ready and delayed tasks retained by the queue.
    len: usize,
}

impl SchedulerQueue {
    /// Creates an empty scheduler queue.
    ///
    /// # Returns
    ///
    /// A queue with no ready or delayed tasks.
    pub(crate) fn new() -> Self {
        Self {
            ready: VecDeque::new(),
            delayed: BTreeMap::new(),
            len: 0,
        }
    }

    /// Adds a task to the ready queue or its retry deadline bucket.
    ///
    /// # Parameters
    ///
    /// * `task` - Accepted task metadata to retain for scheduling.
    pub(crate) fn push(&mut self, task: QueuedTask) {
        self.len += 1;
        if let Some(deadline) = task.retry_not_before_ms {
            self.delayed.entry(deadline).or_default().push_back(task);
        } else {
            self.ready.push_back(task);
        }
    }

    /// Takes at most `budget` due tasks in FIFO order for one scheduling pass.
    ///
    /// # Parameters
    ///
    /// * `budget` - Maximum number of tasks returned in this window.
    /// * `now_ms` - Current epoch time used to identify due retry tasks.
    ///
    /// # Returns
    ///
    /// Due delayed tasks followed by ready tasks, up to the effective budget.
    pub(crate) fn take_window(&mut self, budget: usize, now_ms: u64) -> Vec<QueuedTask> {
        let budget = budget.max(1);
        let mut window = Vec::with_capacity(budget.min(self.len));
        while window.len() < budget {
            let Some((&deadline, _)) = self.delayed.first_key_value() else {
                break;
            };
            if deadline > now_ms {
                break;
            }
            let bucket = self.delayed.get_mut(&deadline).expect("first key exists");
            if let Some(mut task) = bucket.pop_front() {
                task.retry_not_before_ms = None;
                window.push(task);
            }
            if bucket.is_empty() {
                self.delayed.remove(&deadline);
            }
        }
        while window.len() < budget {
            let Some(task) = self.ready.pop_front() else {
                break;
            };
            window.push(task);
        }
        self.len -= window.len();
        window
    }

    /// Restores unstarted tasks at the front while preserving their order.
    ///
    /// # Parameters
    ///
    /// * `tasks` - Unstarted tasks removed from a previous scheduling window.
    pub(crate) fn restore_front(&mut self, tasks: Vec<QueuedTask>) {
        for task in tasks.into_iter().rev() {
            if let Some(deadline) = task.retry_not_before_ms {
                self.len += 1;
                self.delayed.entry(deadline).or_default().push_front(task);
            } else {
                self.len += 1;
                self.ready.push_front(task);
            }
        }
    }

    /// Restores unstarted tasks at the back so a later window can be scanned.
    ///
    /// # Parameters
    ///
    /// * `tasks` - Unstarted tasks removed from the current scheduling window.
    pub(crate) fn restore_back(&mut self, tasks: Vec<QueuedTask>) {
        for task in tasks {
            self.len += 1;
            if let Some(deadline) = task.retry_not_before_ms {
                self.delayed.entry(deadline).or_default().push_back(task);
            } else {
                self.ready.push_back(task);
            }
        }
    }

    /// Promotes the listed still-ready tasks while preserving their supplied
    /// order.
    ///
    /// Tasks that were removed or moved to a delayed retry bucket are ignored.
    ///
    /// # Parameters
    ///
    /// * `ids` - Task identifiers in their original FIFO order.
    pub(crate) fn promote_ids_front(&mut self, ids: &[TaskId]) {
        let tasks = self.take_ready_ids(ids);
        self.restore_front(tasks);
    }

    /// Counts one successful bypass for each listed ready task and promotes
    /// them.
    ///
    /// # Parameters
    ///
    /// * `ids` - Earlier task identifiers in their original FIFO order.
    pub(crate) fn record_bypass_and_promote_front(&mut self, ids: &[TaskId]) {
        let mut tasks = self.take_ready_ids(ids);
        for task in &mut tasks {
            task.bypasses = task.bypasses.saturating_add(1);
        }
        self.restore_front(tasks);
    }

    /// Removes matching ready tasks without changing their relative order.
    ///
    /// # Parameters
    ///
    /// * `ids` - Task identifiers to extract from the ready queue.
    ///
    /// # Returns
    ///
    /// Matching tasks in the order supplied by `ids`; absent IDs are ignored.
    fn take_ready_ids(&mut self, ids: &[TaskId]) -> Vec<QueuedTask> {
        use std::collections::HashMap;
        use std::collections::HashSet;

        let selected = ids.iter().copied().collect::<HashSet<_>>();
        let mut extracted = HashMap::with_capacity(selected.len());
        let mut retained = VecDeque::with_capacity(self.ready.len());
        while let Some(task) = self.ready.pop_front() {
            if selected.contains(&task.id) {
                extracted.insert(task.id, task);
            } else {
                retained.push_back(task);
            }
        }
        self.ready = retained;
        self.len -= extracted.len();

        // Output order follows the caller's selection order, independent of
        // queue order; duplicate and absent IDs are ignored.
        ids.iter().filter_map(|id| extracted.remove(id)).collect()
    }

    /// Removes one queued task by ID from either scheduling class.
    ///
    /// # Parameters
    ///
    /// * `id` - Identity of the task to remove.
    ///
    /// # Returns
    ///
    /// Whether a matching queued task was removed.
    pub(crate) fn remove(&mut self, id: TaskId) -> bool {
        if let Some(index) = self.ready.iter().position(|task| task.id == id) {
            self.ready.remove(index);
            self.len -= 1;
            return true;
        }
        let deadline = self
            .delayed
            .iter()
            .find_map(|(deadline, tasks)| tasks.iter().any(|task| task.id == id).then_some(*deadline));
        let Some(deadline) = deadline else {
            return false;
        };
        let bucket = self.delayed.get_mut(&deadline).expect("matching deadline exists");
        let index = bucket
            .iter()
            .position(|task| task.id == id)
            .expect("matching task exists");
        bucket.remove(index);
        if bucket.is_empty() {
            self.delayed.remove(&deadline);
        }
        self.len -= 1;
        true
    }

    /// Returns the earliest pending retry deadline.
    ///
    /// # Returns
    ///
    /// The smallest delayed retry timestamp, or `None` when no delayed task
    /// remains.
    #[must_use]
    #[inline]
    pub(crate) fn next_deadline(&self) -> Option<u64> {
        self.delayed.first_key_value().map(|(deadline, _)| *deadline)
    }

    /// Returns whether no ready or delayed tasks are retained.
    ///
    /// # Returns
    ///
    /// Whether the combined queue length is zero.
    #[must_use]
    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the number of tasks in both scheduling classes.
    ///
    /// # Returns
    ///
    /// The combined number of ready and delayed tasks.
    #[allow(dead_code)]
    #[must_use]
    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.len
    }
}

#[cfg(test)]
mod tests {
    use super::SchedulerQueue;
    use crate::model::TaskId;
    use crate::scheduling::QueuedTask;

    /// Builds a queue item for scheduler queue tests.
    fn task(retry_not_before_ms: Option<u64>) -> QueuedTask {
        QueuedTask {
            id: TaskId::generate(),
            resources: Default::default(),
            retry_not_before_ms,
            bypasses: 0,
        }
    }

    /// Bounds one scheduling window despite a large delayed population.
    #[test]
    fn test_take_window_skips_far_future_tasks_and_respects_budget() {
        let mut queue = SchedulerQueue::new();
        for deadline in 1_000..2_000 {
            queue.push(task(Some(deadline)));
        }
        let ready = task(None);
        let ready_id = ready.id;
        queue.push(ready);
        let window = queue.take_window(1, 10);
        assert_eq!(window.len(), 1);
        assert_eq!(window[0].id, ready_id);
        assert_eq!(queue.len(), 1_000);
        assert_eq!(queue.next_deadline(), Some(1_000));
    }

    /// Restores a ready window in its original order and removes delayed IDs.
    #[test]
    fn test_restore_and_remove_preserve_queue_accounting() {
        let mut queue = SchedulerQueue::new();
        let first = task(None);
        let first_id = first.id;
        let second = task(None);
        let second_id = second.id;
        let delayed = task(Some(50));
        let delayed_id = delayed.id;
        queue.push(first);
        queue.push(second);
        queue.push(delayed);
        let window = queue.take_window(2, 0);
        queue.restore_front(window);
        assert_eq!(
            queue.take_window(2, 0).iter().map(|task| task.id).collect::<Vec<_>>(),
            vec![first_id, second_id]
        );
        assert!(queue.remove(delayed_id));
        assert!(!queue.remove(delayed_id));
        assert_eq!(queue.len(), 0);
        assert!(queue.is_empty());
    }

    /// Moves an unstarted window behind tasks that have not been inspected.
    #[test]
    fn test_restore_back_advances_to_later_candidates() {
        let mut queue = SchedulerQueue::new();
        let first = task(None);
        let first_id = first.id;
        let second = task(None);
        let second_id = second.id;
        let later = task(None);
        let later_id = later.id;
        queue.push(first);
        queue.push(second);
        queue.push(later);

        let window = queue.take_window(2, 0);
        queue.restore_back(window);

        assert_eq!(queue.len(), 3);
        assert_eq!(queue.take_window(1, 0)[0].id, later_id);
        assert_eq!(
            queue.take_window(2, 0).iter().map(|item| item.id).collect::<Vec<_>>(),
            vec![first_id, second_id]
        );
    }

    /// Promotes still-queued tasks in the original order after a successful
    /// bypass.
    #[test]
    fn test_record_bypass_promotes_ids_without_changing_queue_length() {
        let mut queue = SchedulerQueue::new();
        let first = task(None);
        let first_id = first.id;
        let second = task(None);
        let second_id = second.id;
        let later = task(None);
        let later_id = later.id;
        queue.push(first);
        queue.push(second);
        queue.push(later);

        let skipped = queue.take_window(2, 0);
        queue.restore_back(skipped);
        let launched = queue.take_window(1, 0);
        assert_eq!(launched[0].id, later_id);
        queue.record_bypass_and_promote_front(&[first_id, second_id]);

        let promoted = queue.take_window(2, 0);
        assert_eq!(
            promoted.iter().map(|item| item.id).collect::<Vec<_>>(),
            vec![first_id, second_id]
        );
        assert_eq!(promoted[0].bypasses, 1);
        assert_eq!(promoted[1].bypasses, 1);
        assert_eq!(queue.len(), 0);
    }
}
