use std::collections::BTreeMap;
use std::collections::VecDeque;

use crate::model::TaskId;
use crate::scheduling::QueuedTask;

/// Holds ready work separately from tasks waiting for retry deadlines.
pub(crate) struct SchedulerQueue {
    ready: VecDeque<QueuedTask>,
    delayed: BTreeMap<u64, VecDeque<QueuedTask>>,
    len: usize,
}

impl SchedulerQueue {
    /// Creates an empty scheduler queue.
    pub(crate) fn new() -> Self {
        Self {
            ready: VecDeque::new(),
            delayed: BTreeMap::new(),
            len: 0,
        }
    }

    /// Adds a task to the ready queue or its retry deadline bucket.
    pub(crate) fn push(&mut self, task: QueuedTask) {
        self.len += 1;
        if let Some(deadline) = task.retry_not_before_ms {
            self.delayed.entry(deadline).or_default().push_back(task);
        } else {
            self.ready.push_back(task);
        }
    }

    /// Takes at most `budget` due tasks in FIFO order for one scheduling pass.
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

    /// Removes one queued task by ID from either scheduling class.
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
    pub(crate) fn next_deadline(&self) -> Option<u64> {
        self.delayed.first_key_value().map(|(deadline, _)| *deadline)
    }

    /// Returns whether no ready or delayed tasks are retained.
    pub(crate) fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the number of tasks in both scheduling classes.
    #[allow(dead_code)]
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
    fn take_window_skips_far_future_tasks_and_respects_budget() {
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
    fn restore_and_remove_preserve_queue_accounting() {
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
}
