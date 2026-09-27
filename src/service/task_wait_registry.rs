// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
// qubit-style: allow multiple-public-types
use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::Notify;
use tokio::sync::futures::Notified;

use crate::model::TaskId;

/// Tracks task-specific wait notifications and active subscribers.
#[derive(Default)]
pub(super) struct TaskWaitRegistry {
    /// Entries retained while they have at least one subscriber.
    entries: Mutex<HashMap<TaskId, Entry>>,
}
/// Notification primitive and subscriber count for one task ID.
struct Entry {
    /// Shared wakeup source for callers waiting on this task.
    notify: Arc<Notify>,
    /// Number of live subscriptions keeping this entry registered.
    subscribers: usize,
}
/// Keeps one task notification registered until the subscription is dropped.
pub(super) struct WaitSubscription {
    /// Registry whose subscriber count this value owns.
    registry: Arc<TaskWaitRegistry>,
    /// Task whose notifications this value observes.
    id: TaskId,
    /// Shared notification primitive for this task.
    notify: Arc<Notify>,
}
impl TaskWaitRegistry {
    /// Registers a subscriber and returns its task-specific notification
    /// handle.
    ///
    /// # Parameters
    ///
    /// * `id` - Task identity whose state changes should wake this subscriber.
    ///
    /// # Returns
    ///
    /// A subscription that unregisters when dropped.
    pub(super) fn subscribe(self: &Arc<Self>, id: TaskId) -> WaitSubscription {
        let notify = {
            let mut entries = self.entries.lock();
            let entry = entries.entry(id).or_insert_with(|| Entry {
                notify: Arc::new(Notify::new()),
                subscribers: 0,
            });
            entry.subscribers += 1;
            entry.notify.clone()
        };
        WaitSubscription {
            registry: self.clone(),
            id,
            notify,
        }
    }
    /// Wakes all current subscribers for one task ID.
    ///
    /// # Parameters
    ///
    /// * `id` - Task identity whose current subscribers are notified.
    pub(super) fn notify(&self, id: TaskId) {
        let notify = self.entries.lock().get(&id).map(|entry| entry.notify.clone());
        if let Some(notify) = notify {
            notify.notify_waiters();
        }
    }
    /// Wakes subscribers for every task currently present in the registry.
    pub(super) fn notify_all(&self) {
        let notifies = self
            .entries
            .lock()
            .values()
            .map(|entry| entry.notify.clone())
            .collect::<Vec<_>>();
        for notify in notifies {
            notify.notify_waiters();
        }
    }
}
impl WaitSubscription {
    /// Creates the notification future used to await a task update.
    ///
    /// # Returns
    ///
    /// A future that completes after the next notification for this task.
    #[must_use]
    #[inline]
    pub fn notified(&self) -> Notified<'_> {
        self.notify.notified()
    }
}
impl Drop for WaitSubscription {
    /// Removes the registry entry after its final subscriber is gone.
    fn drop(&mut self) {
        let mut entries = self.registry.entries.lock();
        if let Some(entry) = entries.get_mut(&self.id) {
            entry.subscribers -= 1;
            if entry.subscribers == 0 {
                entries.remove(&self.id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::pin;
    use tokio::test as tokio_test;
    use tokio::time;

    use super::TaskWaitRegistry;
    use crate::model::TaskId;
    #[tokio_test]
    async fn test_subscriptions_are_scoped_and_removed_on_drop() {
        let registry = Arc::new(TaskWaitRegistry::default());
        let a = TaskId::generate();
        let b = TaskId::generate();
        let sub_a = registry.subscribe(a);
        let sub_a_second = registry.subscribe(a);
        let sub_b = registry.subscribe(b);
        {
            let notified_a = sub_a.notified();
            pin!(notified_a);
            notified_a.as_mut().enable();
            let notified_a_second = sub_a_second.notified();
            pin!(notified_a_second);
            notified_a_second.as_mut().enable();
            let notified_b = sub_b.notified();
            pin!(notified_b);
            notified_b.as_mut().enable();
            registry.notify(a);
            time::timeout(Duration::from_millis(50), &mut notified_a).await.unwrap();
            time::timeout(Duration::from_millis(50), &mut notified_a_second)
                .await
                .unwrap();
            assert!(time::timeout(Duration::from_millis(10), &mut notified_b).await.is_err());
            registry.notify_all();
            time::timeout(Duration::from_millis(50), &mut notified_b).await.unwrap();
        }
        drop(sub_a);
        assert!(registry.entries.lock().contains_key(&a));
        drop(sub_a_second);
        assert!(!registry.entries.lock().contains_key(&a));
    }
}
