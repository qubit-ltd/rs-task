// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
// =============================================================================
use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::Notify;

use crate::model::TaskId;

#[derive(Default)]
pub(super) struct TaskWaitRegistry {
    entries: Mutex<HashMap<TaskId, Entry>>,
}
struct Entry {
    notify: Arc<Notify>,
    subscribers: usize,
}
pub(super) struct WaitSubscription {
    registry: Arc<TaskWaitRegistry>,
    id: TaskId,
    notify: Arc<Notify>,
}
impl TaskWaitRegistry {
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
    pub(super) fn notify(&self, id: TaskId) {
        let notify = self.entries.lock().get(&id).map(|entry| entry.notify.clone());
        if let Some(notify) = notify {
            notify.notify_waiters();
        }
    }
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
    pub fn notified(&self) -> tokio::sync::futures::Notified<'_> {
        self.notify.notified()
    }
}
impl Drop for WaitSubscription {
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

    use super::TaskWaitRegistry;
    use crate::model::TaskId;
    #[tokio::test]
    async fn subscriptions_are_scoped_and_removed_on_drop() {
        let registry = Arc::new(TaskWaitRegistry::default());
        let a = TaskId::generate();
        let b = TaskId::generate();
        let sub_a = registry.subscribe(a);
        let sub_a_second = registry.subscribe(a);
        let sub_b = registry.subscribe(b);
        {
            let notified_a = sub_a.notified();
            tokio::pin!(notified_a);
            notified_a.as_mut().enable();
            let notified_a_second = sub_a_second.notified();
            tokio::pin!(notified_a_second);
            notified_a_second.as_mut().enable();
            let notified_b = sub_b.notified();
            tokio::pin!(notified_b);
            notified_b.as_mut().enable();
            registry.notify(a);
            tokio::time::timeout(std::time::Duration::from_millis(50), &mut notified_a)
                .await
                .unwrap();
            tokio::time::timeout(std::time::Duration::from_millis(50), &mut notified_a_second)
                .await
                .unwrap();
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(10), &mut notified_b)
                    .await
                    .is_err()
            );
            registry.notify_all();
            tokio::time::timeout(std::time::Duration::from_millis(50), &mut notified_b)
                .await
                .unwrap();
        }
        drop(sub_a);
        assert!(registry.entries.lock().contains_key(&a));
        drop(sub_a_second);
        assert!(!registry.entries.lock().contains_key(&a));
    }
}
