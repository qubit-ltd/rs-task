// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
// =============================================================================
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::Topic;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::event::TaskEvent;

pub(super) struct TaskEventConfig {
    pub(super) bus: Arc<AsyncEventBus>,
    pub(super) topic: Topic<TaskEvent>,
    pub(super) capacity: std::num::NonZeroUsize,
    pub(super) flush_timeout: Duration,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NotificationStats {
    pub queued: u64,
    pub published: u64,
    pub dropped: u64,
    pub failed: u64,
}

#[derive(Default)]
struct Counters {
    queued: AtomicU64,
    published: AtomicU64,
    dropped: AtomicU64,
    failed: AtomicU64,
    pending: AtomicU64,
}

pub(super) struct TaskEventDispatcher {
    sender: parking_lot::Mutex<Option<mpsc::Sender<TaskEvent>>>,
    counters: Arc<Counters>,
    worker: tokio::sync::Mutex<Option<JoinHandle<()>>>,
    flush_timeout: Duration,
}

impl TaskEventDispatcher {
    pub(super) fn new(
        bus: Arc<AsyncEventBus>,
        topic: Topic<TaskEvent>,
        capacity: std::num::NonZeroUsize,
        flush_timeout: Duration,
    ) -> Arc<Self> {
        let (sender, mut receiver) = mpsc::channel(capacity.get());
        let counters = Arc::new(Counters::default());
        let worker_counters = Arc::clone(&counters);
        let worker = tokio::spawn(async move {
            while let Some(event) = receiver.recv().await {
                match PublishRequest::new(topic.clone(), event) {
                    Ok(request) => {
                        if bus.publish(request).await.is_ok() {
                            worker_counters.published.fetch_add(1, Ordering::Relaxed);
                        } else {
                            worker_counters.failed.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Err(_) => {
                        worker_counters.failed.fetch_add(1, Ordering::Relaxed);
                    }
                };
                worker_counters.pending.fetch_sub(1, Ordering::AcqRel);
            }
        });
        Arc::new(Self {
            sender: parking_lot::Mutex::new(Some(sender)),
            counters,
            worker: tokio::sync::Mutex::new(Some(worker)),
            flush_timeout,
        })
    }

    pub(super) fn enqueue(&self, event: TaskEvent) {
        let sender = self.sender.lock();
        let Some(sender) = sender.as_ref() else {
            self.counters.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        };
        self.counters.pending.fetch_add(1, Ordering::AcqRel);
        match sender.try_send(event) {
            Ok(()) => {
                self.counters.queued.fetch_add(1, Ordering::Relaxed);
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.counters.pending.fetch_sub(1, Ordering::AcqRel);
                self.counters.dropped.fetch_add(1, Ordering::Relaxed);
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.counters.pending.fetch_sub(1, Ordering::AcqRel);
                self.counters.failed.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub(super) fn stats(&self) -> NotificationStats {
        NotificationStats {
            queued: self.counters.queued.load(Ordering::Relaxed),
            published: self.counters.published.load(Ordering::Relaxed),
            dropped: self.counters.dropped.load(Ordering::Relaxed),
            failed: self.counters.failed.load(Ordering::Relaxed),
        }
    }

    pub(super) async fn shutdown(&self) {
        self.sender.lock().take();
        let Some(mut worker) = self.worker.lock().await.take() else {
            return;
        };
        if tokio::time::timeout(self.flush_timeout, &mut worker).await.is_err() {
            worker.abort();
            let _ = worker.await;
            let pending = self.counters.pending.swap(0, Ordering::AcqRel);
            self.counters.dropped.fetch_add(pending, Ordering::Relaxed);
        } else if self.counters.pending.load(Ordering::Acquire) > 0 {
            let pending = self.counters.pending.swap(0, Ordering::AcqRel);
            self.counters.dropped.fetch_add(pending, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::TaskState;

    fn event() -> TaskEvent {
        TaskEvent {
            schema_version: 1,
            task_id: crate::model::next::TaskId::from_id(qubit_id::Id::new(1)),
            state_version: 1,
            state: TaskState::Queued,
            correlation_key: None,
        }
    }

    #[tokio::test]
    async fn full_queue_drops_without_blocking_and_shutdown_flushes_accepted_event() {
        let bus = Arc::new(AsyncEventBus::local(Default::default()).await.unwrap());
        let dispatcher = TaskEventDispatcher::new(
            bus,
            Topic::new("task.test").unwrap(),
            std::num::NonZeroUsize::new(1).unwrap(),
            Duration::from_secs(1),
        );
        dispatcher.enqueue(event());
        dispatcher.enqueue(event());
        assert_eq!(dispatcher.stats().queued, 1);
        assert_eq!(dispatcher.stats().dropped, 1);
        dispatcher.shutdown().await;
        assert_eq!(dispatcher.stats().published, 1);
        assert_eq!(dispatcher.stats().failed, 0);
    }

    #[tokio::test]
    async fn shutdown_timeout_counts_queued_events_as_dropped() {
        let bus = Arc::new(AsyncEventBus::local(Default::default()).await.unwrap());
        let dispatcher = TaskEventDispatcher::new(
            bus,
            Topic::new("task.timeout").unwrap(),
            std::num::NonZeroUsize::new(2).unwrap(),
            Duration::ZERO,
        );
        let old_worker = dispatcher.worker.lock().await.take().unwrap();
        old_worker.abort();
        let _ = old_worker.await;
        dispatcher.counters.pending.store(1, Ordering::Release);
        dispatcher.counters.queued.store(1, Ordering::Release);
        *dispatcher.worker.lock().await = Some(tokio::spawn(std::future::pending::<()>()));
        dispatcher.shutdown().await;
        assert_eq!(dispatcher.stats().queued, 1);
        assert_eq!(dispatcher.stats().dropped, 1);
    }
}
