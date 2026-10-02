//! Shared state and the sequential outbox publication loop.
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::model::{AdmissionOutcome, AdmissionRequirement, EventId, PublishRequest, Topic};
use tokio::sync::Notify;
use crate::event::TaskEvent;
use crate::store::{EventOutboxEntry, TaskStore};

/// Values shared with the worker without keeping its join handle alive.
pub(in crate::service::task_event_publisher) struct PublisherState {
    /// Persistent outbox storage guarded by the service owner.
    pub(in crate::service::task_event_publisher) store: Arc<dyn TaskStore>,
    /// Caller-owned facade; stopping this worker does not shut down the bus.
    pub(in crate::service::task_event_publisher) bus: Arc<AsyncEventBus>,
    /// Validated lifecycle event destination.
    pub(in crate::service::task_event_publisher) topic: Topic<TaskEvent>,
    /// Wake signal for new committed rows.
    pub(in crate::service::task_event_publisher) changed: Notify,
    /// One-shot signal interrupting failure backoff when shutdown first starts.
    pub(in crate::service::task_event_publisher) close_changed: Notify,
    /// Requests draining followed by worker exit.
    pub(in crate::service::task_event_publisher) closing: AtomicBool,
    /// Last observed bounded page count, adjusted after confirmed deletion.
    pub(in crate::service::task_event_publisher) pending: AtomicUsize,
    /// Latest publication or storage diagnostic retained for shutdown.
    pub(in crate::service::task_event_publisher) last_error: parking_lot::Mutex<Option<String>>,
}

impl PublisherState {
    /// Creates shared state for one owned publisher worker.
    pub(in crate::service::task_event_publisher) fn new(store: Arc<dyn TaskStore>, bus: Arc<AsyncEventBus>, topic: Topic<TaskEvent>) -> Self {
        Self { store, bus, topic, changed: Notify::new(), close_changed: Notify::new(), closing: AtomicBool::new(false), pending: AtomicUsize::new(0), last_error: parking_lot::Mutex::new(None) }
    }

    /// Publishes in stable store order, retaining the failed head on every error.
    /// Polling supplements notifications to cover cancellation after transaction commit.
    pub(in crate::service::task_event_publisher) async fn run(&self) {
        let mut backoff = Duration::from_millis(25);
        loop {
            let page = self.store.list_event_outbox(128).await;
            let mut failed = false;
            match page {
                Ok(entries) if entries.is_empty() => {
                    self.pending.store(0, Ordering::Release);
                    if self.closing.load(Ordering::Acquire) { return; }
                    backoff = Duration::from_millis(25);
                }
                Ok(entries) => {
                    self.pending.store(entries.len(), Ordering::Release);
                    for entry in entries {
                        if let Err(error) = self.publish(&entry).await { *self.last_error.lock() = Some(error); failed = true; break; }
                        self.pending.fetch_sub(1, Ordering::AcqRel);
                    }
                    if !failed { backoff = Duration::from_millis(25); continue; }
                }
                Err(error) => { *self.last_error.lock() = Some(error.to_string()); failed = true; },
            }
            let wait = if failed { backoff } else { Duration::from_secs(1) };
            // Notifications wake idle reads. Failures retain their finite backoff even
            // under a sustained stream of new commits, avoiding an outage retry storm.
            if failed {
                self.wait_after_failure(wait).await;
                backoff = (backoff * 2).min(Duration::from_secs(1));
            } else {
                tokio::select! { _ = self.changed.notified() => {}, _ = tokio::time::sleep(wait) => {} }
            }
        }
    }

    /// Waits for retry eligibility, allowing the first close signal one immediate
    /// drain attempt. Ordinary commit notifications never bypass failure backoff.
    async fn wait_after_failure(&self, delay: Duration) {
        tokio::select! {
            _ = self.close_changed.notified() => {},
            _ = tokio::time::sleep(delay) => {},
        }
    }

    /// Publishes one immutable snapshot and removes it only after confirmed admission.
    /// Any decoding, publication, or deletion error leaves the row available for replay.
    async fn publish(&self, entry: &EventOutboxEntry) -> Result<(), String> {
        let event: TaskEvent = serde_json::from_str(&entry.event_json).map_err(|error| error.to_string())?;
        let event_id = EventId::new(&entry.event_id).map_err(|error| error.to_string())?;
        let request = PublishRequest::builder().topic(self.topic.clone()).payload(event).event_id(event_id).build().map_err(|error| error.to_string())?;
        let receipt = self.bus.publish(request).await.map_err(|error| error.to_string())?;
        if receipt.admission_outcome() != AdmissionOutcome::OpaqueAccepted {
            receipt.check_admission(AdmissionRequirement::AtLeastOneAccepted).map_err(|error| error.to_string())?;
        }
        self.store.mark_event_published(entry.task_id, entry.state_version).await.map_err(|error| error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;
    use futures::poll;
    use qubit_event_bus::{AsyncEventBusRegistry, EventBusConfig};
    use qubit_event_bus::model::Topic;
    use crate::store::MemoryTaskStore;
    use super::PublisherState;

    #[tokio::test]
    async fn test_failure_backoff_consumes_only_one_close_signal() {
        let registry = AsyncEventBusRegistry::with_local().expect("local registry");
        let bus = Arc::new(registry.create(&EventBusConfig::default()).await.expect("local bus"));
        let state = PublisherState::new(Arc::new(MemoryTaskStore::new(1)), bus, Topic::new("task.lifecycle").expect("topic"));
        let mut first = Box::pin(state.wait_after_failure(Duration::from_secs(3600)));
        assert!(poll!(first.as_mut()).is_pending());
        state.changed.notify_one();
        assert!(poll!(first.as_mut()).is_pending(), "commits cannot bypass failure backoff");
        state.close_changed.notify_one();
        assert!(poll!(first.as_mut()).is_ready(), "close must interrupt a long retry wait");
        let mut second = Box::pin(state.wait_after_failure(Duration::from_secs(3600)));
        assert!(poll!(second.as_mut()).is_pending(), "sustained errors remain rate limited after the close retry");
    }
}
