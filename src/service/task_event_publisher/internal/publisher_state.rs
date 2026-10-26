// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared state and the sequential outbox publication loop.
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::model::AdmissionRequirement;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::Topic;
use tokio::sync::Notify;
use tokio::sync::mpsc;

use crate::event::TaskEvent;
use crate::service::NotificationFailure;
use crate::service::TaskServiceError;
use crate::service::task_event_publisher::QuarantineCommand;
use crate::service::task_event_publisher::internal::Counters;
use crate::store::EventOutboxEntry;
use crate::store::TaskStore;

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
    /// Process-local lifecycle publication outcomes.
    pub(in crate::service::task_event_publisher) counters: Counters,
    /// Latest publication or storage diagnostic retained for shutdown.
    pub(in crate::service::task_event_publisher) last_error: parking_lot::Mutex<Option<String>>,
    /// Current failed outbox head exposed to service callers.
    pub(in crate::service::task_event_publisher) failure: parking_lot::Mutex<Option<NotificationFailure>>,
    /// Event currently awaiting admission, retained if shutdown aborts it.
    pub(in crate::service::task_event_publisher) in_flight_event_id: parking_lot::Mutex<Option<String>>,
}

impl PublisherState {
    /// Creates shared state for one owned publisher worker.
    #[must_use]
    pub(in crate::service::task_event_publisher) fn new(
        store: Arc<dyn TaskStore>,
        bus: Arc<AsyncEventBus>,
        topic: Topic<TaskEvent>,
    ) -> Self {
        Self {
            store,
            bus,
            topic,
            changed: Notify::new(),
            close_changed: Notify::new(),
            closing: AtomicBool::new(false),
            pending: AtomicUsize::new(0),
            counters: Counters::default(),
            last_error: parking_lot::Mutex::new(None),
            failure: parking_lot::Mutex::new(None),
            in_flight_event_id: parking_lot::Mutex::new(None),
        }
    }

    /// Publishes in stable store order, retaining the failed head on every
    /// error. Polling supplements notifications to cover cancellation after
    /// transaction commit.
    ///
    /// # Parameters
    ///
    /// - `controls`: Receives quarantine requests from the owning service. The
    ///   publisher worker receives this channel from the service that starts
    ///   its lifecycle.
    pub(in crate::service::task_event_publisher) async fn run(&self, mut controls: mpsc::Receiver<QuarantineCommand>) {
        let mut backoff = Duration::from_millis(25);
        loop {
            let _ = self.process_queued_controls(&mut controls).await;
            let page = self.store.list_event_outbox(128).await;
            let mut failed = false;
            match page {
                Ok(entries) if entries.is_empty() => {
                    self.pending.store(0, Ordering::Release);
                    if self.closing.load(Ordering::Acquire) {
                        if self.process_queued_controls(&mut controls).await {
                            continue;
                        }
                        return;
                    }
                    backoff = Duration::from_millis(25);
                    tokio::select! {
                        _ = self.changed.notified() => {},
                        command = controls.recv() => {
                            if let Some(command) = command { let _ = self.process_control(command).await; }
                        },
                        _ = tokio::time::sleep(Duration::from_secs(1)) => {},
                    }
                }
                Ok(entries) => {
                    self.pending.store(entries.len(), Ordering::Release);
                    for entry in entries {
                        self.clear_failure_if_head_changed(&entry);
                        if self.process_queued_controls(&mut controls).await {
                            backoff = Duration::from_millis(25);
                            break;
                        }
                        if let Err(error) = self.publish(&entry).await {
                            self.counters.failed.fetch_add(1, Ordering::Relaxed);
                            *self.last_error.lock() = Some(error.clone());
                            *self.failure.lock() = Some(NotificationFailure {
                                task_id: entry.task_id,
                                state_version: entry.state_version,
                                event_id: entry.event_id.clone(),
                                error,
                            });
                            failed = true;
                            break;
                        }
                        self.clear_failure_for(&entry);
                        self.pending.fetch_sub(1, Ordering::AcqRel);
                    }
                    if !failed {
                        backoff = Duration::from_millis(25);
                        continue;
                    }
                    if self.process_queued_controls(&mut controls).await {
                        backoff = Duration::from_millis(25);
                        continue;
                    }
                }
                Err(error) => {
                    self.counters.failed.fetch_add(1, Ordering::Relaxed);
                    *self.last_error.lock() = Some(error.to_string());
                    failed = true;
                }
            }
            let wait = if failed { backoff } else { Duration::from_secs(1) };
            // Notifications wake idle reads. Failures retain their finite backoff even
            // under a sustained stream of new commits, avoiding an outage retry storm.
            if failed {
                if self.wait_after_failure(wait, &mut controls).await {
                    backoff = Duration::from_millis(25);
                    continue;
                }
                backoff = (backoff * 2).min(Duration::from_secs(1));
            }
        }
    }

    /// Waits for retry eligibility, a completed quarantine, or shutdown.
    ///
    /// Ordinary commit notifications do not bypass failure backoff. Returns
    /// `true` when quarantine removed the failed head and the caller should
    /// immediately drain the next entry.
    ///
    /// # Returns
    ///
    /// `true` if quarantine removed the failed head; otherwise, `false` after
    /// the retry delay or shutdown signal.
    #[must_use]
    async fn wait_after_failure(&self, delay: Duration, controls: &mut mpsc::Receiver<QuarantineCommand>) -> bool {
        let deadline = tokio::time::Instant::now() + delay;
        loop {
            tokio::select! {
                _ = self.close_changed.notified() => return false,
                command = controls.recv() => {
                    if let Some(command) = command
                        && self.process_control(command).await
                    {
                        return true;
                    }
                },
                _ = tokio::time::sleep_until(deadline) => return false,
            }
        }
    }

    /// Processes every quarantine request already queued by service callers.
    ///
    /// Returns whether at least one request successfully quarantined the
    /// current failed outbox head.
    ///
    /// # Returns
    ///
    /// `true` if at least one queued request quarantined the head; otherwise,
    /// `false`.
    #[must_use]
    async fn process_queued_controls(&self, controls: &mut mpsc::Receiver<QuarantineCommand>) -> bool {
        let mut quarantined = false;
        while let Ok(command) = controls.try_recv() {
            quarantined |= self.process_control(command).await;
        }
        quarantined
    }

    /// Applies a quarantine request only when it still identifies the failed
    /// outbox head, then sends the result to the waiting caller.
    ///
    /// Returns whether the request successfully removed that head.
    ///
    /// # Returns
    ///
    /// `true` if the request removed the failed head; otherwise, `false`.
    #[must_use]
    async fn process_control(&self, command: QuarantineCommand) -> bool {
        let expected = (
            command.expected_task_id,
            command.expected_state_version,
            command.expected_event_id.as_str(),
        );
        let result = {
            let failure = self.failure.lock().clone();
            match failure {
                Some(failure) if (failure.task_id, failure.state_version, failure.event_id.as_str()) == expected => {
                    self.store
                        .quarantine_event_outbox_head(
                            command.expected_task_id,
                            command.expected_state_version,
                            &command.expected_event_id,
                            &command.reason,
                            &failure.error,
                        )
                        .await
                        .map_err(TaskServiceError::from)
                }
                _ => Err(crate::store::StoreError::Conflict.into()),
            }
        };
        let quarantined = result.is_ok();
        if quarantined {
            *self.failure.lock() = None;
            *self.last_error.lock() = None;
        }
        let _ = command.response.send(result);
        quarantined
    }

    /// Clears the retained failure when the store now exposes a different head.
    fn clear_failure_if_head_changed(&self, entry: &EventOutboxEntry) {
        let mut failure = self.failure.lock();
        if failure.as_ref().is_some_and(|current| {
            current.task_id != entry.task_id
                || current.state_version != entry.state_version
                || current.event_id != entry.event_id
        }) {
            *failure = None;
        }
    }

    /// Clears the retained failure when it belongs to the published entry.
    fn clear_failure_for(&self, entry: &EventOutboxEntry) {
        let mut failure = self.failure.lock();
        if failure.as_ref().is_some_and(|current| {
            current.task_id == entry.task_id
                && current.state_version == entry.state_version
                && current.event_id == entry.event_id
        }) {
            *failure = None;
        }
    }

    /// Publishes one immutable snapshot and removes it only after confirmed
    /// admission. Any decoding, publication, or deletion error leaves the
    /// row available for replay.
    ///
    /// # Errors
    ///
    /// Returns an error if the stored event cannot be decoded, the bus rejects
    /// admission, or the store cannot mark the event as published. In each
    /// case, the outbox row remains available for replay.
    async fn publish(&self, entry: &EventOutboxEntry) -> Result<(), String> {
        let event: TaskEvent = serde_json::from_str(&entry.event_json).map_err(|error| error.to_string())?;
        let event_id = EventId::new(&entry.event_id).map_err(|error| error.to_string())?;
        let request = PublishRequest::builder()
            .topic(self.topic.clone())
            .payload(event)
            .event_id(event_id)
            .build()
            .map_err(|error| error.to_string())?;
        *self.in_flight_event_id.lock() = Some(entry.event_id.clone());
        let publish_result = self
            .bus
            .publish_checked(request, AdmissionRequirement::ProviderOrDestinationAccepted)
            .await;
        *self.in_flight_event_id.lock() = None;
        let _receipt =
            publish_result.map_err(|error| format!("publication for event {} failed: {error}", entry.event_id))?;
        self.store
            .mark_event_published(entry.task_id, entry.state_version)
            .await
            .map_err(|error| error.to_string())?;
        self.counters.published.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use futures::poll;
    use qubit_event_bus::AsyncEventBusRegistry;
    use qubit_event_bus::EventBusConfig;
    use qubit_event_bus::model::Topic;
    use tokio::sync::mpsc;

    use super::PublisherState;
    use crate::model::typed::TaskId;
    use crate::store::EventOutboxEntry;
    use crate::store::MemoryTaskStore;
    #[cfg(feature = "sqlite")]
    use crate::store::SqliteTaskStore;
    #[cfg(feature = "sqlite")]
    use crate::store::TaskStore;

    #[tokio::test]
    async fn test_failure_backoff_consumes_only_one_close_signal() {
        let registry = AsyncEventBusRegistry::with_local().expect("local registry");
        let bus = Arc::new(registry.create(&EventBusConfig::default()).await.expect("local bus"));
        let state = PublisherState::new(
            Arc::new(MemoryTaskStore::new(1)),
            bus,
            Topic::new("task.lifecycle").expect("topic"),
        );
        let (_sender, mut controls) = mpsc::channel(4);
        let mut first = Box::pin(state.wait_after_failure(Duration::from_secs(3600), &mut controls));
        assert!(poll!(first.as_mut()).is_pending());
        state.changed.notify_one();
        assert!(
            poll!(first.as_mut()).is_pending(),
            "commits cannot bypass failure backoff"
        );
        state.close_changed.notify_one();
        assert!(
            poll!(first.as_mut()).is_ready(),
            "close must interrupt a long retry wait"
        );
        drop(first);
        let mut second = Box::pin(state.wait_after_failure(Duration::from_secs(3600), &mut controls));
        assert!(
            poll!(second.as_mut()).is_pending(),
            "sustained errors remain rate limited after the close retry"
        );
    }

    #[tokio::test]
    async fn test_publish_rejects_corrupt_outbox_snapshot_before_bus_admission() {
        let registry = AsyncEventBusRegistry::with_local().expect("local registry");
        let bus = Arc::new(registry.create(&EventBusConfig::default()).await.expect("local bus"));
        let state = PublisherState::new(
            Arc::new(MemoryTaskStore::new(1)),
            bus,
            Topic::new("task.lifecycle").expect("topic"),
        );
        let entry = EventOutboxEntry {
            task_id: TaskId::from_id(qubit_id::Id::new(77)),
            state_version: 4,
            event_id: "task:77:4".into(),
            event_json: "{corrupt".into(),
        };

        let error = state
            .publish(&entry)
            .await
            .expect_err("corrupt snapshots cannot publish");

        assert!(!error.is_empty());
        assert_eq!(*state.in_flight_event_id.lock(), None);
        assert_eq!(state.counters.snapshot().published, 0);
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn test_run_exits_after_close_when_the_outbox_is_empty() {
        let database = std::env::temp_dir().join(format!("task-publisher-state-{}.sqlite", uuid::Uuid::new_v4()));
        let store = Arc::new(SqliteTaskStore::open(&database).expect("SQLite store opens"));
        let owner = store.acquire_owner().await.expect("store owner is acquired");
        store.enable_event_outbox().await.expect("durable outbox is enabled");
        let registry = AsyncEventBusRegistry::with_local().expect("local registry");
        let bus = Arc::new(registry.create(&EventBusConfig::default()).await.expect("local bus"));
        let state = PublisherState::new(store.clone(), bus, Topic::new("task.lifecycle").expect("topic"));
        state.closing.store(true, std::sync::atomic::Ordering::Release);

        let (_sender, receiver) = mpsc::channel(4);
        tokio::time::timeout(Duration::from_secs(1), state.run(receiver))
            .await
            .expect("closed empty publisher exits promptly");

        assert_eq!(state.pending.load(std::sync::atomic::Ordering::Acquire), 0);
        store.release_owner(owner).await.expect("store owner is released");
        drop(state);
        drop(store);
        std::fs::remove_file(database.with_file_name(format!(
            "{}.owner.lock",
            database.file_name().expect("database has a filename").to_string_lossy()
        )))
        .expect("owner lock file is removed");
        std::fs::remove_file(database).expect("database file is removed");
    }
}
