// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Sequential durable lifecycle notification publication.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::model::Topic;
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::model::typed::TaskId;
use crate::service::NotificationFailure;
use crate::service::NotificationStats;
use crate::service::TaskServiceError;
use crate::store::TaskStore;

/// Control operation serialized with outbox publication.
pub(super) struct QuarantineCommand {
    pub(super) expected_task_id: TaskId,
    pub(super) expected_state_version: u64,
    pub(super) expected_event_id: String,
    pub(super) reason: String,
    pub(super) response: oneshot::Sender<Result<(), TaskServiceError>>,
}

mod internal;
use internal::PublisherState;

/// Owns one ordered outbox worker and its bounded shutdown barrier.
pub(super) struct TaskEventPublisher {
    /// Shared store and publication state.
    state: Arc<PublisherState>,
    /// Retained join handle, including across cancelled shutdown callers.
    worker: Mutex<Option<JoinHandle<()>>>,
    /// Bounded operator-control queue consumed by the ordered worker.
    controls: mpsc::Sender<QuarantineCommand>,
    /// Receiver is moved into the worker exactly once when it starts.
    receiver: Mutex<Option<mpsc::Receiver<QuarantineCommand>>>,
    /// Serializes command acceptance against the transition to closing.
    control_gate: Mutex<()>,
    /// Maximum time reserved for notification draining.
    shutdown_timeout: Duration,
}

impl TaskEventPublisher {
    /// Validates the topic and its codec, then prepares a worker without
    /// starting network IO. Returns a configuration diagnostic when either
    /// check fails.
    pub(super) fn new(
        store: Arc<dyn TaskStore>,
        bus: Arc<AsyncEventBus>,
        shutdown_timeout: Duration,
    ) -> Result<Self, TaskServiceError> {
        let topic = Topic::new("task.lifecycle")
            .map_err(|error| TaskServiceError::InvalidRequest(format!("invalid task notification topic: {error}")))?;
        bus.check_publish_codec(&topic)
            .map_err(|source| TaskServiceError::NotificationCodecUnavailable {
                provider_id: bus.provider_id().as_str().to_owned(),
                topic: topic.name().to_owned(),
                source,
            })?;
        let (controls, receiver) = mpsc::channel(16);
        Ok(Self {
            state: Arc::new(PublisherState::new(store, bus, topic)),
            worker: Mutex::new(None),
            controls,
            receiver: Mutex::new(Some(receiver)),
            control_gate: Mutex::new(()),
            shutdown_timeout,
        })
    }

    /// Starts initial replay after recovery succeeds. Requires a Tokio runtime.
    pub(super) async fn start(&self) {
        let mut worker = self.worker.lock().await;
        if worker.is_none() {
            let state = Arc::clone(&self.state);
            let controls = self.receiver.lock().await.take().expect("worker receiver is present");
            *worker = Some(tokio::spawn(async move {
                state.run(controls).await;
            }));
        }
    }

    /// Wakes the worker after a committed lifecycle write.
    pub(super) fn notify(&self) {
        self.state.counters.queued.fetch_add(1, Ordering::Relaxed);
        self.state.changed.notify_one();
    }

    /// Returns the process-local notification counters for this publisher.
    ///
    /// The snapshot excludes durable outbox backlog that may remain in the
    /// store, including events left by a previous process.
    #[must_use]
    #[inline]
    pub(super) fn stats(&self) -> NotificationStats {
        self.state.counters.snapshot()
    }

    /// Returns a snapshot identifying the failed outbox head, if one exists.
    pub(super) fn failure(&self) -> Option<NotificationFailure> {
        self.state.failure.lock().clone()
    }

    /// Queues explicit isolation of the exact currently failed outbox head.
    pub(super) async fn quarantine_failed(
        &self,
        expected_task_id: TaskId,
        expected_state_version: u64,
        expected_event_id: &str,
        reason: &str,
        service_shutting_down: &AtomicBool,
    ) -> Result<(), TaskServiceError> {
        let reason = reason.trim();
        if reason.is_empty() {
            return Err(TaskServiceError::InvalidRequest(
                "notification quarantine reason must not be blank".into(),
            ));
        }
        if reason.len() > 4096 {
            return Err(TaskServiceError::InvalidRequest(
                "notification quarantine reason exceeds 4096 UTF-8 bytes".into(),
            ));
        }
        let _gate = self.control_gate.lock().await;
        if service_shutting_down.load(Ordering::Acquire) || self.state.closing.load(Ordering::Acquire) {
            return Err(TaskServiceError::ShuttingDown);
        }
        let (response, receiver) = oneshot::channel();
        let command = QuarantineCommand {
            expected_task_id,
            expected_state_version,
            expected_event_id: expected_event_id.to_owned(),
            reason: reason.to_owned(),
            response,
        };
        match self.controls.try_send(command) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                return Err(TaskServiceError::InvalidRequest(
                    "notification quarantine control queue is full".into(),
                ));
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                return Err(TaskServiceError::NotificationClose(
                    "notification worker stopped before accepting quarantine command".into(),
                ));
            }
        }
        drop(_gate);
        receiver.await.map_err(|error| {
            TaskServiceError::NotificationClose(format!("notification worker dropped quarantine result: {error}"))
        })?
    }

    /// Drains existing events up to the configured deadline, aborts stalled
    /// publication, and joins the worker before the service releases
    /// ownership. Cancelling this future retains its handle so a later
    /// shutdown can finish the same barrier. Returns `NotificationClose` if
    /// rows remain or the worker panics.
    pub(super) async fn close(&self) -> Result<(), TaskServiceError> {
        let _gate = self.control_gate.lock().await;
        let first_close = !self.state.closing.swap(true, Ordering::AcqRel);
        drop(_gate);
        if first_close {
            self.state.close_changed.notify_one();
        }
        self.state.changed.notify_one();
        let mut worker = self.worker.lock().await;
        let Some(handle) = worker.as_mut() else {
            return Ok(());
        };
        match tokio::time::timeout(self.shutdown_timeout, &mut *handle).await {
            Ok(result) => {
                *worker = None;
                result.map_err(|error| {
                    TaskServiceError::NotificationClose(format!("notification worker stopped: {error}"))
                })
            }
            Err(_) => {
                handle.abort();
                let _ = handle.await;
                *worker = None;
                let pending = self.state.pending.load(Ordering::Acquire);
                let diagnostic = if pending == 0 {
                    "shutdown deadline expired; pending count unavailable".to_owned()
                } else {
                    format!(
                        "shutdown deadline expired; last observed pending page contained {pending} notification(s); final backlog may differ"
                    )
                };
                let publication_diagnostic = self.state.in_flight_event_id.lock().as_deref().map_or_else(
                    || "no publication was active at timeout".to_owned(),
                    |event_id| format!("publication timed out for event {event_id}"),
                );
                Err(TaskServiceError::NotificationClose(format!(
                    "{diagnostic}; {publication_diagnostic}; last error: {:?}",
                    self.state.last_error.lock().as_deref()
                )))
            }
        }
    }
}

impl Drop for TaskEventPublisher {
    /// Prevents a detached worker from continuing after its service is dropped.
    fn drop(&mut self) {
        if let Some(handle) = self.worker.get_mut().take() {
            handle.abort();
        }
    }
}

#[cfg(all(test, feature = "sqlite"))]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use qubit_event_bus::AsyncEventBusRegistry;
    use qubit_event_bus::EventBusConfig;

    use super::TaskEventPublisher;
    use crate::store::SqliteTaskStore;
    use crate::store::TaskStore;

    async fn create_publisher() -> (TaskEventPublisher, std::path::PathBuf) {
        let database = std::env::temp_dir().join(format!("task-publisher-{}.sqlite", uuid::Uuid::new_v4()));
        let store = Arc::new(SqliteTaskStore::open(&database).expect("open temporary task store"));
        store.acquire_owner().await.expect("acquire store owner");
        store.enable_event_outbox().await.expect("enable durable outbox");
        let registry = AsyncEventBusRegistry::with_local().expect("local bus registry");
        let bus = Arc::new(
            registry
                .create(&EventBusConfig::default())
                .await
                .expect("local event bus"),
        );
        TaskEventPublisher::new(store, bus, Duration::from_secs(1))
            .map(|publisher| (publisher, database))
            .expect("publisher configuration is valid")
    }

    #[tokio::test]
    async fn test_close_without_started_worker_is_idempotent() {
        let (publisher, database) = create_publisher().await;

        publisher.close().await.expect("no-worker close succeeds");
        publisher.close().await.expect("repeated close succeeds");
        drop(publisher);
        std::fs::remove_file(database).expect("remove temporary task database");
    }

    #[tokio::test]
    async fn test_start_is_idempotent_and_close_drains_empty_outbox() {
        let (publisher, database) = create_publisher().await;

        publisher.start().await;
        publisher.start().await;
        publisher.notify();
        assert_eq!(publisher.stats().queued, 1);
        assert_eq!(publisher.stats().published, 0);
        assert_eq!(publisher.stats().failed, 0);
        assert_eq!(publisher.stats().dropped, 0);

        publisher.close().await.expect("worker exits after empty outbox");
        publisher.close().await.expect("joined worker is not joined twice");
        drop(publisher);
        std::fs::remove_file(database).expect("remove temporary task database");
    }

    #[tokio::test]
    async fn test_close_reports_outbox_read_failure_at_deadline() {
        let (publisher, database) = create_publisher().await;
        rusqlite::Connection::open(&database)
            .expect("open database for fault injection")
            .execute_batch("DROP TABLE task_event_outbox")
            .expect("remove the outbox table");

        publisher.start().await;
        let error = publisher
            .close()
            .await
            .expect_err("outbox read keeps failing until deadline");
        let diagnostic = error.to_string();
        assert!(diagnostic.contains("pending count unavailable"), "{diagnostic}");
        assert!(
            diagnostic.contains("no publication was active at timeout"),
            "{diagnostic}"
        );
        assert!(diagnostic.contains("task_event_outbox"), "{diagnostic}");
        drop(publisher);
        std::fs::remove_file(database).expect("remove temporary task database");
    }
}
