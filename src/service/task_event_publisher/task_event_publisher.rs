// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Ordered lifecycle notification publication and shutdown.

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

use super::QuarantineCommand;
use super::internal::PublisherState;
use crate::model::typed::TaskId;
use crate::service::NotificationFailure;
use crate::service::NotificationStats;
use crate::service::TaskServiceError;
use crate::store::TaskStore;

/// Owns one ordered outbox worker and its bounded shutdown barrier.
pub(in crate::service) struct TaskEventPublisher {
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
    /// starting network IO.
    ///
    /// # Parameters
    ///
    /// - `store`: durable task store whose outbox the worker will publish.
    /// - `bus`: event bus used to publish lifecycle notifications.
    /// - `shutdown_timeout`: maximum time allowed for draining during shutdown.
    ///
    /// # Returns
    ///
    /// The configured publisher, ready to start after service recovery.
    ///
    /// # Errors
    ///
    /// Returns `InvalidRequest` if the lifecycle topic is invalid, or
    /// `NotificationCodecUnavailable` if the bus cannot encode its events.
    pub(in crate::service) fn new(
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

    /// Starts initial replay after recovery succeeds.
    ///
    /// Requires an active Tokio runtime to spawn the worker. Calling this more
    /// than once leaves the existing worker running.
    ///
    /// # Panics
    ///
    /// Panics if called outside a Tokio runtime or if the worker receiver has
    /// already been taken without a worker being installed.
    pub(in crate::service) async fn start(&self) {
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
    pub(in crate::service) fn notify(&self) {
        self.state.counters.queued.fetch_add(1, Ordering::Relaxed);
        self.state.changed.notify_one();
    }

    /// Returns the process-local notification counters for this publisher.
    ///
    /// The snapshot excludes durable outbox backlog that may remain in the
    /// store, including events left by a previous process.
    ///
    /// # Returns
    ///
    /// A copy of this process's queued, published, failed, and dropped counts.
    #[inline]
    pub(in crate::service) fn stats(&self) -> NotificationStats {
        self.state.counters.snapshot()
    }

    /// Returns a snapshot identifying the failed outbox head, if one exists.
    ///
    /// # Returns
    ///
    /// The latest failed event's task, version, and ID, or `None` if the worker
    /// has not recorded a failed head.
    pub(in crate::service) fn failure(&self) -> Option<NotificationFailure> {
        self.state.failure.lock().clone()
    }

    /// Queues explicit isolation of the exact currently failed outbox head.
    ///
    /// # Parameters
    ///
    /// - `expected_task_id`, `expected_state_version`, and `expected_event_id`:
    ///   identity of the failed outbox head that may be isolated.
    /// - `reason`: non-blank explanation of the operator's decision, limited to
    ///   4096 UTF-8 bytes.
    /// - `service_shutting_down`: flag that rejects commands during shutdown.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` after the worker has isolated the matching failed
    /// outbox event.
    ///
    /// # Errors
    ///
    /// Returns `InvalidRequest` for an invalid reason or a full control queue,
    /// `ShuttingDown` during shutdown, a store error if the failed head no
    /// longer matches or cannot be quarantined, or `NotificationClose` if the
    /// worker has stopped or drops the command result.
    pub(in crate::service) async fn quarantine_failed(
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
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` after the worker exits, or a shutdown diagnostic if
    /// the worker panics or the deadline expires before draining completes.
    ///
    /// # Errors
    ///
    /// Returns `NotificationClose` when the worker panics or publication does
    /// not drain before the configured deadline.
    pub(in crate::service) async fn close(&self) -> Result<(), TaskServiceError> {
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
