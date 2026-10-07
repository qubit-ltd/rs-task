// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Sequential durable lifecycle notification publication.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::model::Topic;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use crate::service::NotificationStats;
use crate::service::TaskServiceError;
use crate::store::TaskStore;

mod internal;
use internal::PublisherState;

/// Owns one ordered outbox worker and its bounded shutdown barrier.
pub(super) struct TaskEventPublisher {
    /// Shared store and publication state.
    state: Arc<PublisherState>,
    /// Retained join handle, including across cancelled shutdown callers.
    worker: Mutex<Option<JoinHandle<()>>>,
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
        Ok(Self {
            state: Arc::new(PublisherState::new(store, bus, topic)),
            worker: Mutex::new(None),
            shutdown_timeout,
        })
    }

    /// Starts initial replay after recovery succeeds. Requires a Tokio runtime.
    pub(super) async fn start(&self) {
        let mut worker = self.worker.lock().await;
        if worker.is_none() {
            let state = Arc::clone(&self.state);
            *worker = Some(tokio::spawn(async move {
                state.run().await;
            }));
        }
    }

    /// Wakes the worker after a committed lifecycle write.
    pub(super) fn notify(&self) {
        self.state.counters.queued.fetch_add(1, Ordering::Relaxed);
        self.state.changed.notify_one();
    }

    pub(super) fn stats(&self) -> NotificationStats {
        self.state.counters.snapshot()
    }

    /// Drains existing events up to the configured deadline, aborts stalled
    /// publication, and joins the worker before the service releases
    /// ownership. Cancelling this future retains its handle so a later
    /// shutdown can finish the same barrier. Returns `NotificationClose` if
    /// rows remain or the worker panics.
    pub(super) async fn close(&self) -> Result<(), TaskServiceError> {
        if !self.state.closing.swap(true, Ordering::AcqRel) {
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
