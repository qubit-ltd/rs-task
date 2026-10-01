// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Bounded serial publication of best-effort task lifecycle notifications.

use std::io;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use qubit_event_bus::EventBus;
use qubit_event_bus::NotificationOutcome;
use qubit_event_bus::NotificationPublisher;
use qubit_event_bus::TryPublishError;
use qubit_event_bus::model::AdmissionOutcome;
use qubit_event_bus::model::PublishEffect;
use qubit_event_bus::model::Topic;
use tokio::runtime;

mod internal;

use internal::Counters;

use super::task_event_notification_stats::TaskEventNotificationStats;
use crate::event::TaskEvent;

/// Increments a counter without wrapping its accumulated diagnostic value.
///
/// # Parameters
///
/// * `counter` - Atomic diagnostic counter to increment.
fn increment(counter: &AtomicU64) {
    let _ = counter.fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
        Some(value.saturating_add(1))
    });
}

/// Owns one worker and one bounded queue for a service's event bus.
pub(super) struct TaskEventPublisher {
    /// Bounded publisher used by the single background worker.
    publisher: Arc<NotificationPublisher<TaskEvent>>,
    /// Counters shared with publish completion callbacks.
    counters: Arc<Counters>,
    /// Maximum time a close call waits for worker completion.
    close_timeout: Duration,
}

impl TaskEventPublisher {
    /// Starts a dedicated worker before service scheduling begins.
    ///
    /// # Parameters
    ///
    /// * `bus` - Event bus that receives lifecycle notifications.
    /// * `capacity` - Maximum number of queued notifications.
    /// * `close_timeout` - Maximum wait for worker completion during close.
    ///
    /// # Returns
    ///
    /// A publisher with one dedicated worker.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the publisher worker cannot be started.
    ///
    /// # Panics
    ///
    /// Panics only if the fixed internal lifecycle topic is invalid.
    pub(super) fn new(bus: EventBus, capacity: NonZeroUsize, close_timeout: Duration) -> io::Result<Self> {
        let counters = Arc::new(Counters::default());
        let worker_counters = Arc::clone(&counters);
        let topic = Topic::<TaskEvent>::new("task.lifecycle").expect("fixed task lifecycle topic is valid");
        let publisher = NotificationPublisher::new(bus, topic, capacity, move |outcome| match outcome {
            NotificationOutcome::Published(receipt) => record_admission(&worker_counters, receipt.admission_outcome()),
            NotificationOutcome::PublishFailed(failure) => {
                increment(&worker_counters.publish_error);
                if failure.effect() == PublishEffect::MayHaveBeenAccepted {
                    increment(&worker_counters.uncertain_publish);
                }
            }
            _ => increment(&worker_counters.publish_error),
        })?;
        Ok(Self {
            publisher: Arc::new(publisher),
            counters,
            close_timeout,
        })
    }

    /// Attempts to enqueue without waiting for the worker or event bus.
    ///
    /// # Parameters
    ///
    /// * `event` - Lifecycle snapshot to enqueue.
    pub(super) fn enqueue(&self, event: TaskEvent) {
        match self.publisher.try_publish(event) {
            Ok(()) => increment(&self.counters.enqueued),
            Err(TryPublishError::Full(_)) => increment(&self.counters.queue_full),
            Err(TryPublishError::Closed(_)) => increment(&self.counters.queue_closed),
        }
    }

    /// Returns a monotonic snapshot of enqueue and publication outcomes.
    ///
    /// # Returns
    ///
    /// A snapshot of counters observed with acquire ordering.
    pub(super) fn stats(&self) -> TaskEventNotificationStats {
        let load = |counter: &AtomicU64| counter.load(Ordering::Acquire);
        TaskEventNotificationStats {
            enqueued: load(&self.counters.enqueued),
            queue_full: load(&self.counters.queue_full),
            queue_closed: load(&self.counters.queue_closed),
            accepted: load(&self.counters.accepted),
            opaque_accepted: load(&self.counters.opaque_accepted),
            unaccepted: load(&self.counters.unaccepted),
            partial_rejection: load(&self.counters.partial_rejection),
            publish_error: load(&self.counters.publish_error),
            uncertain_publish: load(&self.counters.uncertain_publish),
            worker_panicked: self.publisher.stats().worker_panicked(),
        }
    }

    /// Stops enqueue, drains accepted events, and waits up to the configured
    /// timeout.
    ///
    /// # Parameters
    ///
    /// * `runtime_handle` - Runtime used to join the blocking close operation.
    ///
    /// # Returns
    ///
    /// Success when the worker closes before the timeout.
    ///
    /// # Errors
    ///
    /// Returns an error when the blocking close task fails to join, the
    /// notification publisher worker panics, or it does not exit before the
    /// configured timeout. On timeout, the worker continues draining accepted
    /// events and a later close call can wait for completion.
    pub(super) async fn close(&self, runtime_handle: &runtime::Handle) -> io::Result<()> {
        let publisher = Arc::clone(&self.publisher);
        let close_timeout = self.close_timeout;
        runtime_handle
            .spawn_blocking(move || publisher.close_with_timeout(close_timeout))
            .await
            .map_err(io::Error::other)?
    }
}

/// Adds one provider admission result to the corresponding observable counters.
///
/// # Parameters
///
/// * `counters` - Shared event publication counters.
/// * `outcome` - Provider's destination admission result.
fn record_admission(counters: &Counters, outcome: AdmissionOutcome) {
    match outcome {
        AdmissionOutcome::OpaqueAccepted => increment(&counters.opaque_accepted),
        AdmissionOutcome::Accepted(_) => increment(&counters.accepted),
        AdmissionOutcome::PartiallyAccepted(_) => {
            increment(&counters.accepted);
            increment(&counters.partial_rejection);
        }
        AdmissionOutcome::NoneAccepted(_) | AdmissionOutcome::NoDestinations | AdmissionOutcome::Dropped => {
            increment(&counters.unaccepted);
        }
        _ => increment(&counters.unaccepted),
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::sync::Condvar;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use qubit_event_bus::EventBus;
    use qubit_event_bus::SubscribeError;
    use qubit_event_bus::error::SpiError;
    use qubit_event_bus::model::AdmissionOutcome;
    use qubit_event_bus::model::AdmissionStatus;
    use qubit_event_bus::model::AdmissionSummary;
    use qubit_event_bus::model::DestinationAdmission;
    use qubit_event_bus::model::ProviderId;
    use qubit_event_bus::model::PublishAcknowledgement;
    use qubit_event_bus::model::PublishEffect;
    use qubit_event_bus::model::SubscribeRequest;
    use qubit_event_bus::model::SubscriberId;
    use qubit_event_bus::model::Topic;
    use qubit_event_bus::spi::DelayedDeliveryCapability;
    use qubit_event_bus::spi::DurabilityCapability;
    use qubit_event_bus::spi::EventBusCapabilities;
    use qubit_event_bus::spi::EventBusSpi;
    use qubit_event_bus::spi::EventSubscriptionSpi;
    use qubit_event_bus::spi::OrderingCapability;
    use qubit_event_bus::spi::OutboundMessage;
    use qubit_event_bus::spi::PayloadModes;
    use qubit_event_bus::spi::PublishGuarantee;
    use qubit_event_bus::spi::PublishVisibility;
    use qubit_event_bus::spi::ReplayCapability;
    use qubit_event_bus::spi::SettlementCapabilities;
    use qubit_event_bus::spi::ShutdownMode;
    use qubit_event_bus::spi::ShutdownOutcome;
    use qubit_event_bus::spi::SpiSubscriptionRequest;
    use qubit_event_bus::spi::SubscriptionModes;
    use qubit_event_bus::spi::TransportPayload;
    use qubit_id::Id;
    use tokio as tokio_crate;
    use tokio::runtime;
    use tokio::spawn;
    use tokio::task;
    use tokio::time;

    use super::Counters;
    use super::TaskEventPublisher;
    use super::record_admission;
    use crate::event::TaskEvent;
    use crate::model::TaskId;
    use crate::model::TaskState;

    struct FakeSpi {
        calls: Mutex<Vec<u64>>,
        entered: AtomicUsize,
        gate: Arc<(Mutex<bool>, Condvar)>,
        block_first: bool,
        outcome: Outcome,
    }

    #[derive(Clone, Copy)]
    enum Outcome {
        Opaque,
        AllRejected,
        Partial,
        Error,
        CertainError,
        Empty,
        Dropped,
        Panic,
    }

    impl FakeSpi {
        fn new(block_first: bool, outcome: Outcome) -> Arc<Self> {
            Arc::new(Self {
                calls: Mutex::new(Vec::new()),
                entered: AtomicUsize::new(0),
                gate: Arc::new((Mutex::new(false), Condvar::new())),
                block_first,
                outcome,
            })
        }

        fn release(&self) {
            let (lock, changed) = &*self.gate;
            *lock.lock().expect("gate lock") = true;
            changed.notify_all();
        }
    }

    impl EventBusSpi for FakeSpi {
        fn capabilities(&self) -> EventBusCapabilities {
            EventBusCapabilities::new(
                PayloadModes::Native,
                SettlementCapabilities::None,
                OrderingCapability::None,
                DelayedDeliveryCapability::None,
                DurabilityCapability::Ephemeral,
                SubscriptionModes::EPHEMERAL,
                false,
                ReplayCapability::None,
                PublishGuarantee::Accepted,
                PublishVisibility::Opaque,
            )
        }

        fn publish(&self, message: OutboundMessage) -> Result<PublishAcknowledgement, SpiError> {
            let index = self.entered.fetch_add(1, Ordering::AcqRel);
            if self.block_first && index == 0 {
                let (lock, changed) = &*self.gate;
                let mut open = lock.lock().expect("gate lock");
                while !*open {
                    open = changed.wait(open).expect("gate wait");
                }
            }
            if let TransportPayload::Native(payload) = message.payload() {
                let event = payload.downcast_ref::<TaskEvent>().expect("task event payload");
                self.calls.lock().expect("calls lock").push(event.state_version);
            }
            let admission = |index, status| {
                DestinationAdmission::new(
                    Id::new(index),
                    SubscriberId::new("fake").expect("subscriber ID"),
                    status,
                )
            };
            match self.outcome {
                Outcome::Opaque => Ok(PublishAcknowledgement::Accepted {
                    provider_message_id: None,
                    metadata: Default::default(),
                }),
                Outcome::AllRejected => Ok(PublishAcknowledgement::DestinationAdmissions(vec![admission(
                    1,
                    AdmissionStatus::Rejected("rejected".into()),
                )])),
                Outcome::Partial => Ok(PublishAcknowledgement::DestinationAdmissions(vec![
                    admission(1, AdmissionStatus::Accepted),
                    admission(2, AdmissionStatus::Rejected("rejected".into())),
                ])),
                Outcome::Empty => Ok(PublishAcknowledgement::DestinationAdmissions(Vec::new())),
                Outcome::Dropped => Ok(PublishAcknowledgement::DroppedByInterceptor),
                Outcome::Error => Err(SpiError::Operation {
                    provider_id: "fake".into(),
                    operation: "publish",
                    resource: None,
                    kind: "scripted",
                    retryable: Some(false),
                    source: Box::new(std::io::Error::other("scripted error")),
                }),
                Outcome::CertainError => Err(SpiError::Publish {
                    provider_id: "fake".into(),
                    resource: None,
                    kind: "not-accepted",
                    retryable: Some(false),
                    effect: PublishEffect::NotAccepted,
                    source: Box::new(std::io::Error::other("not submitted")),
                }),
                Outcome::Panic => panic!("scripted worker panic"),
            }
        }

        fn subscribe(&self, _: SpiSubscriptionRequest) -> Result<Box<dyn EventSubscriptionSpi>, SpiError> {
            Err(SpiError::Operation {
                provider_id: "fake".into(),
                operation: "subscribe",
                resource: None,
                kind: "unsupported",
                retryable: Some(false),
                source: Box::new(std::io::Error::other("subscriptions are unsupported")),
            })
        }
        fn shutdown(&self, _: ShutdownMode) -> Result<ShutdownOutcome, SpiError> {
            Ok(ShutdownOutcome::Complete)
        }
    }

    fn event(version: u64) -> TaskEvent {
        TaskEvent {
            task_id: TaskId::generate().to_string(),
            state_version: version,
            state: TaskState::Queued,
            correlation_key: None,
        }
    }

    fn publisher_with_timeout(spi: Arc<FakeSpi>, capacity: usize, close_timeout: Duration) -> TaskEventPublisher {
        let bus = EventBus::from_spi(ProviderId::new("fake").expect("provider ID"), spi)
            .expect("fake provider descriptor is valid");
        TaskEventPublisher::new(
            bus,
            NonZeroUsize::new(capacity).expect("nonzero capacity"),
            close_timeout,
        )
        .expect("publisher starts")
    }

    fn publisher(spi: Arc<FakeSpi>, capacity: usize) -> TaskEventPublisher {
        publisher_with_timeout(spi, capacity, Duration::from_secs(30))
    }

    #[test]
    fn test_task_event_publisher_maps_each_admission_outcome_to_one_counter() {
        let counters = Counters::default();
        let summary = AdmissionSummary {
            accepted: 1,
            filtered: 0,
            rejected: 0,
        };

        record_admission(&counters, AdmissionOutcome::OpaqueAccepted);
        record_admission(&counters, AdmissionOutcome::Accepted(summary));
        record_admission(
            &counters,
            AdmissionOutcome::PartiallyAccepted(AdmissionSummary {
                accepted: 1,
                filtered: 0,
                rejected: 1,
            }),
        );
        record_admission(
            &counters,
            AdmissionOutcome::NoneAccepted(AdmissionSummary {
                accepted: 0,
                filtered: 1,
                rejected: 0,
            }),
        );
        record_admission(&counters, AdmissionOutcome::NoDestinations);
        record_admission(&counters, AdmissionOutcome::Dropped);

        assert_eq!(1, counters.opaque_accepted.load(Ordering::Acquire));
        assert_eq!(2, counters.accepted.load(Ordering::Acquire));
        assert_eq!(1, counters.partial_rejection.load(Ordering::Acquire));
        assert_eq!(3, counters.unaccepted.load(Ordering::Acquire));
        assert_eq!(0, counters.publish_error.load(Ordering::Acquire));
    }

    #[test]
    fn test_task_event_publisher_fake_spi_unsupported_subscription_is_reported() {
        let spi = FakeSpi::new(false, Outcome::Opaque);
        let bus = EventBus::from_spi(ProviderId::new("fake").expect("provider ID"), spi)
            .expect("fake provider descriptor is valid");
        let request = SubscribeRequest::new(
            "task-observer",
            Topic::<TaskEvent>::new("task.lifecycle").expect("task lifecycle topic"),
        )
        .expect("subscribe request");

        let error = match bus.subscribe(request, |_| ()) {
            Ok(_) => panic!("fake SPI does not support subscriptions"),
            Err(error) => error,
        };
        let SubscribeError::Spi(error) = error else {
            panic!("expected provider SPI error");
        };
        assert_eq!(error.provider_id(), "fake");
        assert_eq!(error.operation(), "subscribe");
        assert_eq!(error.kind(), "unsupported");
        assert_eq!(error.retryable(), Some(false));
    }

    #[test]
    fn test_task_event_publisher_fake_spi_shutdown_is_complete() {
        let spi = FakeSpi::new(false, Outcome::Opaque);
        let bus = EventBus::from_spi(ProviderId::new("fake").expect("provider ID"), spi)
            .expect("fake provider descriptor is valid");

        let outcome = bus.shutdown(ShutdownMode::Immediate).expect("bus shutdown");

        assert!(matches!(outcome.outcome, ShutdownOutcome::Complete));
    }

    #[tokio_crate::test]
    async fn test_task_event_publisher_queue_full_is_nonblocking_and_ordered() {
        let spi = FakeSpi::new(true, Outcome::Opaque);
        let publisher = Arc::new(publisher(spi.clone(), 1));
        publisher.enqueue(event(1));
        while spi.entered.load(Ordering::Acquire) == 0 {
            task::yield_now().await;
        }
        publisher.enqueue(event(2));
        let third_publisher = Arc::clone(&publisher);
        let (returned, received) = std::sync::mpsc::channel();
        let third = std::thread::spawn(move || {
            third_publisher.enqueue(event(3));
            returned.send(()).expect("enqueue return signal");
        });
        let nonblocking = received.recv_timeout(std::time::Duration::from_millis(100)).is_ok();
        spi.release();
        third.join().expect("third enqueue thread");
        assert!(nonblocking, "full-queue enqueue returned without waiting for publish");
        assert_eq!(publisher.stats().queue_full, 1);
        publisher
            .close(&runtime::Handle::current())
            .await
            .expect("publisher closes after queue-full coverage");
        assert_eq!(*spi.calls.lock().expect("calls lock"), vec![1, 2]);
        assert_eq!(publisher.stats().enqueued, 2);
        assert_eq!(publisher.stats().opaque_accepted, 2);
        assert_eq!(publisher.stats().uncertain_publish, 0);
    }

    #[tokio_crate::test]
    async fn test_task_event_publisher_admission_outcomes_and_panic() {
        for outcome in [
            Outcome::AllRejected,
            Outcome::Partial,
            Outcome::Error,
            Outcome::CertainError,
            Outcome::Empty,
            Outcome::Dropped,
            Outcome::Panic,
        ] {
            let publisher = publisher(FakeSpi::new(false, outcome), 1);
            publisher.enqueue(event(1));
            let close_result = publisher.close(&runtime::Handle::current()).await;
            let stats = publisher.stats();
            match outcome {
                Outcome::AllRejected | Outcome::Empty | Outcome::Dropped => {
                    assert_eq!(stats.unaccepted, 1);
                    close_result.expect("non-panicking worker closes successfully");
                }
                Outcome::Partial => {
                    assert_eq!(stats.accepted, 1);
                    assert_eq!(stats.partial_rejection, 1);
                    close_result.expect("non-panicking worker closes successfully");
                }
                Outcome::Error => {
                    assert_eq!(stats.publish_error, 1);
                    assert_eq!(stats.uncertain_publish, 1);
                    close_result.expect("publish errors do not panic the worker");
                }
                Outcome::CertainError => {
                    assert_eq!(stats.publish_error, 1);
                    assert_eq!(stats.uncertain_publish, 0);
                    close_result.expect("definite publish failures do not panic the worker");
                }
                Outcome::Panic => {
                    assert_eq!(stats.publish_error, 1);
                    assert_eq!(stats.worker_panicked, 0);
                    assert_eq!(stats.uncertain_publish, 1);
                    close_result.expect("SPI publish panics are converted to publish errors");
                }
                Outcome::Opaque => unreachable!(),
            }
        }
    }

    #[tokio_crate::test]
    async fn test_task_event_publisher_close_reports_blocking_join_failure() {
        let publisher = publisher(FakeSpi::new(false, Outcome::Opaque), 1);
        let runtime = runtime::Builder::new_current_thread()
            .build()
            .expect("separate runtime builds");
        let stopped_handle = runtime.handle().clone();
        runtime.shutdown_background();

        let close_result = time::timeout(Duration::from_secs(2), publisher.close(&stopped_handle))
            .await
            .expect("close task join returns");
        assert!(
            close_result.is_err(),
            "failure to join the blocking close task must be reported"
        );
        publisher
            .close(&runtime::Handle::current())
            .await
            .expect("current runtime can close the worker");
    }

    #[tokio_crate::test]
    async fn test_task_event_publisher_concurrent_close_waits_for_drain() {
        let spi = FakeSpi::new(true, Outcome::Opaque);
        let publisher = Arc::new(publisher(spi.clone(), 1));
        let runtime_handle = runtime::Handle::current();
        publisher.enqueue(event(1));
        while spi.entered.load(Ordering::Acquire) == 0 {
            task::yield_now().await;
        }
        publisher.enqueue(event(2));
        let first = {
            let publisher = publisher.clone();
            let runtime_handle = runtime_handle.clone();
            spawn(async move { publisher.close(&runtime_handle).await })
        };
        let second = {
            let publisher = publisher.clone();
            let runtime_handle = runtime_handle.clone();
            spawn(async move { publisher.close(&runtime_handle).await })
        };
        task::yield_now().await;
        assert!(!first.is_finished());
        assert!(!second.is_finished());
        spi.release();
        first.await.expect("first close task").expect("first close succeeds");
        second.await.expect("second close task").expect("second close succeeds");
        publisher.enqueue(event(3));
        assert_eq!(publisher.stats().queue_closed, 1);
        assert_eq!(*spi.calls.lock().expect("calls lock"), vec![1, 2]);
    }

    #[tokio_crate::test]
    async fn test_task_event_publisher_close_timeout_can_be_retried() {
        let spi = FakeSpi::new(true, Outcome::Opaque);
        let publisher = Arc::new(publisher_with_timeout(spi.clone(), 1, Duration::from_millis(20)));
        publisher.enqueue(event(1));
        while spi.entered.load(Ordering::Acquire) == 0 {
            task::yield_now().await;
        }

        let error = publisher
            .close(&runtime::Handle::current())
            .await
            .expect_err("blocked provider exceeds the close timeout");
        assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
        publisher.enqueue(event(2));
        assert_eq!(publisher.stats().queue_closed, 1);

        spi.release();
        time::timeout(Duration::from_secs(2), async {
            loop {
                if publisher.stats().opaque_accepted == 1 {
                    break;
                }
                task::yield_now().await;
            }
        })
        .await
        .expect("accepted notification drains after provider unblocks");
        publisher
            .close(&runtime::Handle::current())
            .await
            .expect("close can be retried after worker completion");
    }
}
