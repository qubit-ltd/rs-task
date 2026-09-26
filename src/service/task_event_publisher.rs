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

use qubit_event_bus::EventBus;
use qubit_event_bus::NotificationOutcome;
use qubit_event_bus::NotificationPublisher;
use qubit_event_bus::TryPublishError;
use qubit_event_bus::model::AdmissionOutcome;
use qubit_event_bus::model::Topic;

use super::task_event_notification_stats::TaskEventNotificationStats;
use crate::event::TaskEvent;

/// Atomic counters shared between the service thread and publisher worker.
#[derive(Default)]
struct Counters {
    enqueued: AtomicU64,
    queue_full: AtomicU64,
    queue_closed: AtomicU64,
    accepted: AtomicU64,
    opaque_accepted: AtomicU64,
    unaccepted: AtomicU64,
    partial_rejection: AtomicU64,
    publish_error: AtomicU64,
}

/// Increments a counter without wrapping its accumulated diagnostic value.
fn increment(counter: &AtomicU64) {
    let _ = counter.fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
        Some(value.saturating_add(1))
    });
}

/// Owns one worker and one bounded queue for a service's event bus.
pub(super) struct TaskEventPublisher {
    publisher: Arc<NotificationPublisher<TaskEvent>>,
    counters: Arc<Counters>,
}

impl TaskEventPublisher {
    /// Starts a dedicated worker before service scheduling begins.
    pub(super) fn new(bus: EventBus, capacity: NonZeroUsize) -> io::Result<Self> {
        let counters = Arc::new(Counters::default());
        let worker_counters = Arc::clone(&counters);
        let topic = Topic::<TaskEvent>::new("task.lifecycle").expect("fixed task lifecycle topic is valid");
        let publisher = NotificationPublisher::new(bus, topic, capacity, move |outcome| match outcome {
            NotificationOutcome::Published(receipt) => record_admission(&worker_counters, receipt.admission_outcome()),
            _ => increment(&worker_counters.publish_error),
        })?;
        Ok(Self {
            publisher: Arc::new(publisher),
            counters,
        })
    }

    /// Attempts to enqueue without waiting for the worker or event bus.
    pub(super) fn enqueue(&self, event: TaskEvent) {
        match self.publisher.try_publish(event) {
            Ok(()) => increment(&self.counters.enqueued),
            Err(TryPublishError::Full(_)) => increment(&self.counters.queue_full),
            Err(TryPublishError::Closed(_)) => increment(&self.counters.queue_closed),
        }
    }

    /// Returns a monotonic snapshot of enqueue and publication outcomes.
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
            worker_panicked: self.publisher.stats().worker_panicked(),
        }
    }

    /// Stops enqueue, drains accepted events, and waits for the worker to exit.
    pub(super) async fn close(&self, runtime_handle: &tokio::runtime::Handle) {
        let publisher = Arc::clone(&self.publisher);
        let _ = runtime_handle.spawn_blocking(move || publisher.close()).await;
    }
}

/// Adds one provider admission result to the corresponding observable counters.
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

    use qubit_event_bus::EventBus;
    use qubit_event_bus::SubscribeError;
    use qubit_event_bus::error::SpiError;
    use qubit_event_bus::model::AdmissionOutcome;
    use qubit_event_bus::model::AdmissionStatus;
    use qubit_event_bus::model::AdmissionSummary;
    use qubit_event_bus::model::DestinationAdmission;
    use qubit_event_bus::model::ProviderId;
    use qubit_event_bus::model::PublishAcknowledgement;
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
    use qubit_event_bus::spi::TransportPayload;

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
                    qubit_id::Id::new(index),
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
            task_id: TaskId::generate(),
            state_version: version,
            state: TaskState::Queued,
            correlation_key: None,
        }
    }

    fn publisher(spi: Arc<FakeSpi>, capacity: usize) -> TaskEventPublisher {
        let bus = EventBus::from_spi(ProviderId::new("fake").expect("provider ID"), spi);
        TaskEventPublisher::new(bus, NonZeroUsize::new(capacity).expect("nonzero capacity")).expect("publisher starts")
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
        let bus = EventBus::from_spi(ProviderId::new("fake").expect("provider ID"), spi);
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
        let bus = EventBus::from_spi(ProviderId::new("fake").expect("provider ID"), spi);

        let outcome = bus.shutdown(ShutdownMode::Immediate).expect("bus shutdown");

        assert!(matches!(outcome, ShutdownOutcome::Complete));
    }

    #[tokio::test]
    async fn test_task_event_publisher_queue_full_is_nonblocking_and_ordered() {
        let spi = FakeSpi::new(true, Outcome::Opaque);
        let publisher = Arc::new(publisher(spi.clone(), 1));
        publisher.enqueue(event(1));
        while spi.entered.load(Ordering::Acquire) == 0 {
            tokio::task::yield_now().await;
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
        publisher.close(&tokio::runtime::Handle::current()).await;
        assert_eq!(*spi.calls.lock().expect("calls lock"), vec![1, 2]);
        assert_eq!(publisher.stats().enqueued, 2);
        assert_eq!(publisher.stats().opaque_accepted, 2);
    }

    #[tokio::test]
    async fn test_task_event_publisher_admission_outcomes_and_panic() {
        for outcome in [
            Outcome::AllRejected,
            Outcome::Partial,
            Outcome::Error,
            Outcome::Empty,
            Outcome::Dropped,
            Outcome::Panic,
        ] {
            let publisher = publisher(FakeSpi::new(false, outcome), 1);
            publisher.enqueue(event(1));
            publisher.close(&tokio::runtime::Handle::current()).await;
            let stats = publisher.stats();
            match outcome {
                Outcome::AllRejected | Outcome::Empty | Outcome::Dropped => {
                    assert_eq!(stats.unaccepted, 1)
                }
                Outcome::Partial => {
                    assert_eq!(stats.accepted, 1);
                    assert_eq!(stats.partial_rejection, 1);
                }
                Outcome::Error => assert_eq!(stats.publish_error, 1),
                Outcome::Panic => assert_eq!(stats.publish_error + stats.worker_panicked, 1),
                Outcome::Opaque => unreachable!(),
            }
        }
    }

    #[tokio::test]
    async fn test_task_event_publisher_concurrent_close_waits_for_drain() {
        let spi = FakeSpi::new(true, Outcome::Opaque);
        let publisher = Arc::new(publisher(spi.clone(), 1));
        let runtime_handle = tokio::runtime::Handle::current();
        publisher.enqueue(event(1));
        while spi.entered.load(Ordering::Acquire) == 0 {
            tokio::task::yield_now().await;
        }
        publisher.enqueue(event(2));
        let first = {
            let publisher = publisher.clone();
            let runtime_handle = runtime_handle.clone();
            tokio::spawn(async move { publisher.close(&runtime_handle).await })
        };
        let second = {
            let publisher = publisher.clone();
            let runtime_handle = runtime_handle.clone();
            tokio::spawn(async move { publisher.close(&runtime_handle).await })
        };
        tokio::task::yield_now().await;
        assert!(!first.is_finished());
        assert!(!second.is_finished());
        spi.release();
        first.await.expect("first close");
        second.await.expect("second close");
        publisher.enqueue(event(3));
        assert_eq!(publisher.stats().queue_closed, 1);
        assert_eq!(*spi.calls.lock().expect("calls lock"), vec![1, 2]);
    }
}
