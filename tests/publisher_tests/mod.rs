// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Publisher fault scenarios through the public service and real SQLite.
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;
use std::time::Duration;

use futures::poll;
use qubit_codec::ValueBytesCodecRegistry;
use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::CapabilityError;
use qubit_event_bus::EventBusFacadeConfig;
use qubit_event_bus::codec::CodecRegistry;
use qubit_event_bus::error::SpiError;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::AdmissionStatus;
use qubit_event_bus::model::DestinationAdmission;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::PublishEffect;
use qubit_event_bus::model::SubscriberId;
use qubit_event_bus::spi::AsyncEventBusSpi;
use qubit_event_bus::spi::AsyncEventSubscriptionSpi;
use qubit_event_bus::spi::DelayedDeliveryCapability;
use qubit_event_bus::spi::DurabilityCapability;
use qubit_event_bus::spi::EventBusCapabilities;
use qubit_event_bus::spi::OrderingCapability;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::PayloadModes;
use qubit_event_bus::spi::PublishGuarantee;
use qubit_event_bus::spi::PublishVisibility;
use qubit_event_bus::spi::ReplayCapability;
use qubit_event_bus::spi::SettlementCapabilities;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;
use qubit_event_bus::spi::SpiFuture;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::SubscriptionModes;
use qubit_event_bus::spi::TransportPayload;
use qubit_id::Id;
use qubit_id::IdGenerationError;
use qubit_id::IdGenerator;
use qubit_task::TaskExecutionService;
use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::event::TaskEvent;
use qubit_task::model::StartCommand;
use qubit_task::model::TaskId;
use qubit_task::model::TaskState;
use qubit_task::model::TransitionCommand;
use qubit_task::service::NotificationFailure;
use qubit_task::service::TaskServiceError;
use qubit_task::store::MemoryTaskStore;
use qubit_task::store::SqliteTaskStore;
use qubit_task::store::StoreError;
use qubit_task::store::TaskStore;
use serde_json::from_slice;

use crate::support::task_event_codec;

const OPAQUE_ACCEPTED: u8 = 0;
const NOT_ACCEPTED_ERROR: u8 = 1;
const UNCERTAIN_ERROR: u8 = 2;
const HUNG_PUBLISH: u8 = 3;
const ACCEPTED: u8 = 4;
const PARTIALLY_ACCEPTED: u8 = 5;
const DROPPED: u8 = 6;
const NO_DESTINATIONS: u8 = 7;
const NONE_ACCEPTED: u8 = 8;
const BLOCKED_ACCEPTED: u8 = 9;

/// A fixed-capability provider with controllable admission and fault outcomes.
struct FakeSpi {
    mode: AtomicU8,
    ids: Mutex<Vec<String>>,
    state_versions: Mutex<Vec<u64>>,
    visibility: PublishVisibility,
    durability: DurabilityCapability,
    publish_guarantee: PublishGuarantee,
    publish_entered: tokio::sync::Notify,
    release_publish: tokio::sync::Notify,
}

/// Creates one individual destination acknowledgement for the receipt matrix.
fn destination(id: u64, status: AdmissionStatus) -> DestinationAdmission {
    DestinationAdmission::new(
        Id::new(id),
        SubscriberId::new(format!("subscriber-{id}")).expect("subscriber ID"),
        status,
    )
}

/// Produces a successful receipt without conflating acceptance with successful
/// IO.
fn acknowledgement(mode: u8) -> Option<PublishAcknowledgement> {
    match mode {
        OPAQUE_ACCEPTED => Some(PublishAcknowledgement::Accepted {
            provider_message_id: None,
            metadata: Default::default(),
        }),
        ACCEPTED => Some(PublishAcknowledgement::DestinationAdmissions(vec![destination(
            1,
            AdmissionStatus::Accepted,
        )])),
        PARTIALLY_ACCEPTED => Some(PublishAcknowledgement::DestinationAdmissions(vec![
            destination(1, AdmissionStatus::Accepted),
            destination(2, AdmissionStatus::Rejected("queue full".into())),
        ])),
        DROPPED => Some(PublishAcknowledgement::DroppedByInterceptor),
        NO_DESTINATIONS => Some(PublishAcknowledgement::DestinationAdmissions(Vec::new())),
        NONE_ACCEPTED => Some(PublishAcknowledgement::DestinationAdmissions(vec![
            destination(1, AdmissionStatus::Filtered),
            destination(2, AdmissionStatus::Rejected("queue full".into())),
        ])),
        _ => None,
    }
}
impl AsyncEventBusSpi for FakeSpi {
    fn capabilities(&self) -> EventBusCapabilities {
        EventBusCapabilities::builder()
            .payload_modes(PayloadModes::Encoded)
            .settlement(SettlementCapabilities::None)
            .ordering(OrderingCapability::None)
            .delayed_delivery(DelayedDeliveryCapability::None)
            .durability(self.durability)
            .subscription_modes(SubscriptionModes::EPHEMERAL)
            .consumer_groups(false)
            .replay(ReplayCapability::None)
            .publish_guarantee(self.publish_guarantee)
            .publish_visibility(self.visibility)
            .build()
            .expect("capabilities")
    }
    fn publish<'a>(&'a self, message: OutboundMessage) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>> {
        Box::pin(async move {
            self.ids.lock().expect("ids").push(message.id().as_str().to_owned());
            let TransportPayload::Encoded(payload) = message.payload() else {
                panic!("task event must be encoded for the fake provider");
            };
            let event: TaskEvent = from_slice(payload.bytes()).expect("decode published task event");
            self.state_versions
                .lock()
                .expect("state versions")
                .push(event.state_version);
            let mode = self.mode.load(Ordering::Acquire);
            if mode == BLOCKED_ACCEPTED {
                self.publish_entered.notify_one();
                self.release_publish.notified().await;
                return Ok(acknowledgement(ACCEPTED).expect("accepted receipt"));
            }
            if mode == HUNG_PUBLISH {
                return std::future::pending().await;
            }
            if let Some(receipt) = acknowledgement(mode) {
                return Ok(receipt);
            }
            Err(SpiError::Publish {
                provider_id: "fake".into(),
                resource: None,
                kind: "injected",
                retryable: Some(false),
                effect: if mode == NOT_ACCEPTED_ERROR {
                    PublishEffect::NotAccepted
                } else {
                    PublishEffect::MayHaveBeenAccepted
                },
                source: Box::new(std::io::Error::other("injected")),
            })
        })
    }
    fn subscribe<'a>(
        &'a self,
        _: SpiSubscriptionRequest,
    ) -> SpiFuture<'a, Result<Box<dyn AsyncEventSubscriptionSpi>, SpiError>> {
        Box::pin(async {
            Err(SpiError::Operation {
                provider_id: "fake".into(),
                operation: "subscribe",
                resource: None,
                kind: "unsupported",
                retryable: Some(false),
                source: Box::new(std::io::Error::other("unused")),
            })
        })
    }
    fn shutdown<'a>(&'a self, _: ShutdownMode) -> SpiFuture<'a, Result<ShutdownOutcome, SpiError>> {
        Box::pin(async { Ok(ShutdownOutcome::Complete) })
    }
}

struct Ids;
impl IdGenerator for Ids {
    fn generate(&self) -> Result<Id, IdGenerationError> {
        Ok(Id::new(100))
    }
}

/// Builds an encoded facade that exercises codec lookup and admission checking.
fn bus(mode: u8, codec: bool) -> (Arc<AsyncEventBus>, Arc<FakeSpi>) {
    bus_with_durability(mode, codec, DurabilityCapability::Durable)
}

/// Builds a fake with explicit durability for the rejected-provider case.
fn bus_with_durability(mode: u8, codec: bool, durability: DurabilityCapability) -> (Arc<AsyncEventBus>, Arc<FakeSpi>) {
    bus_with_capabilities(mode, codec, durability, PublishGuarantee::Accepted)
}

fn bus_with_capabilities(
    mode: u8,
    codec: bool,
    durability: DurabilityCapability,
    publish_guarantee: PublishGuarantee,
) -> (Arc<AsyncEventBus>, Arc<FakeSpi>) {
    let visibility = if mode >= ACCEPTED {
        PublishVisibility::DestinationAdmissions
    } else {
        PublishVisibility::Opaque
    };
    let spi = Arc::new(FakeSpi {
        mode: AtomicU8::new(mode),
        ids: Mutex::new(Vec::new()),
        state_versions: Mutex::new(Vec::new()),
        visibility,
        durability,
        publish_guarantee,
        publish_entered: tokio::sync::Notify::new(),
        release_publish: tokio::sync::Notify::new(),
    });
    let mut registry = CodecRegistry::new();
    if codec {
        registry
            .register(Arc::new(task_event_codec::TaskEventJsonCodec::new().expect("codec")))
            .expect("register codec");
    }
    let config = EventBusFacadeConfig::new().with_codec_registry(Arc::new(registry));
    (
        Arc::new(
            AsyncEventBus::with_config(ProviderId::new("fake").expect("provider"), spi.clone(), config).expect("bus"),
        ),
        spi,
    )
}

/// Rejects a durable provider whose successful publish does not promise
/// acceptance.
#[tokio::test]
async fn test_durable_fire_and_forget_provider_is_rejected_before_outbox_enable() {
    let store = Arc::new(SqliteTaskStore::open(super::database_path()).expect("store"));
    let (bus, spi) = bus_with_capabilities(
        OPAQUE_ACCEPTED,
        true,
        DurabilityCapability::Durable,
        PublishGuarantee::FireAndForget,
    );
    let result = service(store.clone(), bus).await;
    assert!(matches!(
        result,
        Err(TaskServiceError::NotificationProviderInsufficientGuarantee {
            provider_id,
            actual: PublishGuarantee::FireAndForget,
        }) if provider_id == "fake"
    ));
    let owner = store.acquire_owner().await.expect("owner released");
    store
        .accept_encoded(TaskId::from_id(Id::new(51)), super::request())
        .await
        .expect("accept without enabling outbox");
    assert!(
        store
            .list_event_outbox(128)
            .await
            .expect("outbox stays disabled")
            .is_empty()
    );
    assert!(spi.ids.lock().expect("published ids").is_empty());
    store.release_owner(owner).await.expect("release");
}

/// Seeds committed terminal history before the next service owner starts.
async fn seed() -> Arc<SqliteTaskStore> {
    seed_with_path().await.0
}

async fn seed_with_path() -> (Arc<SqliteTaskStore>, std::path::PathBuf) {
    let path = super::database_path();
    let store = Arc::new(SqliteTaskStore::open(&path).expect("store"));
    let owner = store.acquire_owner().await.expect("owner");
    store.enable_event_outbox().await.expect("enable");
    let id = TaskId::from_id(Id::new(42));
    store.accept_encoded(id, super::request()).await.expect("accept");
    store
        .start_encoded(StartCommand {
            id,
            expected_state_version: 0,
            started_at_ms: 1,
        })
        .await
        .expect("start");
    store
        .transition_encoded(TransitionCommand {
            id,
            expected_state_version: 1,
            expected_attempt: 1,
            state: TaskState::Succeeded,
            cancel_requested: false,
            cancel_error: None,
            retry_not_before_ms: None,
            finished_at_ms: Some(2),
            output: None,
        })
        .await
        .expect("finish");
    store.release_owner(owner).await.expect("release");
    (store, path)
}

/// Starts the public service with a bounded publisher shutdown.
async fn service(store: Arc<dyn TaskStore>, bus: Arc<AsyncEventBus>) -> Result<TaskExecutionService, TaskServiceError> {
    service_with_timeout(store, bus, Duration::from_secs(5)).await
}

/// Applies a short deadline only in tests that intentionally cannot drain.
async fn service_with_timeout(
    store: Arc<dyn TaskStore>,
    bus: Arc<AsyncEventBus>,
    timeout: Duration,
) -> Result<TaskExecutionService, TaskServiceError> {
    TaskExecutionServiceBuilder::new(store, Arc::new(ValueBytesCodecRegistry::empty()), Arc::new(Ids))
        .event_bus(bus)
        .notification_shutdown_timeout(timeout)
        .build()
        .await
}

async fn wait_for_notification_failure(service: &TaskExecutionService) -> NotificationFailure {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(failure) = service.notification_failure() {
                break failure;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("notification failure becomes observable")
}

async fn blocked_retry_service() -> (TaskExecutionService, Arc<FakeSpi>, NotificationFailure) {
    let store = seed().await;
    let (bus, spi) = bus(NOT_ACCEPTED_ERROR, true);
    let service = service(store, bus).await.expect("service");
    let failure = wait_for_notification_failure(&service).await;
    spi.mode.store(BLOCKED_ACCEPTED, Ordering::Release);
    tokio::time::timeout(Duration::from_secs(2), spi.publish_entered.notified())
        .await
        .expect("retry entered the controlled provider");
    (service, spi, failure)
}

#[tokio::test]
async fn test_startup_replay_deletes_only_confirmed_admissions() {
    for mode in [OPAQUE_ACCEPTED, ACCEPTED, PARTIALLY_ACCEPTED] {
        let store = seed().await;
        let (bus, spi) = bus(mode, true);
        let service = service(store.clone(), bus).await.expect("service");
        let queued_before_shutdown = service.notification_stats().queued;
        service.shutdown().await.expect("drained");
        let stats = service.notification_stats();
        assert_eq!(stats.queued, queued_before_shutdown, "shutdown is not an enqueue");
        assert_eq!(stats.published, 3);
        assert_eq!(stats.dropped, 0);
        assert_eq!(stats.failed, 0);
        assert_eq!(*spi.ids.lock().expect("ids"), ["task:42:0", "task:42:1", "task:42:2"]);
        assert_eq!(*spi.state_versions.lock().expect("state versions"), [0, 1, 2]);
        let owner = store.acquire_owner().await.expect("owner released");
        assert!(store.list_event_outbox(128).await.expect("empty").is_empty());
        store.release_owner(owner).await.expect("release");
    }
}

#[tokio::test]
async fn test_rejected_and_uncertain_events_replay_with_stable_identity() {
    for mode in [
        NOT_ACCEPTED_ERROR,
        UNCERTAIN_ERROR,
        HUNG_PUBLISH,
        DROPPED,
        NO_DESTINATIONS,
        NONE_ACCEPTED,
    ] {
        let store = seed().await;
        let (bus, spi) = bus(mode, true);
        let first = service_with_timeout(store.clone(), bus.clone(), Duration::from_millis(500))
            .await
            .expect("service");
        let shutdown_error = first.shutdown().await.expect_err("failed head prevents draining");
        assert!(matches!(&shutdown_error, TaskServiceError::NotificationClose(_)));
        let diagnostic = shutdown_error.to_string();
        assert!(
            diagnostic.contains("task:42:0"),
            "publication diagnostic must retain the stable event ID: {diagnostic}"
        );
        let expected_category = match mode {
            NOT_ACCEPTED_ERROR => Some("NotAccepted"),
            UNCERTAIN_ERROR => Some("MayHaveBeenAccepted"),
            DROPPED => Some("dropped by an interceptor"),
            NO_DESTINATIONS | NONE_ACCEPTED => Some("no destination accepted"),
            HUNG_PUBLISH => Some("publication timed out"),
            _ => unreachable!("matrix contains only retained outcomes"),
        };
        if let Some(expected_category) = expected_category {
            assert!(
                diagnostic.contains(expected_category),
                "publication diagnostic must retain its failure category: {diagnostic}"
            );
        }
        assert!(
            matches!(first.shutdown().await, Err(TaskServiceError::NotificationClose(_))),
            "repeated shutdown retains its drain failure"
        );
        let owner = store.acquire_owner().await.expect("released despite timeout");
        let retained = store.list_event_outbox(128).await.expect("retained");
        assert_eq!(
            retained.iter().map(|event| event.event_id.as_str()).collect::<Vec<_>>(),
            ["task:42:0", "task:42:1", "task:42:2"]
        );
        assert_eq!(
            retained.iter().map(|event| event.state_version).collect::<Vec<_>>(),
            [0, 1, 2]
        );
        assert!(
            spi.ids.lock().expect("ids").iter().all(|id| id == "task:42:0"),
            "a failed head must retain order"
        );
        store.release_owner(owner).await.expect("release");
        let accepted_mode = if spi.visibility == PublishVisibility::DestinationAdmissions {
            ACCEPTED
        } else {
            OPAQUE_ACCEPTED
        };
        spi.mode.store(accepted_mode, Ordering::Release);
        let second = service(store, bus).await.expect("restart");
        second.shutdown().await.expect("replayed");
        let ids = spi.ids.lock().expect("ids");
        assert!(ids.iter().filter(|id| id.as_str() == "task:42:0").count() >= 2);
        assert_eq!(&ids[ids.len() - 3..], ["task:42:0", "task:42:1", "task:42:2"]);
        let state_versions = spi.state_versions.lock().expect("state versions");
        assert!(state_versions.iter().filter(|version| **version == 0).count() >= 2);
        assert_eq!(&state_versions[state_versions.len() - 3..], [0, 1, 2]);
    }
}

#[tokio::test]
async fn test_missing_codec_preserves_outbox_and_releases_owner_for_codec_retry() {
    let (missing_codec_bus, spi) = bus(OPAQUE_ACCEPTED, false);
    let store = seed().await;
    let result = service(store.clone(), missing_codec_bus).await;
    assert!(matches!(
        result,
        Err(TaskServiceError::NotificationCodecUnavailable {
            provider_id,
            topic,
        source: CapabilityError::CodecRequired,
        }) if provider_id == "fake" && topic == "task.lifecycle"
    ));
    assert!(spi.ids.lock().expect("ids").is_empty());
    let owner = store.acquire_owner().await.expect("owner");
    assert_eq!(store.list_event_outbox(128).await.expect("retained").len(), 3);
    store.release_owner(owner).await.expect("release");

    let (bus, spi) = bus(OPAQUE_ACCEPTED, true);
    let service = service(store, bus)
        .await
        .expect("restart succeeds after registering the codec");
    service.shutdown().await.expect("replay");
    let ids = spi.ids.lock().expect("ids");
    assert_eq!(
        ids.as_slice(),
        ["task:42:0", "task:42:1", "task:42:2"].map(str::to_owned)
    );
}

#[tokio::test]
async fn test_memory_store_is_rejected_for_event_outbox() {
    let (bus, _) = bus(OPAQUE_ACCEPTED, true);
    let result = TaskExecutionServiceBuilder::new(
        Arc::new(MemoryTaskStore::new(10)),
        Arc::new(ValueBytesCodecRegistry::empty()),
        Arc::new(Ids),
    )
    .event_bus(bus)
    .build()
    .await;
    assert!(matches!(
        result,
        Err(TaskServiceError::Store(StoreError::UnsupportedCapability))
    ));
}

#[tokio::test]
async fn test_service_cancellation_persists_snapshot_before_publication() {
    let store = Arc::new(SqliteTaskStore::open(super::database_path()).expect("store"));
    let id = TaskId::from_id(Id::new(42));
    store.accept_encoded(id, super::request()).await.expect("existing task");
    store
        .transition_encoded(TransitionCommand {
            id,
            expected_state_version: 0,
            expected_attempt: 0,
            state: TaskState::Blocked {
                reason: "operator intervention".into(),
            },
            cancel_requested: false,
            cancel_error: None,
            retry_not_before_ms: None,
            finished_at_ms: None,
            output: None,
        })
        .await
        .expect("blocked task");
    let (bus, spi) = bus(NOT_ACCEPTED_ERROR, true);
    let service = service(store.clone(), bus).await.expect("service");
    let _outcome = service.cancel(id).await.expect("cancel blocked task");
    let events = store.list_event_outbox(128).await.expect("committed snapshot");
    assert_eq!(events.len(), 1, "enabling notifications does not backfill old history");
    assert_eq!(events[0].state_version, 2);
    assert!(events[0].event_json.contains("Cancelled"));
    spi.mode.store(OPAQUE_ACCEPTED, Ordering::Release);
    service.shutdown().await.expect("drain cancellation event");
}

#[tokio::test]
async fn test_no_destination_admission_retains_notifications() {
    let (bus, _) = bus(NO_DESTINATIONS, true);
    let store = seed().await;
    let service = service_with_timeout(store.clone(), bus, Duration::from_millis(100))
        .await
        .expect("service");
    assert!(matches!(
        service.shutdown().await,
        Err(TaskServiceError::NotificationClose(_))
    ));
    let owner = store.acquire_owner().await.expect("owner");
    assert_eq!(store.list_event_outbox(128).await.expect("retained").len(), 3);
    store.release_owner(owner).await.expect("release");
}

/// Rejects an explicitly ephemeral SPI as well as the built-in local provider.
#[tokio::test]
async fn test_ephemeral_fake_provider_is_rejected() {
    let store = Arc::new(SqliteTaskStore::open(super::database_path()).expect("store"));
    let (bus, _) = bus_with_durability(OPAQUE_ACCEPTED, true, DurabilityCapability::Ephemeral);
    let result = service(store.clone(), bus).await;
    assert!(matches!(
        result,
        Err(TaskServiceError::NotificationProviderNotDurable { provider_id }) if provider_id == "fake"
    ));
    let owner = store.acquire_owner().await.expect("owner released");
    store.release_owner(owner).await.expect("release");
}

/// Rejects a local provider before enabling outbox writes or draining saved
/// rows.
#[tokio::test]
async fn test_ephemeral_provider_releases_owner_without_outbox_side_effects() {
    let bus = Arc::new(
        AsyncEventBus::local(LocalEventBusConfig::default())
            .await
            .expect("local bus"),
    );
    let store = Arc::new(SqliteTaskStore::open(super::database_path()).expect("store"));
    let result = service(store.clone(), bus.clone()).await;
    assert!(matches!(
        result,
        Err(TaskServiceError::NotificationProviderNotDurable { provider_id })
            if provider_id == bus.provider_id().as_str()
    ));
    let owner = store.acquire_owner().await.expect("owner released after rejection");
    store
        .accept_encoded(TaskId::from_id(Id::new(51)), super::request())
        .await
        .expect("accept without enabling outbox");
    assert!(
        store
            .list_event_outbox(128)
            .await
            .expect("outbox stays disabled")
            .is_empty()
    );
    store.release_owner(owner).await.expect("release");

    let seeded = seed().await;
    let result = service(seeded.clone(), bus).await;
    assert!(matches!(
        result,
        Err(TaskServiceError::NotificationProviderNotDurable { .. })
    ));
    let owner = seeded.acquire_owner().await.expect("seeded owner released");
    let retained = seeded.list_event_outbox(128).await.expect("saved rows retained");
    assert_eq!(
        retained.iter().map(|entry| entry.event_id.as_str()).collect::<Vec<_>>(),
        ["task:42:0", "task:42:1", "task:42:2"]
    );
    seeded.release_owner(owner).await.expect("release");
}

/// Accepts a provider that promises to retain undelivered messages.
#[tokio::test]
async fn test_durable_provider_allows_service_construction() {
    for guarantee in [
        PublishGuarantee::Accepted,
        PublishGuarantee::Confirmed,
        PublishGuarantee::DurablyStored,
    ] {
        let store = Arc::new(SqliteTaskStore::open(super::database_path()).expect("store"));
        let (bus, _) = bus_with_capabilities(OPAQUE_ACCEPTED, true, DurabilityCapability::Durable, guarantee);
        let service = service(store.clone(), bus)
            .await
            .expect("durable provider with sufficient guarantee accepted");
        service.shutdown().await.expect("shutdown");
        let owner = store.acquire_owner().await.expect("owner released");
        store.release_owner(owner).await.expect("release");
    }
}

#[tokio::test]
async fn test_failed_head_can_be_inspected_and_quarantined_to_resume_ordered_delivery() {
    let (store, path) = seed_with_path().await;
    // Corrupt the oldest event while retaining the two valid successors.
    rusqlite::Connection::open(&path)
        .expect("open database")
        .execute(
            "UPDATE task_event_outbox SET event_json = '{bad' WHERE event_id = 'task:42:0'",
            [],
        )
        .expect("corrupt oldest event");
    let (bus, spi) = bus(ACCEPTED, true);
    let service = service(store.clone(), bus).await.expect("service");

    let failure = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(failure) = service.notification_failure() {
                break failure;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("failed head becomes observable");
    assert_eq!(failure.task_id, TaskId::from_id(Id::new(42)));
    assert_eq!(failure.state_version, 0);
    assert_eq!(failure.event_id, "task:42:0");
    assert!(!failure.error.is_empty());
    assert!(spi.ids.lock().expect("ids").is_empty(), "later events stay ordered");

    service
        .quarantine_failed_notification(
            failure.task_id,
            failure.state_version,
            &failure.event_id,
            " operator approved ",
        )
        .await
        .expect("quarantine exact failed head");
    assert_eq!(service.notification_failure(), None);
    service.shutdown().await.expect("publish successors");

    let connection = rusqlite::Connection::open(&path).expect("inspect archive");
    let archive: (String, String, String) = connection
        .query_row(
            "SELECT event_json, reason, last_error FROM task_event_outbox_quarantine WHERE event_id = 'task:42:0'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .expect("archived failed event");
    assert_eq!(archive.0, "{bad");
    assert_eq!(archive.1, "operator approved");
    assert!(!archive.2.is_empty());
    assert_eq!(*spi.ids.lock().expect("ids"), ["task:42:1", "task:42:2"]);
}

#[tokio::test]
async fn test_notification_quarantine_rejects_invalid_reason_and_stale_identity() {
    let (store, path) = seed_with_path().await;
    rusqlite::Connection::open(&path)
        .expect("open database")
        .execute(
            "UPDATE task_event_outbox SET event_json = '{bad' WHERE event_id = 'task:42:0'",
            [],
        )
        .expect("corrupt oldest event");
    let (bus, _) = bus(ACCEPTED, true);
    let service = service(store, bus).await.expect("service");
    let failure = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(failure) = service.notification_failure() {
                break failure;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("failed head becomes observable");
    assert!(matches!(
        service
            .quarantine_failed_notification(failure.task_id, 99, &failure.event_id, "reason")
            .await,
        Err(TaskServiceError::Store(StoreError::Conflict))
    ));
    assert!(matches!(
        service
            .quarantine_failed_notification(failure.task_id, failure.state_version, &failure.event_id, "  ")
            .await,
        Err(TaskServiceError::InvalidRequest(_))
    ));
    assert!(matches!(
        service
            .quarantine_failed_notification(
                failure.task_id,
                failure.state_version,
                &failure.event_id,
                &"界".repeat(1366),
            )
            .await,
        Err(TaskServiceError::InvalidRequest(_))
    ));
    assert_eq!(service.notification_failure(), Some(failure.clone()));
    assert!(service.shutdown().await.is_err(), "bad head remains pending");
    assert!(matches!(
        service
            .quarantine_failed_notification(failure.task_id, failure.state_version, &failure.event_id, "reason")
            .await,
        Err(TaskServiceError::ShuttingDown)
    ));
}

#[tokio::test]
async fn test_notification_failure_is_cleared_after_a_temporary_provider_failure_recovers() {
    let store = seed().await;
    let (bus, spi) = bus(NOT_ACCEPTED_ERROR, true);
    let service = service(store, bus).await.expect("service");
    let failure = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(failure) = service.notification_failure() {
                break failure;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("provider failure is diagnosed");
    assert_eq!(failure.event_id, "task:42:0");

    spi.mode.store(ACCEPTED, Ordering::Release);
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if service.notification_failure().is_none() && service.notification_stats().published == 3 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("head retries and later notifications publish");
    service.shutdown().await.expect("publisher drained");
}

#[tokio::test]
async fn test_notification_quarantine_reports_unsupported_without_a_publisher() {
    let store = Arc::new(SqliteTaskStore::open(super::database_path()).expect("store"));
    let service = TaskExecutionServiceBuilder::new(store, Arc::new(ValueBytesCodecRegistry::empty()), Arc::new(Ids))
        .build()
        .await
        .expect("service without event bus");

    assert_eq!(service.notification_failure(), None);
    assert!(matches!(
        service
            .quarantine_failed_notification(TaskId::from_id(Id::new(43)), 0, "task:43:0", "operator reason")
            .await,
        Err(TaskServiceError::Store(StoreError::UnsupportedCapability))
    ));
    service.shutdown().await.expect("service shutdown");
}

#[tokio::test]
async fn test_accepted_quarantine_command_is_revalidated_before_shutdown_finishes() {
    let (service, spi, failure) = blocked_retry_service().await;
    let quarantine = service.quarantine_failed_notification(
        failure.task_id,
        failure.state_version,
        &failure.event_id,
        "operator decision",
    );
    tokio::pin!(quarantine);
    assert!(
        poll!(quarantine.as_mut()).is_pending(),
        "command waits behind active publish"
    );

    let shutdown = service.shutdown();
    tokio::pin!(shutdown);
    assert!(
        poll!(shutdown.as_mut()).is_pending(),
        "shutdown waits for the publisher worker"
    );
    spi.mode.store(ACCEPTED, Ordering::Release);
    spi.release_publish.notify_one();

    assert!(matches!(
        quarantine.await,
        Err(TaskServiceError::Store(StoreError::Conflict))
    ));
    shutdown.await.expect("shutdown drains after command completion");
    assert_eq!(service.notification_stats().published, 3);
    assert_eq!(service.notification_failure(), None);
}

#[tokio::test]
async fn test_cancelling_quarantine_waiter_does_not_cancel_accepted_command() {
    let (service, spi, failure) = blocked_retry_service().await;
    {
        let quarantine = service.quarantine_failed_notification(
            failure.task_id,
            failure.state_version,
            &failure.event_id,
            "operator decision",
        );
        tokio::pin!(quarantine);
        assert!(
            poll!(quarantine.as_mut()).is_pending(),
            "command is accepted and waits behind publish"
        );
    }
    spi.mode.store(ACCEPTED, Ordering::Release);
    spi.release_publish.notify_one();

    tokio::time::timeout(Duration::from_secs(3), async {
        while service.notification_stats().published < 3 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("worker continues after the caller stops waiting");
    service.shutdown().await.expect("worker completes and shuts down");
}

#[tokio::test]
async fn test_worker_shutdown_failure_replies_to_an_accepted_quarantine_command() {
    let store = seed().await;
    let (bus, spi) = bus(HUNG_PUBLISH, true);
    let service = service_with_timeout(store, bus, Duration::from_millis(100))
        .await
        .expect("service");
    tokio::time::timeout(Duration::from_secs(2), async {
        while spi.ids.lock().expect("published ids").is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("worker is blocked in provider publication");

    let quarantine =
        service.quarantine_failed_notification(TaskId::from_id(Id::new(42)), 0, "task:42:0", "operator decision");
    tokio::pin!(quarantine);
    assert!(
        poll!(quarantine.as_mut()).is_pending(),
        "command is accepted by the bounded queue"
    );

    let shutdown = service.shutdown();
    tokio::pin!(shutdown);
    assert!(
        poll!(shutdown.as_mut()).is_pending(),
        "shutdown waits for the worker deadline"
    );
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), quarantine).await,
        Ok(Err(TaskServiceError::NotificationClose(_)))
    ));
    assert!(matches!(shutdown.await, Err(TaskServiceError::NotificationClose(_))));
}
