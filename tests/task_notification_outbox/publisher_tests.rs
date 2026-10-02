//! Publisher fault scenarios through the public service and real SQLite.
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;
use qubit_event_bus::{AsyncEventBus, EventBusFacadeConfig};
use qubit_event_bus::codec::CodecRegistry;
use qubit_event_bus::error::SpiError;
use qubit_event_bus::model::{ProviderId, PublishAcknowledgement, PublishEffect};
use qubit_event_bus::spi::{AsyncEventBusSpi, AsyncEventSubscriptionSpi, DelayedDeliveryCapability, DurabilityCapability, EventBusCapabilities, OrderingCapability, OutboundMessage, PayloadModes, PublishGuarantee, PublishVisibility, ReplayCapability, SettlementCapabilities, ShutdownMode, ShutdownOutcome, SpiFuture, SpiSubscriptionRequest, SubscriptionModes};
use qubit_task::{TaskExecutionService, TaskExecutionServiceBuilder};
use qubit_task::model::{TaskId, StartCommand, TaskState, TransitionCommand};
use qubit_task::service::TaskServiceError;
use qubit_task::store::{MemoryTaskStore, SqliteTaskStore, TaskStore};

#[path = "../fixtures/doc-examples/src/task_event_codec.rs"]
mod task_event_codec;

/// Mode: 0 accepts, 1 rejects, 2 loses acknowledgement, 3 never returns.
struct FakeSpi { mode: AtomicU8, ids: Mutex<Vec<String>> }
impl AsyncEventBusSpi for FakeSpi {
    fn capabilities(&self) -> EventBusCapabilities {
        EventBusCapabilities::builder().payload_modes(PayloadModes::Encoded)
            .settlement(SettlementCapabilities::None).ordering(OrderingCapability::None)
            .delayed_delivery(DelayedDeliveryCapability::None).durability(DurabilityCapability::Ephemeral)
            .subscription_modes(SubscriptionModes::EPHEMERAL).consumer_groups(false)
            .replay(ReplayCapability::None).publish_guarantee(PublishGuarantee::Accepted)
            .publish_visibility(PublishVisibility::Opaque).build().expect("capabilities")
    }
    fn publish<'a>(&'a self, message: OutboundMessage) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>> {
        Box::pin(async move {
            self.ids.lock().expect("ids").push(message.id().as_str().to_owned());
            let mode = self.mode.load(Ordering::Acquire);
            if mode == 3 { return std::future::pending().await; }
            if mode == 0 { return Ok(PublishAcknowledgement::Accepted { provider_message_id: None, metadata: Default::default() }); }
            Err(SpiError::Publish { provider_id: "fake".into(), resource: None, kind: "injected", retryable: Some(false), effect: if mode == 1 { PublishEffect::NotAccepted } else { PublishEffect::MayHaveBeenAccepted }, source: Box::new(std::io::Error::other("injected")) })
        })
    }
    fn subscribe<'a>(&'a self, _: SpiSubscriptionRequest) -> SpiFuture<'a, Result<Box<dyn AsyncEventSubscriptionSpi>, SpiError>> {
        Box::pin(async { Err(SpiError::Operation { provider_id: "fake".into(), operation: "subscribe", resource: None, kind: "unsupported", retryable: Some(false), source: Box::new(std::io::Error::other("unused")) }) })
    }
    fn shutdown<'a>(&'a self, _: ShutdownMode) -> SpiFuture<'a, Result<ShutdownOutcome, SpiError>> { Box::pin(async { Ok(ShutdownOutcome::Complete) }) }
}

struct Ids;
impl qubit_id::IdGenerator for Ids {
    fn generate(&self) -> Result<qubit_id::Id, qubit_id::IdGenerationError> { Ok(qubit_id::Id::new(100)) }
}

/// Builds an encoded facade that exercises codec lookup and admission checking.
fn bus(mode: u8, codec: bool) -> (Arc<AsyncEventBus>, Arc<FakeSpi>) {
    let spi = Arc::new(FakeSpi { mode: AtomicU8::new(mode), ids: Mutex::new(Vec::new()) });
    let mut registry = CodecRegistry::new();
    if codec { registry.register(Arc::new(task_event_codec::TaskEventJsonCodec::new().expect("codec"))); }
    let config = EventBusFacadeConfig::new().with_codec_registry(Arc::new(registry));
    (Arc::new(AsyncEventBus::with_config(ProviderId::new("fake").expect("provider"), spi.clone(), config).expect("bus")), spi)
}

/// Seeds committed terminal history before the next service owner starts.
async fn seed() -> Arc<SqliteTaskStore> {
    let store = Arc::new(SqliteTaskStore::open_next(super::database_path()).expect("store"));
    let owner = store.acquire_owner().await.expect("owner");
    store.enable_event_outbox().await.expect("enable");
    let id = TaskId::from_id(qubit_id::Id::new(42));
    store.accept_encoded(id, super::request()).await.expect("accept");
    store.start_encoded(StartCommand { id, expected_state_version: 0, started_at_ms: 1 }).await.expect("start");
    store.transition_encoded(TransitionCommand { id, expected_state_version: 1, expected_attempt: 1, state: TaskState::Succeeded, cancel_requested: false, cancel_error: None, retry_not_before_ms: None, finished_at_ms: Some(2), output: None }).await.expect("finish");
    store.release_owner(owner).await.expect("release");
    store
}

/// Starts the public service with a bounded publisher shutdown.
async fn service(store: Arc<dyn TaskStore>, bus: Arc<AsyncEventBus>) -> Result<TaskExecutionService, TaskServiceError> {
    service_with_timeout(store, bus, Duration::from_secs(5)).await
}

/// Applies a short deadline only in tests that intentionally cannot drain.
async fn service_with_timeout(store: Arc<dyn TaskStore>, bus: Arc<AsyncEventBus>, timeout: Duration) -> Result<TaskExecutionService, TaskServiceError> {
    TaskExecutionServiceBuilder::new(store, Arc::new(qubit_codec::ValueBytesCodecRegistry::empty()), Arc::new(Ids))
        .event_bus(bus).notification_shutdown_timeout(timeout).build().await
}

#[tokio::test]
async fn test_startup_replay_deletes_only_confirmed_admissions() {
    let store = seed().await;
    let (bus, spi) = bus(0, true);
    let service = service(store.clone(), bus).await.expect("service");
    service.shutdown().await.expect("drained");
    assert_eq!(*spi.ids.lock().expect("ids"), ["task:42:0", "task:42:1", "task:42:2"]);
    let owner = store.acquire_owner().await.expect("owner released");
    assert!(store.list_event_outbox(128).await.expect("empty").is_empty());
    store.release_owner(owner).await.expect("release");
}

#[tokio::test]
async fn test_rejected_and_uncertain_events_replay_with_stable_identity() {
    for mode in [1,2,3] {
        let store = seed().await;
        let (bus, spi) = bus(mode, true);
        let first = service_with_timeout(store.clone(), bus.clone(), Duration::from_millis(500)).await.expect("service");
        assert!(matches!(first.shutdown().await, Err(TaskServiceError::NotificationClose(_))));
        assert!(matches!(first.shutdown().await, Err(TaskServiceError::NotificationClose(_))), "repeated shutdown retains its drain failure");
        let owner = store.acquire_owner().await.expect("released despite timeout");
        assert_eq!(store.list_event_outbox(128).await.expect("retained").len(), 3);
        store.release_owner(owner).await.expect("release");
        spi.mode.store(0, Ordering::Release);
        let second = service(store, bus).await.expect("restart");
        second.shutdown().await.expect("replayed");
        let ids = spi.ids.lock().expect("ids");
        assert!(ids.iter().filter(|id| id.as_str() == "task:42:0").count() >= 2);
        assert_eq!(&ids[ids.len()-3..], ["task:42:0", "task:42:1", "task:42:2"]);
    }
}

#[tokio::test]
async fn test_missing_codec_preserves_outbox_and_memory_store_is_rejected() {
    let (bus, spi) = bus(0, false);
    let store = seed().await;
    let service = service_with_timeout(store.clone(), bus.clone(), Duration::from_millis(500)).await.expect("service");
    assert!(matches!(service.shutdown().await, Err(TaskServiceError::NotificationClose(_))));
    assert!(spi.ids.lock().expect("ids").is_empty());
    let owner = store.acquire_owner().await.expect("owner");
    assert_eq!(store.list_event_outbox(128).await.expect("retained").len(), 3);
    store.release_owner(owner).await.expect("release");
    let result = TaskExecutionServiceBuilder::new(Arc::new(MemoryTaskStore::new(10)), Arc::new(qubit_codec::ValueBytesCodecRegistry::empty()), Arc::new(Ids)).event_bus(bus).build().await;
    assert!(matches!(result, Err(TaskServiceError::Store(qubit_task::store::StoreError::UnsupportedCapability))));
}

#[tokio::test]
async fn test_service_cancellation_persists_snapshot_before_publication() {
    let store = Arc::new(SqliteTaskStore::open_next(super::database_path()).expect("store"));
    let id = TaskId::from_id(qubit_id::Id::new(42));
    store.accept_encoded(id, super::request()).await.expect("existing task");
    store.transition_encoded(TransitionCommand { id, expected_state_version: 0, expected_attempt: 0, state: TaskState::Blocked { reason: "operator intervention".into() }, cancel_requested: false, cancel_error: None, retry_not_before_ms: None, finished_at_ms: None, output: None }).await.expect("blocked task");
    let (bus, spi) = bus(1, true);
    let service = service(store.clone(), bus).await.expect("service");
    let _outcome = service.cancel(id).await.expect("cancel blocked task");
    let events = store.list_event_outbox(128).await.expect("committed snapshot");
    assert_eq!(events.len(), 1, "enabling notifications does not backfill old history");
    assert_eq!(events[0].state_version, 2);
    assert!(events[0].event_json.contains("Cancelled"));
    spi.mode.store(0, Ordering::Release);
    service.shutdown().await.expect("drain cancellation event");
}

#[tokio::test]
async fn test_no_destination_admission_retains_notifications() {
    let registry = qubit_event_bus::AsyncEventBusRegistry::with_local().expect("local registry");
    let bus = Arc::new(registry.create(&qubit_event_bus::EventBusConfig::default()).await.expect("local bus"));
    let store = seed().await;
    let service = service_with_timeout(store.clone(), bus, Duration::from_millis(100)).await.expect("service");
    assert!(matches!(service.shutdown().await, Err(TaskServiceError::NotificationClose(_))));
    let owner = store.acquire_owner().await.expect("owner");
    assert_eq!(store.list_event_outbox(128).await.expect("retained").len(), 3);
    store.release_owner(owner).await.expect("release");
}
