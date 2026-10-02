//! Real typed service → SQLite outbox → Redis → duplicate-aware durable consumer.
#![cfg(all(feature = "sqlite", feature = "event-bus"))]

#[path = "redis_task_outbox/support.rs"]
mod support;

use std::collections::HashMap;
use std::error::Error;
use std::path::PathBuf;
use std::process::Command;
use std::sync::{Arc, mpsc};
use std::time::Duration;

use qubit_event_bus::{AsyncEventBus, DeliveryError, EventBus, EventBusConfig, EventBusFacadeConfig, EventBusRegistry};
use qubit_event_bus::codec::CodecRegistry;
use qubit_event_bus::model::{ConsumerGroup, ProviderId, ProviderOptions, StartPosition, SubscribeRequest, SubscriberId, SubscriptionDurability, Topic};
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus_redis::r#async::AsyncRedisEventBusProvider;
use qubit_event_bus_redis::naming::stream_key;
use qubit_model_metadata::metadata::ModelIdBuf;
use qubit_spi::{AsyncServiceProvider, ProviderSelection};
use qubit_task::{CancellationMode, TaskContext, TaskExecutionService, TaskExecutionServiceBuilder, TaskHandler, TaskHandlerDescriptor};
use qubit_task::event::TaskEvent;
use qubit_task::handler::{TaskRunOutcome, TaskRunResult};
use qubit_task::model::{TaskId, TaskOutput, TaskState};
use qubit_task::service::TaskServiceError;
use qubit_task::store::{SqliteTaskStore, TaskFuture, TaskStore};
use support::controlled_redis::proxy::ControlledRedis;
use support::interrupt_before_mark::InterruptBeforeMark;
use support::redis_server::RedisServer;
use support::task_event_codec::TaskEventJsonCodec;
use support::typed_support;

type TestResult = Result<(), Box<dyn Error>>;
const DEADLINE: Duration = Duration::from_secs(15);

/// Returns an unused per-test SQLite path; no user database is touched.
fn database_path() -> PathBuf {
    std::env::temp_dir().join(format!("redis-task-outbox-{}.sqlite", uuid::Uuid::new_v4()))
}

/// Configures identical wire codecs and stream namespaces for both facades.
fn config(url: &str, namespace: &str) -> Result<EventBusConfig, Box<dyn Error>> {
    let mut codecs = CodecRegistry::new();
    codecs.register(Arc::new(TaskEventJsonCodec::new()?));
    let options: ProviderOptions = [
        ("redis.url".into(), url.into()),
        ("redis.namespace".into(), namespace.into()),
        ("redis.claim_min_idle_ms".into(), "60000".into()),
    ].into();
    Ok(EventBusConfig::default().with_selection(ProviderSelection::named("redis-streams")?)
        .with_provider_options(options)
        .with_facade_config(EventBusFacadeConfig::new().with_codec_registry(Arc::new(codecs))))
}

/// Creates the production asynchronous Redis provider with the task event codec.
async fn producer(url: &str, namespace: &str) -> Result<Arc<AsyncEventBus>, Box<dyn Error>> {
    let config = config(url, namespace)?;
    let spi = AsyncRedisEventBusProvider.create_configured(&config).await.map_err(|failure| failure.into_error())?;
    Ok(Arc::new(AsyncEventBus::with_config(ProviderId::new("redis-streams")?, spi, config.facade_config().clone())?))
}

/// Finishes normal tasks or waits for explicit cooperative cancellation.
struct Handler { started: tokio::sync::mpsc::UnboundedSender<TaskId> }
impl TaskHandler<serde_json::Value> for Handler {
    fn run<'a>(&'a self, input: serde_json::Value, context: TaskContext) -> TaskFuture<'a, TaskRunResult> {
        Box::pin(async move {
            let _ = self.started.send(context.task_id());
            if input["cancel"].as_bool() == Some(true) {
                while !context.is_cancelled() { tokio::task::yield_now().await; }
                Ok(TaskRunOutcome::Cancelled)
            } else {
                Ok(TaskRunOutcome::Succeeded(TaskOutput::default()))
            }
        })
    }
}

/// Builds the real typed service; returned channel identifies actual handler start.
async fn service(store: Arc<dyn TaskStore>, bus: Arc<AsyncEventBus>, timeout: Duration) -> Result<(TaskExecutionService, tokio::sync::mpsc::UnboundedReceiver<TaskId>), Box<dyn Error>> {
    let (started, receiver) = tokio::sync::mpsc::unbounded_channel();
    let mut builder = TaskExecutionServiceBuilder::new(store, Arc::new(typed_support::codecs()?), Arc::new(typed_support::SequentialIds::new(100)))
        .event_bus(bus).notification_shutdown_timeout(timeout);
    builder.handlers_mut().register::<serde_json::Value, _>(TaskHandlerDescriptor {
        kind_id: "example.process".into(), payload_type_id: ModelIdBuf::try_from("example.TaskPayload")?, accepted_schema_versions: vec![1], cancellation_mode: CancellationMode::Cooperative,
    }, Arc::new(Handler { started }))?;
    Ok((builder.build().await?, receiver))
}

/// Waits for a persisted terminal state, using actual store evidence under a deadline.
async fn terminal(service: &TaskExecutionService, id: TaskId) -> Result<TaskState, Box<dyn Error>> {
    tokio::time::timeout(DEADLINE, async {
        loop {
            let summary = service.get(id).await?.expect("accepted task is retained");
            if summary.state.is_terminal() { return Ok::<_, TaskServiceError>(summary.state); }
            tokio::task::yield_now().await;
        }
    }).await?.map_err(Into::into)
}

/// Reads committed Redis wire records, providing independent XADD evidence.
fn wires(url: &str, namespace: &str) -> Result<Vec<serde_json::Value>, Box<dyn Error>> {
    let mut connection = redis::Client::open(url)?.get_connection()?;
    let reply: redis::streams::StreamRangeReply = redis::cmd("XRANGE").arg(stream_key(namespace, "task.lifecycle")).arg("-").arg("+").query(&mut connection)?;
    reply.ids.iter().map(|entry| {
        let wire: String = redis::from_redis_value(entry.map.get("wire").expect("provider wire field"))?;
        Ok(serde_json::from_str(&wire)?)
    }).collect()
}

/// Consumes the actual durable stream and applies each task revision once.
fn consume(url: &str, namespace: &str) -> Result<HashMap<(String, u64), TaskState>, Box<dyn Error>> {
    let records = wires(url, namespace)?;
    assert!(!records.is_empty(), "the real Redis stream must contain task events");
    let bus: EventBus = EventBusRegistry::discover()?.create(&config(url, namespace)?)?;
    let (sender, receiver) = mpsc::channel();
    let subscription = bus.subscribe(SubscribeRequest::builder()
        .subscriber_id(SubscriberId::new("outbox-consumer")?)
        .consumer_group(ConsumerGroup::new("outbox-projection")?)
        .topic(Topic::<TaskEvent>::new("task.lifecycle")?)
        .durability(SubscriptionDurability::Durable).start_position(StartPosition::Earliest).build()?,
        move |delivery| { let _ = sender.send(delivery.payload().clone()); Ok::<(), DeliveryError>(()) })?;
    let mut projection = HashMap::new();
    for _ in &records {
        let event = receiver.recv_timeout(DEADLINE)?;
        let key = (event.task_id.clone(), event.state_version);
        match projection.entry(key) {
            std::collections::hash_map::Entry::Vacant(entry) => { entry.insert(event.state); },
            std::collections::hash_map::Entry::Occupied(entry) => { assert_eq!(entry.get(), &event.state, "duplicates must preserve their immutable snapshot"); },
        }
    }
    subscription.cancel()?;
    let _report = bus.shutdown(ShutdownMode::Immediate)?;
    for (id, version) in projection.keys() {
        let stable_id = format!("task:{id}:{version}");
        assert!(records.iter().any(|wire| wire["event_id"].as_str() == Some(stable_id.as_str())), "Redis preserves the stable outbox event ID");
    }
    Ok(projection)
}

/// Verifies all three committed revisions of one successful typed task.
fn assert_success(projection: &HashMap<(String, u64), TaskState>, id: TaskId) {
    for (version, state) in [(0, TaskState::Queued), (1, TaskState::Running), (2, TaskState::Succeeded)] {
        assert_eq!(projection.get(&(id.to_string(), version)), Some(&state));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_typed_lifecycle_and_cancellation_reach_durable_consumer() -> TestResult {
    let redis = RedisServer::start()?;
    let namespace = "typed-outbox-lifecycle";
    let store = Arc::new(SqliteTaskStore::open_next(database_path())?);
    let bus = producer(redis.url(), namespace).await?;
    let (service, mut started) = service(store, bus.clone(), Duration::from_secs(5)).await?;
    let done = service.submit(typed_support::request(serde_json::json!({}), "success")).await?;
    assert_eq!(tokio::time::timeout(DEADLINE, started.recv()).await?, Some(done.id));
    assert_eq!(terminal(&service, done.id).await?, TaskState::Succeeded);
    let cancelled = service.submit(typed_support::request(serde_json::json!({"cancel": true}), "cancel")).await?;
    assert_eq!(tokio::time::timeout(DEADLINE, started.recv()).await?, Some(cancelled.id));
    let _outcome = service.cancel(cancelled.id).await?;
    assert_eq!(terminal(&service, cancelled.id).await?, TaskState::Cancelled);
    service.shutdown().await?;
    let projection = consume(redis.url(), namespace)?;
    assert_success(&projection, done.id);
    assert_eq!(projection.get(&(cancelled.id.to_string(), 0)), Some(&TaskState::Queued));
    assert_eq!(projection.get(&(cancelled.id.to_string(), 1)), Some(&TaskState::Running));
    assert_eq!(projection.get(&(cancelled.id.to_string(), 2)), Some(&TaskState::Running));
    assert_eq!(projection.get(&(cancelled.id.to_string(), 3)), Some(&TaskState::Cancelled));
    assert_eq!(projection.len(), 7);
    let _report = bus.shutdown(ShutdownMode::Immediate).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_redis_outage_retains_committed_events_until_restart() -> TestResult {
    let mut redis = RedisServer::start()?;
    let namespace = "typed-outbox-outage";
    redis.stop()?;
    let path = database_path();
    let store = Arc::new(SqliteTaskStore::open_next(&path)?);
    let bus = producer(redis.url(), namespace).await?;
    let (first, _) = service(store.clone(), bus.clone(), Duration::from_millis(100)).await?;
    let done = first.submit(typed_support::request(serde_json::json!({}), "outage")).await?;
    assert_eq!(terminal(&first, done.id).await?, TaskState::Succeeded);
    tokio::time::timeout(DEADLINE, async { while bus.publish_metrics().errors == 0 { tokio::task::yield_now().await; } }).await?;
    assert_eq!(store.list_event_outbox(128).await?.len(), 3);
    assert!(matches!(first.shutdown().await, Err(TaskServiceError::NotificationClose(_))));
    drop(first);
    drop(store);
    redis.restart()?;
    let reopened = Arc::new(SqliteTaskStore::open_next(&path)?);
    let fresh_bus = producer(redis.url(), namespace).await?;
    let (second, _) = service(reopened, fresh_bus.clone(), Duration::from_secs(5)).await?;
    second.shutdown().await?;
    let projection = consume(redis.url(), namespace)?;
    assert_success(&projection, done.id);
    assert_eq!(projection.len(), 3);
    let _report = bus.shutdown(ShutdownMode::Immediate).await?;
    let _report = fresh_bus.shutdown(ShutdownMode::Immediate).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_lost_xadd_reply_replays_the_same_event_identity() -> TestResult {
    let mut redis = RedisServer::start()?;
    let proxy = ControlledRedis::start(redis.url())?;
    let gate = proxy.pause_after_reply("XADD");
    let namespace = "typed-outbox-lost-reply";
    let store = Arc::new(SqliteTaskStore::open_next(database_path())?);
    let bus = producer(&proxy.url(), namespace).await?;
    let (first, _) = service(store.clone(), bus.clone(), Duration::from_millis(100)).await?;
    let done = first.submit(typed_support::request(serde_json::json!({}), "reply-loss")).await?;
    tokio::time::timeout(DEADLINE, gate.wait_applied()).await?;
    assert_eq!(terminal(&first, done.id).await?, TaskState::Succeeded);
    assert_eq!(wires(redis.url(), namespace)?.len(), 1, "XADD committed upstream while its reply is held");
    assert_eq!(store.list_event_outbox(128).await?.len(), 3);
    // Keep later retries unavailable while the first, already applied XADD loses its reply.
    // Docker completion and the facade error counter prove both fault boundaries.
    redis.stop()?;
    gate.release_without_reply();
    tokio::time::timeout(DEADLINE, async { while bus.publish_metrics().errors == 0 { tokio::task::yield_now().await; } }).await?;
    assert_eq!(store.list_event_outbox(128).await?.len(), 3);
    assert!(matches!(first.shutdown().await, Err(TaskServiceError::NotificationClose(_))));
    redis.restart()?;
    let fresh_bus = producer(redis.url(), namespace).await?;
    let (second, _) = service(store, fresh_bus.clone(), Duration::from_secs(5)).await?;
    second.shutdown().await?;
    let records = wires(redis.url(), namespace)?;
    assert_eq!(records.len(), 4);
    assert_eq!(records[0]["event_id"], records[1]["event_id"], "uncertain publication reuses the same EventId");
    let projection = consume(redis.url(), namespace)?;
    assert_success(&projection, done.id);
    assert_eq!(projection.len(), 3, "four deliveries apply only three revisions");
    let _report = bus.shutdown(ShutdownMode::Immediate).await?;
    let _report = fresh_bus.shutdown(ShutdownMode::Immediate).await?;
    Ok(())
}

/// Child-only entry: exit after confirmed Redis acceptance, before deleting any snapshot.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_outbox_crash_child() -> TestResult {
    let Ok(path) = std::env::var("TASK_OUTBOX_CRASH_DATABASE") else { return Ok(()); };
    let url = std::env::var("TASK_OUTBOX_CRASH_REDIS")?;
    let namespace = std::env::var("TASK_OUTBOX_CRASH_NAMESPACE")?;
    let inner = Arc::new(SqliteTaskStore::open_next(path)?);
    let store = Arc::new(InterruptBeforeMark { inner: inner.clone(), reached: tokio::sync::Notify::new() });
    let bus = producer(&url, &namespace).await?;
    let (service, _) = service(store.clone(), bus, Duration::from_secs(5)).await?;
    let done = service.submit(typed_support::request(serde_json::json!({}), "crash")).await?;
    assert_eq!(terminal(&service, done.id).await?, TaskState::Succeeded);
    tokio::time::timeout(DEADLINE, store.reached.notified()).await?;
    assert_eq!(inner.list_event_outbox(128).await?.len(), 3);
    std::process::exit(91);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_process_exit_after_publish_before_delete_recovers_outbox() -> TestResult {
    let redis = RedisServer::start()?;
    let namespace = "typed-outbox-process-crash";
    let path = database_path();
    let child = Command::new(std::env::current_exe()?)
        .args(["--exact", "test_outbox_crash_child", "--nocapture"])
        .env("TASK_OUTBOX_CRASH_DATABASE", &path)
        .env("TASK_OUTBOX_CRASH_REDIS", redis.url())
        .env("TASK_OUTBOX_CRASH_NAMESPACE", namespace)
        .output()?;
    assert_eq!(child.status.code(), Some(91), "child must reach the confirmed-publish / uncommitted-delete barrier: {} {}", String::from_utf8_lossy(&child.stdout), String::from_utf8_lossy(&child.stderr));
    assert_eq!(wires(redis.url(), namespace)?.len(), 1);
    let store = Arc::new(SqliteTaskStore::open_next(&path)?);
    let owner = store.acquire_owner().await?;
    assert_eq!(store.list_event_outbox(128).await?.len(), 3);
    store.release_owner(owner).await?;
    let bus = producer(redis.url(), namespace).await?;
    let (service, _) = service(store, bus.clone(), Duration::from_secs(5)).await?;
    service.shutdown().await?;
    let records = wires(redis.url(), namespace)?;
    assert_eq!(records.len(), 4);
    assert_eq!(records[0]["event_id"], records[1]["event_id"]);
    let projection = consume(redis.url(), namespace)?;
    assert_success(&projection, TaskId::from_id(qubit_id::Id::new(100)));
    assert_eq!(projection.len(), 3, "process restart must not apply a duplicate revision twice");
    let _report = bus.shutdown(ShutdownMode::Immediate).await?;
    Ok(())
}
