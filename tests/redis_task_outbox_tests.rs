// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Real typed service → SQLite outbox → Redis → duplicate-aware durable
//! consumer.
#![cfg(all(feature = "sqlite", feature = "event-bus"))]

mod redis_task_outbox_support;
use std::collections::HashMap;
use std::error::Error;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::sync::mpsc;
use std::time::Duration;

use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::DeliveryError;
use qubit_event_bus::EventBus;
use qubit_event_bus::EventBusConfig;
use qubit_event_bus::EventBusFacadeConfig;
use qubit_event_bus::EventBusRegistry;
use qubit_event_bus::codec::CodecRegistry;
use qubit_event_bus::model::ConsumerGroup;
use qubit_event_bus::model::FailureDirective;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::ProviderOptions;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::StartPosition;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::SubscriberId;
use qubit_event_bus::model::SubscriptionDurability;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus_redis::r#async::AsyncRedisEventBusProvider;
use qubit_event_bus_redis::naming::group_name;
use qubit_event_bus_redis::naming::stream_key;
use qubit_model_id::ModelIdBuf;
use qubit_spi::AsyncServiceProvider;
use qubit_spi::ProviderSelection;
use qubit_task::CancellationMode;
use qubit_task::TaskContext;
use qubit_task::TaskExecutionService;
use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::TaskHandler;
use qubit_task::TaskHandlerDescriptor;
use qubit_task::event::TaskEvent;
use qubit_task::handler::TaskRunOutcome;
use qubit_task::handler::TaskRunResult;
use qubit_task::model::TaskId;
use qubit_task::model::TaskOutput;
use qubit_task::model::TaskState;
use qubit_task::service::TaskServiceError;
use qubit_task::store::SqliteTaskStore;
use qubit_task::store::TaskFuture;
use qubit_task::store::TaskStore;
use redis_task_outbox_support::support;
use rusqlite::Connection;
use rusqlite::OptionalExtension;
use rusqlite::params;
use support::controlled_redis::proxy::ControlledRedis;
use support::interrupt_before_mark::InterruptBeforeMark;
use support::redis_server::RedisServer;
use support::task_event_codec::TaskEventJsonCodec;
use support::temporary_database::TemporaryDatabase;
use support::typed_support;

type TestResult = Result<(), Box<dyn Error>>;
type ProjectionResult<T> = Result<T, Box<dyn Error + Send + Sync>>;
type ProjectionSnapshot = (Option<(u64, TaskState)>, Vec<u64>);
const DEADLINE: Duration = Duration::from_secs(15);

/// Widens a thread-safe projection error for the integration test result.
fn test_error(error: Box<dyn Error + Send + Sync>) -> Box<dyn Error> {
    error
}

/// Owns a connection only for one consumer lifetime; the file survives reopen.
struct DurableProjection {
    connection: Connection,
}

impl DurableProjection {
    /// Opens the test projection and creates its checkpoint and effect ledger.
    fn open(path: &Path) -> ProjectionResult<Self> {
        let connection = Connection::open(path)?;
        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS task_projection (
                task_id TEXT PRIMARY KEY,
                highest_state_version INTEGER NOT NULL,
                state_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS projection_effects (
                task_id TEXT NOT NULL,
                state_version INTEGER NOT NULL,
                PRIMARY KEY (task_id, state_version)
            );",
        )?;
        Ok(Self { connection })
    }

    /// Commits the checkpoint and business effect together, consulting the
    /// task service for a missing initial or intermediate version.
    fn apply(
        &mut self,
        event: &TaskEvent,
        authoritative: impl FnOnce(TaskId) -> ProjectionResult<(u64, TaskState)>,
    ) -> ProjectionResult<()> {
        let transaction = self.connection.transaction()?;
        let id = event.task_id.to_padded_decimal();
        let checkpoint: Option<u64> = transaction
            .query_row(
                "SELECT highest_state_version FROM task_projection WHERE task_id = ?1",
                [&id],
                |row| row.get(0),
            )
            .optional()?;
        if checkpoint.is_some_and(|version| event.state_version <= version) {
            transaction.commit()?;
            return Ok(());
        }
        let next = checkpoint.map_or(0, |version| version + 1);
        let (version, state) = if event.state_version == next {
            (event.state_version, event.state.clone())
        } else {
            let (version, state) = authoritative(event.task_id)?;
            if version < event.state_version || checkpoint.is_some_and(|current| version <= current) {
                return Err(std::io::Error::other("task service did not resolve the version gap").into());
            }
            (version, state)
        };
        transaction.execute(
            "INSERT INTO task_projection (task_id, highest_state_version, state_json)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(task_id) DO UPDATE SET
               highest_state_version = excluded.highest_state_version,
               state_json = excluded.state_json",
            params![id, version, serde_json::to_string(&state)?],
        )?;
        transaction.execute(
            "INSERT INTO projection_effects (task_id, state_version) VALUES (?1, ?2)",
            params![id, version],
        )?;
        transaction.commit()?;
        Ok(())
    }

    /// Reads the committed state and effect versions from a fresh connection.
    fn snapshot(path: &Path, id: TaskId) -> ProjectionResult<ProjectionSnapshot> {
        let connection = Connection::open(path)?;
        let id = id.to_padded_decimal();
        let checkpoint: Option<(u64, String)> = connection
            .query_row(
                "SELECT highest_state_version, state_json FROM task_projection WHERE task_id = ?1",
                [&id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let mut statement = connection
            .prepare("SELECT state_version FROM projection_effects WHERE task_id = ?1 ORDER BY state_version")?;
        let versions = statement
            .query_map([&id], |row| row.get(0))?
            .collect::<Result<Vec<_>, _>>()?;
        let checkpoint = checkpoint
            .map(|(version, json)| Ok::<_, serde_json::Error>((version, serde_json::from_str(&json)?)))
            .transpose()?;
        Ok((checkpoint, versions))
    }
}

/// Configures identical wire codecs and stream namespaces for both facades.
fn config(url: &str, namespace: &str) -> Result<EventBusConfig, Box<dyn Error>> {
    let mut codecs = CodecRegistry::new();
    codecs.register(Arc::new(TaskEventJsonCodec::new()?))?;
    let options: ProviderOptions = [
        ("redis.url".into(), url.into()),
        ("redis.namespace".into(), namespace.into()),
        ("redis.claim_min_idle_ms".into(), "60000".into()),
    ]
    .into();
    Ok(EventBusConfig::default()
        .with_selection(ProviderSelection::named("redis-streams")?)
        .with_provider_options(options)
        .with_facade_config(EventBusFacadeConfig::new().with_codec_registry(Arc::new(codecs))))
}

/// Creates the production asynchronous Redis provider with the task event
/// codec.
async fn producer(url: &str, namespace: &str) -> Result<Arc<AsyncEventBus>, Box<dyn Error>> {
    let config = config(url, namespace)?;
    let spi = AsyncRedisEventBusProvider
        .create_configured(&config)
        .await
        .map_err(|failure| failure.into_error())?;
    Ok(Arc::new(AsyncEventBus::with_config(
        ProviderId::new("redis-streams")?,
        spi,
        config.facade_config().clone(),
    )?))
}

/// Finishes normal tasks or waits for explicit cooperative cancellation.
struct Handler {
    started: tokio::sync::mpsc::UnboundedSender<TaskId>,
}
impl TaskHandler<serde_json::Value> for Handler {
    fn run<'a>(&'a self, input: serde_json::Value, context: TaskContext) -> TaskFuture<'a, TaskRunResult> {
        Box::pin(async move {
            let _ = self.started.send(context.task_id());
            if input["cancel"].as_bool() == Some(true) {
                while !context.is_cancelled() {
                    tokio::task::yield_now().await;
                }
                Ok(TaskRunOutcome::Cancelled)
            } else {
                Ok(TaskRunOutcome::Succeeded(TaskOutput::default()))
            }
        })
    }
}

/// Builds the real typed service; returned channel identifies actual handler
/// start.
async fn service(
    store: Arc<dyn TaskStore>,
    bus: Arc<AsyncEventBus>,
    timeout: Duration,
) -> Result<(TaskExecutionService, tokio::sync::mpsc::UnboundedReceiver<TaskId>), Box<dyn Error>> {
    let (started, receiver) = tokio::sync::mpsc::unbounded_channel();
    let mut builder = TaskExecutionServiceBuilder::new(
        store,
        Arc::new(typed_support::codecs()?),
        Arc::new(typed_support::SequentialIds::new(100)),
    )
    .event_bus(bus)
    .notification_shutdown_timeout(timeout);
    builder.handlers_mut().register::<serde_json::Value, _>(
        TaskHandlerDescriptor {
            kind_id: "example.process".into(),
            payload_type_id: ModelIdBuf::try_from("example.TaskPayload")?,
            accepted_schema_versions: vec![1],
            cancellation_mode: CancellationMode::Cooperative,
        },
        Arc::new(Handler { started }),
    )?;
    Ok((builder.build().await?, receiver))
}

/// Waits for a persisted terminal state, using actual store evidence under a
/// deadline.
async fn terminal(service: &TaskExecutionService, id: TaskId) -> Result<TaskState, Box<dyn Error>> {
    tokio::time::timeout(DEADLINE, async {
        loop {
            let summary = service.get(id).await?.expect("accepted task is retained");
            if summary.state.is_terminal() {
                return Ok::<_, TaskServiceError>(summary.state);
            }
            tokio::task::yield_now().await;
        }
    })
    .await?
    .map_err(Into::into)
}

/// Reads committed Redis wire records, providing independent XADD evidence.
fn wires(url: &str, namespace: &str) -> Result<Vec<serde_json::Value>, Box<dyn Error>> {
    let mut connection = redis::Client::open(url)?.get_connection()?;
    let reply: redis::streams::StreamRangeReply = redis::cmd("XRANGE")
        .arg(stream_key(namespace, "task.lifecycle"))
        .arg("-")
        .arg("+")
        .query(&mut connection)?;
    reply
        .ids
        .iter()
        .map(|entry| {
            let wire: String = redis::from_redis_value(entry.map.get("wire").expect("provider wire field"))?;
            Ok(serde_json::from_str(&wire)?)
        })
        .collect()
}

/// Reads the consumer group's pending count after provider settlement.
fn pending(url: &str, namespace: &str, group: &str) -> Result<u64, Box<dyn Error>> {
    let mut connection = redis::Client::open(url)?.get_connection()?;
    let reply: Vec<redis::Value> = redis::cmd("XPENDING")
        .arg(stream_key(namespace, "task.lifecycle"))
        .arg(group_name(namespace, "task.lifecycle", "outbox-consumer", Some(group)))
        .query(&mut connection)?;
    Ok(redis::from_redis_value(&reply[0])?)
}

/// Runs one consumer instance and waits until every current wire is ACKed.
fn consume_durable(url: &str, namespace: &str, group: &str, path: &Path, service: &TaskExecutionService) -> TestResult {
    let expected = wires(url, namespace)?.len();
    assert!(expected > 0, "durable consumer requires real Redis records");
    let projection = Arc::new(std::sync::Mutex::new(
        DurableProjection::open(path).map_err(test_error)?,
    ));
    let bus: EventBus = EventBusRegistry::discover()?.create(&config(url, namespace)?)?;
    let handle = tokio::runtime::Handle::current();
    let authoritative_service = service.clone();
    let (sender, receiver) = mpsc::channel();
    let subscription = bus.subscribe(
        SubscribeRequest::builder()
            .subscriber_id(SubscriberId::new("outbox-consumer")?)
            .consumer_group(ConsumerGroup::new(group)?)
            .topic(Topic::<TaskEvent>::new("task.lifecycle")?)
            .durability(SubscriptionDurability::Durable)
            .start_position(StartPosition::Earliest)
            .error_handler(|_, _| FailureDirective::Requeue)
            .build()?,
        move |delivery| {
            let event = delivery.payload().clone();
            let result = projection.lock().expect("projection lock").apply(&event, |id| {
                let summary = handle
                    .block_on(authoritative_service.get(id))?
                    .ok_or_else(|| std::io::Error::other("task service has no task for the event"))?;
                Ok((summary.state_version, summary.state))
            });
            match result {
                Ok(()) => {
                    let _ = sender.send(());
                    Ok::<(), DeliveryError>(())
                }
                Err(source) => Err(DeliveryError::Handler { source }),
            }
        },
    )?;
    for _ in 0..expected {
        receiver.recv_timeout(DEADLINE)?;
    }
    let started = std::time::Instant::now();
    while pending(url, namespace, group)? != 0 {
        assert!(started.elapsed() < DEADLINE, "successful transactions must be ACKed");
        std::thread::sleep(Duration::from_millis(10));
    }
    subscription.cancel()?;
    let _report = bus.shutdown(ShutdownMode::Immediate)?;
    Ok(())
}

/// Consumes the actual durable stream and applies each task revision once.
fn consume(url: &str, namespace: &str) -> Result<HashMap<(TaskId, u64), TaskState>, Box<dyn Error>> {
    let records = wires(url, namespace)?;
    assert!(!records.is_empty(), "the real Redis stream must contain task events");
    let bus: EventBus = EventBusRegistry::discover()?.create(&config(url, namespace)?)?;
    let (sender, receiver) = mpsc::channel();
    let subscription = bus.subscribe(
        SubscribeRequest::builder()
            .subscriber_id(SubscriberId::new("outbox-consumer")?)
            .consumer_group(ConsumerGroup::new("outbox-projection")?)
            .topic(Topic::<TaskEvent>::new("task.lifecycle")?)
            .durability(SubscriptionDurability::Durable)
            .start_position(StartPosition::Earliest)
            .build()?,
        move |delivery| {
            let _ = sender.send(delivery.payload().clone());
            Ok::<(), DeliveryError>(())
        },
    )?;
    let mut projection = HashMap::new();
    for _ in &records {
        let event = receiver.recv_timeout(DEADLINE)?;
        let key = (event.task_id, event.state_version);
        match projection.entry(key) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(event.state);
            }
            std::collections::hash_map::Entry::Occupied(entry) => {
                assert_eq!(
                    entry.get(),
                    &event.state,
                    "duplicates must preserve their immutable snapshot"
                );
            }
        }
    }
    subscription.cancel()?;
    let _report = bus.shutdown(ShutdownMode::Immediate)?;
    for (id, version) in projection.keys() {
        let stable_id = format!("task:{id}:{version}");
        assert!(
            records
                .iter()
                .any(|wire| wire["event_id"].as_str() == Some(stable_id.as_str())),
            "Redis preserves the stable outbox event ID"
        );
    }
    Ok(projection)
}

/// Verifies all three committed revisions of one successful typed task.
fn assert_success(projection: &HashMap<(TaskId, u64), TaskState>, id: TaskId) {
    for (version, state) in [
        (0, TaskState::Queued),
        (1, TaskState::Running),
        (2, TaskState::Succeeded),
    ] {
        assert_eq!(projection.get(&(id, version)), Some(&state));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_typed_lifecycle_and_cancellation_reach_durable_consumer() -> TestResult {
    let redis = RedisServer::start()?;
    let namespace = "typed-outbox-lifecycle";
    let database = TemporaryDatabase::new();
    let store = Arc::new(SqliteTaskStore::open_next(database.path())?);
    let bus = producer(redis.url(), namespace).await?;
    let (service, mut started) = service(store, bus.clone(), Duration::from_secs(5)).await?;
    let done = service
        .submit(typed_support::request(serde_json::json!({}), "success"))
        .await?;
    assert_eq!(tokio::time::timeout(DEADLINE, started.recv()).await?, Some(done.id));
    assert_eq!(terminal(&service, done.id).await?, TaskState::Succeeded);
    let cancelled = service
        .submit(typed_support::request(serde_json::json!({"cancel": true}), "cancel"))
        .await?;
    assert_eq!(
        tokio::time::timeout(DEADLINE, started.recv()).await?,
        Some(cancelled.id)
    );
    let _outcome = service.cancel(cancelled.id).await?;
    assert_eq!(terminal(&service, cancelled.id).await?, TaskState::Cancelled);
    service.shutdown().await?;
    let projection = consume(redis.url(), namespace)?;
    assert_success(&projection, done.id);
    assert_eq!(projection.get(&(cancelled.id, 0)), Some(&TaskState::Queued));
    assert_eq!(projection.get(&(cancelled.id, 1)), Some(&TaskState::Running));
    assert_eq!(projection.get(&(cancelled.id, 2)), Some(&TaskState::Running));
    assert_eq!(projection.get(&(cancelled.id, 3)), Some(&TaskState::Cancelled));
    assert_eq!(projection.len(), 7);
    let _report = bus.shutdown(ShutdownMode::Immediate).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_redis_outage_retains_committed_events_until_restart() -> TestResult {
    let mut redis = RedisServer::start()?;
    let namespace = "typed-outbox-outage";
    redis.stop()?;
    let database = TemporaryDatabase::new();
    let path = database.path();
    let store = Arc::new(SqliteTaskStore::open_next(path)?);
    let bus = producer(redis.url(), namespace).await?;
    let (first, _) = service(store.clone(), bus.clone(), Duration::from_millis(100)).await?;
    let done = first
        .submit(typed_support::request(serde_json::json!({}), "outage"))
        .await?;
    assert_eq!(terminal(&first, done.id).await?, TaskState::Succeeded);
    tokio::time::timeout(DEADLINE, async {
        while bus.publish_metrics().errors == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert_eq!(store.list_event_outbox(128).await?.len(), 3);
    assert!(matches!(
        first.shutdown().await,
        Err(TaskServiceError::NotificationClose(_))
    ));
    drop(first);
    drop(store);
    redis.restart()?;
    let reopened = Arc::new(SqliteTaskStore::open_next(path)?);
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
    let database = TemporaryDatabase::new();
    let projection_database = TemporaryDatabase::new();
    let store = Arc::new(SqliteTaskStore::open_next(database.path())?);
    let bus = producer(&proxy.url(), namespace).await?;
    let (first, _) = service(store.clone(), bus.clone(), Duration::from_millis(100)).await?;
    let done = first
        .submit(typed_support::request(serde_json::json!({}), "reply-loss"))
        .await?;
    tokio::time::timeout(DEADLINE, gate.wait_applied()).await?;
    assert_eq!(terminal(&first, done.id).await?, TaskState::Succeeded);
    assert_eq!(
        wires(redis.url(), namespace)?.len(),
        1,
        "XADD committed upstream while its reply is held"
    );
    assert_eq!(store.list_event_outbox(128).await?.len(), 3);
    // Keep later retries unavailable while the first, already applied XADD loses
    // its reply. Docker completion and the facade error counter prove both
    // fault boundaries.
    redis.stop()?;
    gate.release_without_reply();
    tokio::time::timeout(DEADLINE, async {
        while bus.publish_metrics().errors == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert_eq!(store.list_event_outbox(128).await?.len(), 3);
    assert!(matches!(
        first.shutdown().await,
        Err(TaskServiceError::NotificationClose(_))
    ));
    redis.restart()?;
    let fresh_bus = producer(redis.url(), namespace).await?;
    let (second, _) = service(store, fresh_bus.clone(), Duration::from_secs(5)).await?;
    second.shutdown().await?;
    let records = wires(redis.url(), namespace)?;
    assert_eq!(records.len(), 4);
    assert_eq!(
        records[0]["event_id"], records[1]["event_id"],
        "uncertain publication reuses the same EventId"
    );
    let projection = consume(redis.url(), namespace)?;
    assert_success(&projection, done.id);
    assert_eq!(projection.len(), 3, "four deliveries apply only three revisions");
    consume_durable(
        redis.url(),
        namespace,
        "persisted-first",
        projection_database.path(),
        &second,
    )?;
    let first_snapshot = DurableProjection::snapshot(projection_database.path(), done.id).map_err(test_error)?;
    assert_eq!(first_snapshot.0, Some((2, TaskState::Succeeded)));
    assert_eq!(first_snapshot.1, vec![0, 1, 2]);
    consume_durable(
        redis.url(),
        namespace,
        "persisted-replay",
        projection_database.path(),
        &second,
    )?;
    assert_eq!(
        DurableProjection::snapshot(projection_database.path(), done.id).map_err(test_error)?,
        first_snapshot
    );
    let _report = bus.shutdown(ShutdownMode::Immediate).await?;
    let _report = fresh_bus.shutdown(ShutdownMode::Immediate).await?;
    Ok(())
}

/// Child-only entry: exit after confirmed Redis acceptance, before deleting any
/// snapshot.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_outbox_crash_child() -> TestResult {
    let Ok(path) = std::env::var("TASK_OUTBOX_CRASH_DATABASE") else {
        return Ok(());
    };
    let url = std::env::var("TASK_OUTBOX_CRASH_REDIS")?;
    let namespace = std::env::var("TASK_OUTBOX_CRASH_NAMESPACE")?;
    let inner = Arc::new(SqliteTaskStore::open_next(path)?);
    let store = Arc::new(InterruptBeforeMark {
        inner: inner.clone(),
        reached: tokio::sync::Notify::new(),
    });
    let bus = producer(&url, &namespace).await?;
    let (service, _) = service(store.clone(), bus, Duration::from_secs(5)).await?;
    let done = service
        .submit(typed_support::request(serde_json::json!({}), "crash"))
        .await?;
    assert_eq!(terminal(&service, done.id).await?, TaskState::Succeeded);
    tokio::time::timeout(DEADLINE, store.reached.notified()).await?;
    assert_eq!(inner.list_event_outbox(128).await?.len(), 3);
    std::process::exit(91);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_process_exit_after_publish_before_delete_recovers_outbox() -> TestResult {
    let redis = RedisServer::start()?;
    let namespace = "typed-outbox-process-crash";
    let database = TemporaryDatabase::new();
    let projection_database = TemporaryDatabase::new();
    let path = database.path();
    let child = Command::new(std::env::current_exe()?)
        .args(["--exact", "test_outbox_crash_child", "--nocapture"])
        .env("TASK_OUTBOX_CRASH_DATABASE", path)
        .env("TASK_OUTBOX_CRASH_REDIS", redis.url())
        .env("TASK_OUTBOX_CRASH_NAMESPACE", namespace)
        .output()?;
    assert_eq!(
        child.status.code(),
        Some(91),
        "child must reach the confirmed-publish / uncommitted-delete barrier: {} {}",
        String::from_utf8_lossy(&child.stdout),
        String::from_utf8_lossy(&child.stderr)
    );
    assert_eq!(wires(redis.url(), namespace)?.len(), 1);
    let store = Arc::new(SqliteTaskStore::open_next(path)?);
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
    let task_id = TaskId::from_id(qubit_id::Id::new(100));
    assert_success(&projection, task_id);
    assert_eq!(
        projection.len(),
        3,
        "process restart must not apply a duplicate revision twice"
    );
    consume_durable(
        redis.url(),
        namespace,
        "persisted-first",
        projection_database.path(),
        &service,
    )?;
    let first_snapshot = DurableProjection::snapshot(projection_database.path(), task_id).map_err(test_error)?;
    assert_eq!(first_snapshot.0, Some((2, TaskState::Succeeded)));
    assert_eq!(first_snapshot.1, vec![0, 1, 2]);
    consume_durable(
        redis.url(),
        namespace,
        "persisted-replay",
        projection_database.path(),
        &service,
    )?;
    assert_eq!(
        DurableProjection::snapshot(projection_database.path(), task_id).map_err(test_error)?,
        first_snapshot
    );
    let _report = bus.shutdown(ShutdownMode::Immediate).await?;
    Ok(())
}

/// Builds a synthetic notification for the projection edge cases.
fn projection_event(id: TaskId, version: u64, state: TaskState) -> TaskEvent {
    TaskEvent {
        schema_version: 1,
        task_id: id,
        state_version: version,
        state,
        correlation_key: None,
    }
}

#[test]
fn test_durable_projection_ignores_stale_version_after_reopen() -> TestResult {
    let database = TemporaryDatabase::new();
    let id = TaskId::from_id(qubit_id::Id::new(301));
    let mut first = DurableProjection::open(database.path()).map_err(test_error)?;
    first
        .apply(&projection_event(id, 3, TaskState::Running), |_| {
            Ok((3, TaskState::Succeeded))
        })
        .map_err(test_error)?;
    drop(first);
    let before = DurableProjection::snapshot(database.path(), id).map_err(test_error)?;
    let mut reopened = DurableProjection::open(database.path()).map_err(test_error)?;
    reopened
        .apply(&projection_event(id, 2, TaskState::Running), |_| {
            panic!("stale notification must not query the service")
        })
        .map_err(test_error)?;
    assert_eq!(
        DurableProjection::snapshot(database.path(), id).map_err(test_error)?,
        before
    );
    assert_eq!(before, (Some((3, TaskState::Succeeded)), vec![3]));
    Ok(())
}

#[test]
fn test_durable_projection_refreshes_first_and_intermediate_version_gaps() -> TestResult {
    let database = TemporaryDatabase::new();
    let first_id = TaskId::from_id(qubit_id::Id::new(302));
    let jump_id = TaskId::from_id(qubit_id::Id::new(303));
    let mut projection = DurableProjection::open(database.path()).map_err(test_error)?;
    let mut queries = 0;
    projection
        .apply(&projection_event(first_id, 3, TaskState::Running), |_| {
            queries += 1;
            Ok((3, TaskState::Succeeded))
        })
        .map_err(test_error)?;
    projection
        .apply(&projection_event(jump_id, 1, TaskState::Running), |_| {
            queries += 1;
            Ok((1, TaskState::Running))
        })
        .map_err(test_error)?;
    projection
        .apply(&projection_event(jump_id, 3, TaskState::Running), |_| {
            queries += 1;
            Ok((3, TaskState::Succeeded))
        })
        .map_err(test_error)?;
    assert_eq!(
        queries, 3,
        "every first or intermediate gap requires an authoritative lookup"
    );
    assert_eq!(
        DurableProjection::snapshot(database.path(), first_id).map_err(test_error)?,
        (Some((3, TaskState::Succeeded)), vec![3])
    );
    assert_eq!(
        DurableProjection::snapshot(database.path(), jump_id).map_err(test_error)?,
        (Some((3, TaskState::Succeeded)), vec![1, 3])
    );
    Ok(())
}

#[test]
fn test_failed_authority_query_rolls_back_projection_transaction() -> TestResult {
    let database = TemporaryDatabase::new();
    let id = TaskId::from_id(qubit_id::Id::new(304));
    let mut projection = DurableProjection::open(database.path()).map_err(test_error)?;
    projection
        .apply(&projection_event(id, 0, TaskState::Queued), |_| {
            panic!("version zero needs no service lookup")
        })
        .map_err(test_error)?;
    let before = DurableProjection::snapshot(database.path(), id).map_err(test_error)?;
    let failure = projection.apply(&projection_event(id, 3, TaskState::Running), |_| {
        Err(std::io::Error::other("task service query failed").into())
    });
    assert!(failure.is_err(), "failed service lookup must fail the transaction");
    assert_eq!(
        DurableProjection::snapshot(database.path(), id).map_err(test_error)?,
        before
    );
    Ok(())
}

#[test]
fn test_failed_effect_insert_rolls_back_checkpoint_and_allows_retry() -> TestResult {
    let database = TemporaryDatabase::new();
    let id = TaskId::from_id(qubit_id::Id::new(306));
    let mut projection = DurableProjection::open(database.path()).map_err(test_error)?;
    projection
        .apply(&projection_event(id, 0, TaskState::Queued), |_| {
            panic!("version zero needs no service lookup")
        })
        .map_err(test_error)?;
    let before = DurableProjection::snapshot(database.path(), id).map_err(test_error)?;
    projection.connection.execute_batch(
        "CREATE TRIGGER fail_projection_effect
         BEFORE INSERT ON projection_effects
         WHEN NEW.state_version = 1
         BEGIN SELECT RAISE(FAIL, 'injected projection effect failure'); END;",
    )?;

    let failure = projection
        .apply(&projection_event(id, 1, TaskState::Running), |_| {
            panic!("contiguous version needs no service lookup")
        })
        .expect_err("effect write failure must abort projection transaction");
    assert!(failure.to_string().contains("injected projection effect failure"));
    assert_eq!(
        DurableProjection::snapshot(database.path(), id).map_err(test_error)?,
        before,
        "checkpoint and business effect must both roll back"
    );

    projection
        .connection
        .execute_batch("DROP TRIGGER fail_projection_effect")?;
    projection
        .apply(&projection_event(id, 1, TaskState::Running), |_| {
            panic!("contiguous version needs no service lookup")
        })
        .map_err(test_error)?;
    assert_eq!(
        DurableProjection::snapshot(database.path(), id).map_err(test_error)?,
        (Some((1, TaskState::Running)), vec![0, 1]),
        "retry must commit the checkpoint and effect exactly once"
    );
    Ok(())
}

#[test]
fn test_failed_authority_query_keeps_redis_delivery_pending() -> TestResult {
    let redis = RedisServer::start()?;
    let namespace = "typed-outbox-failed-query";
    let group = "failed-query";
    let database = TemporaryDatabase::new();
    let projection = Arc::new(std::sync::Mutex::new(
        DurableProjection::open(database.path()).map_err(test_error)?,
    ));
    let bus: EventBus = EventBusRegistry::discover()?.create(&config(redis.url(), namespace)?)?;
    let topic = Topic::<TaskEvent>::new("task.lifecycle")?;
    let (sender, receiver) = mpsc::channel();
    let subscription = bus.subscribe(
        SubscribeRequest::builder()
            .subscriber_id(SubscriberId::new("outbox-consumer")?)
            .consumer_group(ConsumerGroup::new(group)?)
            .topic(topic.clone())
            .durability(SubscriptionDurability::Durable)
            .start_position(StartPosition::Earliest)
            .error_handler(|_, _| FailureDirective::Requeue)
            .build()?,
        move |delivery| {
            let failure = projection
                .lock()
                .expect("projection lock")
                .apply(delivery.payload(), |_| {
                    Err(std::io::Error::other("task service query failed").into())
                });
            let _ = sender.send(());
            Err::<(), DeliveryError>(DeliveryError::Handler {
                source: failure.expect_err("version gap must query the failed service"),
            })
        },
    )?;
    let id = TaskId::from_id(qubit_id::Id::new(305));
    let _receipt = bus.publish(PublishRequest::new(topic, projection_event(id, 3, TaskState::Running))?)?;
    receiver.recv_timeout(DEADLINE)?;
    let started = std::time::Instant::now();
    while bus.delivery_metrics().completed == 0 {
        assert!(
            started.elapsed() < DEADLINE,
            "failed handler must finish Retry settlement"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    while pending(redis.url(), namespace, group)? == 0 {
        assert!(
            started.elapsed() < DEADLINE,
            "failed transaction must retain a Redis PEL entry"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(
        DurableProjection::snapshot(database.path(), id).map_err(test_error)?,
        (None, vec![])
    );
    subscription.cancel()?;
    let _report = bus.shutdown(ShutdownMode::Immediate)?;
    Ok(())
}
