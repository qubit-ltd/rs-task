use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use qubit_codec::ValueBytesCodecDescriptor;
use qubit_codec::ValueBytesCodecRegistry;
use qubit_codec::ValueCodecId;
use qubit_codec::ValueCodecRegistration;
use qubit_codec::ValueCodecRegistrationSource;
use qubit_model_metadata::metadata::ModelId;
use qubit_model_metadata::metadata::ModelIdBuf;
use qubit_task::CancellationMode;
use qubit_task::TaskContext;
use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::TaskHandler;
use qubit_task::TaskHandlerDescriptor;
use qubit_task::handler::TaskRunOutcome;
use qubit_task::model::ResourceCapacity;
use qubit_task::model::ResourceRequest;
use qubit_task::model::StoredPayload;
use qubit_task::model::StoredTaskRequest;
use qubit_task::model::TaskId;
use qubit_task::model::TaskQuery;
use qubit_task::model::TaskRequest;
use qubit_task::model::TaskRunError;
use qubit_task::model::TaskState;
use qubit_task::service::CancelOutcome;
use qubit_task::store::MemoryTaskStore;
use qubit_task::store::TaskFuture;
use qubit_task::store::TaskStore;

#[derive(Default)]
struct U32Codec;

impl qubit_codec::ValueEncoder<u32> for U32Codec {
    type Output = Vec<u8>;
    type Error = std::convert::Infallible;
    fn encode(&mut self, value: &u32) -> Result<Vec<u8>, Self::Error> {
        Ok(value.to_le_bytes().to_vec())
    }
}

impl qubit_codec::ValueDecoder<[u8]> for U32Codec {
    type Output = u32;
    type Error = std::array::TryFromSliceError;
    fn decode(&mut self, bytes: &[u8]) -> Result<u32, Self::Error> {
        let raw: [u8; 4] = bytes.try_into()?;
        Ok(u32::from_le_bytes(raw))
    }
}

static CODEC_DESCRIPTOR: ValueBytesCodecDescriptor = ValueBytesCodecDescriptor::of::<U32Codec, u32>();
static CODEC_REGISTRATION: qubit_codec::ValueBytesCodecRegistration = ValueCodecRegistration::new(
    ValueCodecId::new("qubit_task.typed_service.u32"),
    &CODEC_DESCRIPTOR,
    ValueCodecRegistrationSource::new(
        "qubit-task",
        "typed_service_tests",
        "tests/service/typed_task_execution_tests.rs",
        1,
    ),
);

struct Ids(AtomicU64);

impl qubit_id::IdGenerator for Ids {
    fn generate(&self) -> Result<qubit_id::Id, qubit_id::IdGenerationError> {
        Ok(qubit_id::Id::new(self.0.fetch_add(1, Ordering::Relaxed)))
    }
}

struct FailingIds;

impl qubit_id::IdGenerator for FailingIds {
    fn generate(&self) -> Result<qubit_id::Id, qubit_id::IdGenerationError> {
        Err(qubit_id::IdGenerationError::NodeOutOfRange { node_id: 4, max: 3 })
    }
}

struct Handler {
    cooperative_cancel: bool,
}

impl TaskHandler<u32> for Handler {
    fn run<'a>(&'a self, value: u32, context: TaskContext) -> TaskFuture<'a, qubit_task::handler::TaskRunResult> {
        let cancel = self.cooperative_cancel;
        Box::pin(async move {
            assert_eq!(value, 42);
            if cancel {
                while !context.is_cancelled() {
                    tokio::task::yield_now().await;
                }
                Ok(TaskRunOutcome::Cancelled)
            } else {
                Ok(TaskRunOutcome::Succeeded(qubit_task::model::TaskOutput {
                    summary: b"typed-result".to_vec(),
                }))
            }
        })
    }
}

struct PendingHandler;

impl TaskHandler<u32> for PendingHandler {
    fn run<'a>(&'a self, _value: u32, _context: TaskContext) -> TaskFuture<'a, qubit_task::handler::TaskRunResult> {
        Box::pin(std::future::pending())
    }
}

fn descriptor(mode: CancellationMode) -> TaskHandlerDescriptor {
    TaskHandlerDescriptor {
        kind_id: "test.typed-service".into(),
        payload_type_id: ModelIdBuf::parse("test.TypedServicePayload").unwrap(),
        accepted_schema_versions: vec![1, 2],
        cancellation_mode: mode,
    }
}

fn registry() -> Arc<ValueBytesCodecRegistry> {
    Arc::new(ValueBytesCodecRegistry::from_registrations([&CODEC_REGISTRATION]).unwrap())
}

fn request() -> TaskRequest<u32> {
    let mut request = TaskRequest::new(
        "test.typed-service",
        ModelId::new("test.TypedServicePayload"),
        2,
        ValueCodecId::new("qubit_task.typed_service.u32"),
        42,
    );
    request.resource_limit.cpu_slots = 1;
    request
}

async fn wait_for_terminal(service: &qubit_task::TaskExecutionService, id: qubit_task::model::TaskId) -> TaskState {
    for _ in 0..1000 {
        if let Some(summary) = service.get(id).await.unwrap()
            && summary.state.is_terminal()
        {
            return summary.state;
        }
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    panic!("typed task did not reach a terminal state")
}

#[tokio::test]
async fn typed_submit_decodes_runs_and_persists_terminal_state() {
    let store = Arc::new(MemoryTaskStore::new(16));
    let mut builder = TaskExecutionServiceBuilder::new(store, registry(), Arc::new(Ids(AtomicU64::new(101)))).capacity(
        ResourceCapacity {
            cpu_slots: 1,
            ..ResourceCapacity::default()
        },
    );
    builder
        .handlers_mut()
        .register::<u32, _>(
            descriptor(CancellationMode::Cooperative),
            Arc::new(Handler {
                cooperative_cancel: false,
            }),
        )
        .unwrap();
    let service = builder.build().await.unwrap();
    let accepted = service.submit(request()).await.unwrap();
    assert_eq!(wait_for_terminal(&service, accepted.id).await, TaskState::Succeeded);
    let completed = service.get(accepted.id).await.unwrap().unwrap();
    assert_eq!(completed.output.unwrap().summary, b"typed-result");
}

#[tokio::test]
async fn typed_running_cancel_is_persisted_then_acknowledged_by_handler() {
    let store = Arc::new(MemoryTaskStore::new(16));
    let mut builder = TaskExecutionServiceBuilder::new(store, registry(), Arc::new(Ids(AtomicU64::new(201)))).capacity(
        ResourceCapacity {
            cpu_slots: 1,
            ..ResourceCapacity::default()
        },
    );
    builder
        .handlers_mut()
        .register::<u32, _>(
            descriptor(CancellationMode::Cooperative),
            Arc::new(Handler {
                cooperative_cancel: true,
            }),
        )
        .unwrap();
    let service = builder.build().await.unwrap();
    let accepted = service.submit(request()).await.unwrap();
    for _ in 0..1000 {
        if service
            .get(accepted.id)
            .await
            .unwrap()
            .is_some_and(|summary| matches!(summary.state, TaskState::Running))
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    assert_eq!(
        service.cancel(accepted.id).await.unwrap(),
        CancelOutcome::CancellationRequested
    );
    assert_eq!(wait_for_terminal(&service, accepted.id).await, TaskState::Cancelled);
}

#[tokio::test]
async fn missing_handler_is_retained_as_blocked_and_can_be_cancelled() {
    let store = Arc::new(MemoryTaskStore::new(16));
    let service = TaskExecutionServiceBuilder::new(store, registry(), Arc::new(Ids(AtomicU64::new(301))))
        .build()
        .await
        .unwrap();
    let accepted = service.submit(request()).await.unwrap();
    for _ in 0..1000 {
        if service
            .get(accepted.id)
            .await
            .unwrap()
            .is_some_and(|summary| matches!(summary.state, TaskState::Blocked { .. }))
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    assert_eq!(
        service.cancel(accepted.id).await.unwrap(),
        CancelOutcome::CancelledBeforeStart
    );
    assert_eq!(wait_for_terminal(&service, accepted.id).await, TaskState::Cancelled);
}

#[tokio::test]
async fn id_generation_failure_prevents_acceptance() {
    let store = Arc::new(MemoryTaskStore::new(16));
    let service = TaskExecutionServiceBuilder::new(store.clone(), registry(), Arc::new(FailingIds))
        .build()
        .await
        .unwrap();
    assert!(service.submit(request()).await.is_err());
    let page = store
        .list_encoded(TaskQuery {
            limit: 10,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(page.records.is_empty());
}

#[tokio::test]
async fn unsupported_schema_is_retained_as_blocked() {
    let store = Arc::new(MemoryTaskStore::new(16));
    let mut builder = TaskExecutionServiceBuilder::new(store, registry(), Arc::new(Ids(AtomicU64::new(401))));
    builder
        .handlers_mut()
        .register::<u32, _>(
            descriptor(CancellationMode::Cooperative),
            Arc::new(Handler {
                cooperative_cancel: false,
            }),
        )
        .unwrap();
    let service = builder.build().await.unwrap();
    let mut unsupported = request();
    unsupported.payload.schema_version = 3;
    let accepted = service.submit(unsupported).await.unwrap();
    let state = wait_for_terminal_or_blocked(&service, accepted.id).await;
    assert!(matches!(state, TaskState::Blocked { .. }));
}

async fn wait_for_terminal_or_blocked(service: &qubit_task::TaskExecutionService, id: TaskId) -> TaskState {
    for _ in 0..1000 {
        if let Some(summary) = service.get(id).await.unwrap()
            && (summary.state.is_terminal() || matches!(summary.state, TaskState::Blocked { .. }))
        {
            return summary.state;
        }
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    panic!("typed task did not reach a blocked or terminal state")
}

#[tokio::test]
async fn queued_typed_task_can_be_cancelled_before_resource_admission() {
    let store = Arc::new(MemoryTaskStore::new(16));
    let mut builder = TaskExecutionServiceBuilder::new(store, registry(), Arc::new(Ids(AtomicU64::new(501)))).capacity(
        ResourceCapacity {
            cpu_slots: 1,
            ..ResourceCapacity::default()
        },
    );
    builder
        .handlers_mut()
        .register::<u32, _>(descriptor(CancellationMode::Cooperative), Arc::new(PendingHandler))
        .unwrap();
    let service = builder.build().await.unwrap();
    let running = service.submit(request()).await.unwrap();
    for _ in 0..1000 {
        if service
            .get(running.id)
            .await
            .unwrap()
            .is_some_and(|summary| matches!(summary.state, TaskState::Running))
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    let queued = service.submit(request()).await.unwrap();
    assert_eq!(
        service.cancel(queued.id).await.unwrap(),
        CancelOutcome::CancelledBeforeStart
    );
    assert_eq!(wait_for_terminal(&service, queued.id).await, TaskState::Cancelled);
}

#[tokio::test]
async fn running_handler_without_cancel_support_reports_unsupported() {
    let store = Arc::new(MemoryTaskStore::new(16));
    let mut builder = TaskExecutionServiceBuilder::new(store, registry(), Arc::new(Ids(AtomicU64::new(601))));
    builder
        .handlers_mut()
        .register::<u32, _>(descriptor(CancellationMode::Unsupported), Arc::new(PendingHandler))
        .unwrap();
    let service = builder.build().await.unwrap();
    let accepted = service.submit(request()).await.unwrap();
    for _ in 0..1000 {
        if service
            .get(accepted.id)
            .await
            .unwrap()
            .is_some_and(|summary| matches!(summary.state, TaskState::Running))
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    assert_eq!(
        service.cancel(accepted.id).await.unwrap(),
        CancelOutcome::CancellationUnsupported
    );
}

#[tokio::test]
async fn repeated_idempotency_key_returns_the_existing_typed_task() {
    let store = Arc::new(MemoryTaskStore::new(16));
    let mut builder = TaskExecutionServiceBuilder::new(store, registry(), Arc::new(Ids(AtomicU64::new(801))));
    builder
        .handlers_mut()
        .register::<u32, _>(
            descriptor(CancellationMode::Cooperative),
            Arc::new(Handler {
                cooperative_cancel: false,
            }),
        )
        .unwrap();
    let service = builder.build().await.unwrap();
    let mut first = request();
    first.idempotency_key = Some("client-key-1".into());
    let mut repeated = request();
    repeated.idempotency_key = Some("client-key-1".into());
    let accepted = service.submit(first).await.unwrap();
    let existing = service.submit(repeated).await.unwrap();
    assert_eq!(accepted.id, existing.id);
}

#[tokio::test]
async fn recovery_blocks_unsupported_schema_and_missing_codec() {
    let store = Arc::new(MemoryTaskStore::new(16));
    for (value, version, codec) in [(901, 99, "qubit_task.typed_service.u32"), (902, 1, "missing.codec")] {
        store
            .accept_encoded(
                TaskId::from_id(qubit_id::Id::new(value)),
                StoredTaskRequest {
                    kind_id: "test.typed-service".into(),
                    category: Some("recovery".into()),
                    payload: StoredPayload {
                        type_id: ModelIdBuf::parse("test.TypedServicePayload").unwrap(),
                        schema_version: version,
                        codec_id: codec.into(),
                        bytes: 42_u32.to_le_bytes().to_vec(),
                    },
                    metadata: qubit_metadata::Metadata::new(),
                    resource_limit: ResourceRequest::default(),
                    correlation_key: None,
                    idempotency_key: None,
                },
            )
            .await
            .unwrap();
    }
    let mut builder = TaskExecutionServiceBuilder::new(store, registry(), Arc::new(Ids(AtomicU64::new(903))));
    builder
        .handlers_mut()
        .register::<u32, _>(
            descriptor(CancellationMode::Cooperative),
            Arc::new(Handler {
                cooperative_cancel: false,
            }),
        )
        .unwrap();
    let service = builder.build().await.unwrap();
    for value in [901, 902] {
        let state = wait_for_terminal_or_blocked(&service, TaskId::from_id(qubit_id::Id::new(value))).await;
        assert!(matches!(state, TaskState::Blocked { .. }));
    }
}

#[tokio::test]
async fn external_cancel_hook_failure_remains_queryable() {
    let store = Arc::new(MemoryTaskStore::new(16));
    let mut builder = TaskExecutionServiceBuilder::new(store, registry(), Arc::new(Ids(AtomicU64::new(701))));
    let hook_calls = Arc::new(AtomicU64::new(0));
    let hook_call_counter = Arc::clone(&hook_calls);
    let hook: qubit_task::ExternalCancellationHook = Arc::new(move |_, _| {
        hook_call_counter.fetch_add(1, Ordering::Relaxed);
        Box::pin(async {
            Err(TaskRunError {
                category: "cancel_failed".into(),
                message: "remote cancellation failed".into(),
                retryable: false,
            })
        })
    });
    builder
        .handlers_mut()
        .register_with_cancellation_hook::<u32, _>(
            descriptor(CancellationMode::ExternalHook),
            Arc::new(PendingHandler),
            "test",
            hook,
        )
        .unwrap();
    let service = builder.build().await.unwrap();
    let accepted = service.submit(request()).await.unwrap();
    for _ in 0..1000 {
        if service
            .get(accepted.id)
            .await
            .unwrap()
            .is_some_and(|summary| matches!(summary.state, TaskState::Running))
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    assert!(service.cancel(accepted.id).await.is_err());
    assert!(service.cancel(accepted.id).await.is_err());
    assert_eq!(hook_calls.load(Ordering::Relaxed), 1);
    let summary = service.get(accepted.id).await.unwrap().unwrap();
    assert!(summary.cancel_requested);
    assert_eq!(summary.cancel_error.as_deref(), Some("remote cancellation failed"));
}

#[tokio::test]
async fn shutdown_rejects_new_work_and_waits_for_running_attempts() {
    let store = Arc::new(MemoryTaskStore::new(16));
    let mut builder = TaskExecutionServiceBuilder::new(store, registry(), Arc::new(Ids(AtomicU64::new(751))));
    builder
        .handlers_mut()
        .register::<u32, _>(
            descriptor(CancellationMode::Cooperative),
            Arc::new(Handler {
                cooperative_cancel: true,
            }),
        )
        .unwrap();
    let service = builder.build().await.unwrap();
    let accepted = service.submit(request()).await.unwrap();
    for _ in 0..1000 {
        if service
            .get(accepted.id)
            .await
            .unwrap()
            .is_some_and(|summary| matches!(summary.state, TaskState::Running))
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    let shutdown_service = service.clone();
    let shutdown = tokio::spawn(async move { shutdown_service.shutdown().await });
    assert_eq!(
        service.cancel(accepted.id).await.unwrap(),
        CancelOutcome::CancellationRequested
    );
    shutdown.await.unwrap().unwrap();
    assert!(matches!(
        service.submit(request()).await,
        Err(qubit_task::service::TaskServiceError::ShuttingDown)
    ));
    assert_eq!(wait_for_terminal(&service, accepted.id).await, TaskState::Cancelled);
}

#[tokio::test]
async fn shutdown_fences_cancel_calls_from_previous_owner() {
    let store: Arc<dyn TaskStore> = Arc::new(MemoryTaskStore::new(16));
    let first_builder =
        TaskExecutionServiceBuilder::new(Arc::clone(&store), registry(), Arc::new(Ids(AtomicU64::new(761))));
    let first = first_builder.build().await.unwrap();
    first.shutdown().await.unwrap();

    let mut second_builder = TaskExecutionServiceBuilder::new(store, registry(), Arc::new(Ids(AtomicU64::new(762))));
    second_builder
        .handlers_mut()
        .register::<u32, _>(
            descriptor(CancellationMode::Cooperative),
            Arc::new(Handler {
                cooperative_cancel: true,
            }),
        )
        .unwrap();
    let second = second_builder.build().await.unwrap();
    let accepted = second.submit(request()).await.unwrap();
    for _ in 0..1000 {
        if second
            .get(accepted.id)
            .await
            .unwrap()
            .is_some_and(|summary| matches!(summary.state, TaskState::Running))
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }

    let before = second.get(accepted.id).await.unwrap().unwrap();
    assert!(matches!(
        first.cancel(accepted.id).await,
        Err(qubit_task::service::TaskServiceError::ShuttingDown)
    ));
    let after = second.get(accepted.id).await.unwrap().unwrap();
    assert_eq!(after, before);

    assert_eq!(
        second.cancel(accepted.id).await.unwrap(),
        CancelOutcome::CancellationRequested
    );
    assert_eq!(wait_for_terminal(&second, accepted.id).await, TaskState::Cancelled);
    second.shutdown().await.unwrap();
}

#[tokio::test]
async fn shutdown_waits_for_external_cancel_hook() {
    struct HookWaitHandler(Arc<tokio::sync::Notify>);

    impl TaskHandler<u32> for HookWaitHandler {
        fn run<'a>(&'a self, _value: u32, _context: TaskContext) -> TaskFuture<'a, qubit_task::handler::TaskRunResult> {
            let hook_finished = Arc::clone(&self.0);
            Box::pin(async move {
                hook_finished.notified().await;
                Ok(TaskRunOutcome::Succeeded(qubit_task::model::TaskOutput::default()))
            })
        }
    }

    let store = Arc::new(MemoryTaskStore::new(16));
    let hook_started = Arc::new(tokio::sync::Notify::new());
    let hook_release = Arc::new(tokio::sync::Notify::new());
    let hook_finished = Arc::new(tokio::sync::Notify::new());
    let hook_signal = Arc::clone(&hook_started);
    let hook_wait = Arc::clone(&hook_release);
    let handler_signal = Arc::clone(&hook_finished);
    let hook: qubit_task::ExternalCancellationHook = Arc::new(move |_, _| {
        let hook_signal = Arc::clone(&hook_signal);
        let hook_wait = Arc::clone(&hook_wait);
        let handler_signal = Arc::clone(&handler_signal);
        Box::pin(async move {
            hook_signal.notify_one();
            hook_wait.notified().await;
            handler_signal.notify_one();
            Ok(())
        })
    });
    let mut builder = TaskExecutionServiceBuilder::new(store, registry(), Arc::new(Ids(AtomicU64::new(771))));
    builder
        .handlers_mut()
        .register_with_cancellation_hook::<u32, _>(
            descriptor(CancellationMode::ExternalHook),
            Arc::new(HookWaitHandler(Arc::clone(&hook_finished))),
            "test",
            hook,
        )
        .unwrap();
    let service = builder.build().await.unwrap();
    let accepted = service.submit(request()).await.unwrap();
    for _ in 0..1000 {
        if service
            .get(accepted.id)
            .await
            .unwrap()
            .is_some_and(|summary| matches!(summary.state, TaskState::Running))
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }

    let cancel_service = service.clone();
    let cancel = tokio::spawn(async move { cancel_service.cancel(accepted.id).await });
    hook_started.notified().await;
    let shutdown_service = service.clone();
    let shutdown = tokio::spawn(async move { shutdown_service.shutdown().await });
    tokio::task::yield_now().await;
    assert!(!shutdown.is_finished());
    hook_release.notify_one();
    assert_eq!(cancel.await.unwrap().unwrap(), CancelOutcome::CancellationRequested);
    shutdown.await.unwrap().unwrap();
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn typed_service_persists_sqlite_terminal_transition() {
    let path = std::env::temp_dir().join(format!(
        "qubit-task-typed-service-{}.sqlite",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    ));
    let store: Arc<dyn TaskStore> = Arc::new(qubit_task::store::SqliteTaskStore::open_next(&path).unwrap());
    let mut builder =
        TaskExecutionServiceBuilder::new(Arc::clone(&store), registry(), Arc::new(Ids(AtomicU64::new(1001))));
    builder
        .handlers_mut()
        .register::<u32, _>(
            descriptor(CancellationMode::Cooperative),
            Arc::new(Handler {
                cooperative_cancel: false,
            }),
        )
        .unwrap();
    let service = builder.build().await.unwrap();
    let accepted = service.submit(request()).await.unwrap();
    assert_eq!(wait_for_terminal(&service, accepted.id).await, TaskState::Succeeded);
    service.shutdown().await.unwrap();
    drop(service);
    drop(store);
    let mut lock_path = path.as_os_str().to_owned();
    lock_path.push(".owner.lock");
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(std::path::PathBuf::from(lock_path));
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn typed_service_fences_build_until_shutdown_releases_owner() {
    let path = std::env::temp_dir().join(format!(
        "qubit-task-typed-owner-{}.sqlite",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    ));
    let store: Arc<dyn TaskStore> = Arc::new(qubit_task::store::SqliteTaskStore::open_next(&path).unwrap());
    let build =
        || TaskExecutionServiceBuilder::new(Arc::clone(&store), registry(), Arc::new(Ids(AtomicU64::new(1101))));
    let first = build().build().await.unwrap();
    let second = build().build().await;
    assert!(matches!(
        second,
        Err(qubit_task::service::TaskServiceError::Store(
            qubit_task::store::StoreError::OwnerConflict
        ))
    ));
    first.shutdown().await.unwrap();
    let third = build().build().await.unwrap();
    third.shutdown().await.unwrap();
    drop(third);
    drop(first);
    drop(store);
    let mut lock_path = path.as_os_str().to_owned();
    lock_path.push(".owner.lock");
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(std::path::PathBuf::from(lock_path));
}
