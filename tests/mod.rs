// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;

use qubit_task::TaskExecutionService;
use qubit_task::engine::EngineError;
use qubit_task::engine::ExecutionHandle;
use qubit_task::engine::LocalTaskExecutionEngine;
use qubit_task::engine::PreparedExecution;
use qubit_task::engine::TaskExecutionEngine;
use qubit_task::handler::TaskContext;
use qubit_task::handler::TaskHandler;
use qubit_task::handler::TaskHandlerDescriptor;
use qubit_task::handler::TaskHandlerRegistry;
use qubit_task::model::AcceptOutcome;
use qubit_task::model::ResourceCapacity;
use qubit_task::model::ResourceRequest;
use qubit_task::model::ResourceSnapshot;
use qubit_task::model::TaskId;
use qubit_task::model::TaskOutput;
use qubit_task::model::TaskQuery;
use qubit_task::model::TaskRequest;
use qubit_task::model::TaskRunError;
use qubit_task::model::TaskState;
use qubit_task::model::TransitionCommand;
use qubit_task::scheduling::FairFifoPolicy;
use qubit_task::scheduling::QueueSnapshot;
use qubit_task::scheduling::QueuedTask;
use qubit_task::scheduling::SchedulingPolicy;
use qubit_task::store::MemoryTaskStore;
use qubit_task::store::StoreError;
use qubit_task::store::TaskStore;

struct EchoHandler;

impl TaskHandler for EchoHandler {
    fn descriptor(&self) -> TaskHandlerDescriptor {
        TaskHandlerDescriptor {
            task_type: "echo".into(),
            version: "1".into(),
        }
    }
    fn run<'a>(
        &'a self,
        payload: &'a [u8],
        _context: TaskContext,
    ) -> qubit_task::store::TaskFuture<'a, Result<TaskOutput, TaskRunError>> {
        Box::pin(async move {
            assert!(!_context.task_id().to_string().is_empty());
            assert!(_context.attempt() > 0);
            let _ = _context.assigned_resources();
            let _ = _context.cancellation_signal();
            Ok(TaskOutput {
                summary: payload.to_vec(),
            })
        })
    }
}

struct PanicHandler;

impl TaskHandler for PanicHandler {
    fn descriptor(&self) -> TaskHandlerDescriptor {
        TaskHandlerDescriptor {
            task_type: "panic".into(),
            version: "1".into(),
        }
    }

    fn run<'a>(
        &'a self,
        _payload: &'a [u8],
        _context: TaskContext,
    ) -> qubit_task::store::TaskFuture<'a, Result<TaskOutput, TaskRunError>> {
        Box::pin(async { panic!("handler panic") })
    }
}

struct CooperativeHandler;

impl TaskHandler for CooperativeHandler {
    fn descriptor(&self) -> TaskHandlerDescriptor {
        TaskHandlerDescriptor {
            task_type: "cooperative".into(),
            version: "1".into(),
        }
    }
    fn run<'a>(
        &'a self,
        _payload: &'a [u8],
        context: TaskContext,
    ) -> qubit_task::store::TaskFuture<'a, Result<TaskOutput, TaskRunError>> {
        Box::pin(async move {
            while !context.is_cancelled() {
                tokio::time::sleep(std::time::Duration::from_millis(2)).await;
            }
            Ok(TaskOutput::default())
        })
    }
}

struct ExternalEngine {
    inner: LocalTaskExecutionEngine,
}

impl TaskExecutionEngine for ExternalEngine {
    fn capacity(&self) -> ResourceSnapshot {
        self.inner.capacity()
    }

    fn prepare<'a>(
        &'a self,
        id: TaskId,
        request: ResourceRequest,
    ) -> qubit_task::store::TaskFuture<'a, Result<PreparedExecution, EngineError>> {
        self.inner.prepare(id, request)
    }

    fn activate<'a>(
        &'a self,
        mut prepared: PreparedExecution,
        handler: Arc<dyn TaskHandler>,
        payload: Vec<u8>,
        context: TaskContext,
    ) -> qubit_task::store::TaskFuture<'a, Result<ExecutionHandle, EngineError>> {
        Box::pin(async move {
            let release = prepared.take_release();
            let cancellation = context.cancellation_signal();
            let (sender, receiver) = tokio::sync::oneshot::channel();
            tokio::spawn(async move {
                let _release = ReleaseOnDrop(release);
                let result = handler.run(&payload, context).await;
                let _ = sender.send(result);
            });
            Ok(ExecutionHandle::new(receiver, cancellation))
        })
    }
}

struct ReleaseOnDrop(Option<Box<dyn FnOnce() + Send>>);

impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        if let Some(release) = self.0.take() {
            release();
        }
    }
}

#[tokio::test]
async fn test_in_memory_service_accepts_and_completes_a_local_task() {
    let service = TaskExecutionService::in_memory()
        .await
        .expect("volatile service builds");
    assert!(!service.capabilities().store.restart_recovery);
    let id = service
        .submit_local(|_| {
            Ok(TaskOutput {
                summary: b"done".to_vec(),
            })
        })
        .await
        .expect("local task is accepted");
    let record = service.wait(id).await.expect("task reaches terminal state");
    assert_eq!(record.state, TaskState::Succeeded);
    assert_eq!(record.output.expect("output retained").summary, b"done");
    assert_eq!(
        service.cancel(id).await.expect("terminal cancellation query succeeds"),
        qubit_task::service::CancelOutcome::AlreadyTerminal
    );
    service.shutdown().await.expect("service shuts down");
}

#[tokio::test]
async fn test_local_task_panic_is_recorded_as_panicked() {
    let service = TaskExecutionService::in_memory().await.expect("service builds");
    let id = service
        .submit_local(|_| panic!("local task panic"))
        .await
        .expect("task is accepted");
    let record = service.wait(id).await.expect("panic is terminal");
    assert!(matches!(record.state, TaskState::Panicked { .. }));
    service.shutdown().await.expect("service shuts down");
}

#[tokio::test]
async fn test_local_engine_reserves_and_releases_all_requested_resources() {
    let mut capacity = ResourceCapacity {
        cpu_slots: 2,
        ..ResourceCapacity::default()
    };
    capacity.gpus.insert("gpu0".into(), vec!["cuda".into()]);
    capacity.custom.insert("license".into(), 1);
    let engine = LocalTaskExecutionEngine::new(capacity);
    let request = ResourceRequest {
        cpu_slots: 1,
        gpu_count: 1,
        gpu_labels: vec!["cuda".into()],
        custom: [("license".into(), 1)].into_iter().collect(),
    };
    let first = engine
        .prepare(TaskId::generate(), request.clone())
        .await
        .expect("first reservation succeeds");
    assert_eq!(first.assigned_resources(), &["gpu0"]);
    assert!(matches!(
        engine.prepare(TaskId::generate(), request.clone()).await,
        Err(qubit_task::engine::EngineError::TemporarilyUnavailable)
    ));
    drop(first);
    assert!(engine.prepare(TaskId::generate(), request).await.is_ok());
}

#[test]
fn test_prepared_execution_public_constructor_and_release_paths() {
    let id = TaskId::generate();
    let released = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let released_on_take = released.clone();
    let mut prepared = PreparedExecution::new(id, vec!["gpu-test".into()], move || {
        released_on_take.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
    });
    assert_eq!(prepared.task_id(), id);
    assert_eq!(prepared.assigned_resources(), &["gpu-test"]);
    prepared.take_release().expect("release callback is present")();
    drop(prepared);
    assert_eq!(released.load(std::sync::atomic::Ordering::Acquire), 1);

    let released_on_drop = released.clone();
    drop(PreparedExecution::new(id, Vec::new(), move || {
        released_on_drop.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
    }));
    assert_eq!(released.load(std::sync::atomic::Ordering::Acquire), 2);
}

#[tokio::test]
async fn test_versioned_handler_runs_reconstructable_request() {
    let mut registry = TaskHandlerRegistry::new();
    registry.register(Arc::new(EchoHandler)).expect("handler registers");
    assert!(registry.resolve("echo", "1").is_some());
    assert!(registry.resolve("echo", "2").is_none());
    let service = qubit_task::TaskExecutionServiceBuilder::in_memory()
        .register_handler(Arc::new(EchoHandler))
        .expect("handler registration succeeds")
        .build()
        .await
        .expect("service builds");
    let request = TaskRequest::new("echo", "1", b"payload".to_vec());
    let accepted = service.submit(request).await.expect("task accepted");
    let finished = service.wait(accepted.id).await.expect("task completes");
    assert_eq!(finished.state, TaskState::Succeeded);
    assert_eq!(finished.output.expect("output present").summary, b"payload");
    service.shutdown().await.expect("service shuts down");
}

#[tokio::test]
async fn test_service_query_listing_stats_and_unknown_cancellation() {
    let service = qubit_task::TaskExecutionServiceBuilder::in_memory()
        .register_handler(Arc::new(EchoHandler))
        .expect("handler registers")
        .build()
        .await
        .expect("service builds");
    let unknown = service.cancel(TaskId::generate()).await.unwrap_err();
    assert!(matches!(
        unknown,
        qubit_task::service::TaskServiceError::Store(StoreError::NotFound)
    ));

    let first = service
        .submit(TaskRequest::new("echo", "1", b"one".to_vec()))
        .await
        .unwrap();
    let second = service
        .submit(TaskRequest::new("echo", "1", b"two".to_vec()))
        .await
        .unwrap();
    let page = service
        .list(TaskQuery {
            limit: 1,
            ..TaskQuery::default()
        })
        .await
        .unwrap();
    assert_eq!(page.records.len(), 1);
    assert!(page.next.is_some());
    let first_result = service.wait(first.id).await.unwrap();
    let second_result = service.wait(second.id).await.unwrap();
    assert!(first_result.state.is_terminal() && second_result.state.is_terminal());
    let terminal = service
        .list(TaskQuery {
            states: vec![TaskState::Succeeded],
            limit: 8,
            ..TaskQuery::default()
        })
        .await
        .unwrap();
    assert_eq!(terminal.records.len(), 2);
    assert_eq!(service.stats().await.unwrap().terminal, 2);
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_async_handler_panic_is_recorded_and_resources_are_released() {
    let service = qubit_task::TaskExecutionServiceBuilder::in_memory()
        .register_handler(Arc::new(PanicHandler))
        .expect("handler registration succeeds")
        .build()
        .await
        .expect("service builds");
    let accepted = service
        .submit(TaskRequest::new("panic", "1", Vec::new()))
        .await
        .expect("task accepted");
    let finished = service.wait(accepted.id).await.expect("panic is terminal");
    assert!(matches!(finished.state, TaskState::Panicked { .. }));
    assert_eq!(
        service.stats().await.expect("stats available").resources.used_cpu_slots,
        0
    );
    service.shutdown().await.expect("service shuts down");
}

#[tokio::test]
async fn test_service_idempotency_returns_the_original_task_id() {
    let service = TaskExecutionService::in_memory().await.expect("service builds");
    let mut request = TaskRequest::new("echo", "1", b"same".to_vec());
    request.idempotency_key = Some("same-request".into());
    let first = service
        .submit(request.clone())
        .await
        .expect("first request is accepted");
    let second = service.submit(request).await.expect("duplicate request resolves");
    assert_eq!(first.id, second.id);
    service.shutdown().await.expect("service shuts down");
}

#[tokio::test]
async fn test_external_execution_engine_can_implement_public_contract() {
    let capacity = ResourceCapacity {
        cpu_slots: 1,
        ..ResourceCapacity::default()
    };
    let service = qubit_task::TaskExecutionServiceBuilder::from_components(
        Arc::new(MemoryTaskStore::new(8)),
        Arc::new(ExternalEngine {
            inner: LocalTaskExecutionEngine::new(capacity),
        }),
        Arc::new(FairFifoPolicy::default()),
    )
    .register_handler(Arc::new(EchoHandler))
    .expect("handler registration succeeds")
    .build()
    .await
    .expect("service builds with an external engine");
    let accepted = service
        .submit(TaskRequest::new("echo", "1", b"external".to_vec()))
        .await
        .expect("request is accepted");
    let finished = service.wait(accepted.id).await.expect("task completes");
    assert_eq!(finished.state, TaskState::Succeeded);
    service.shutdown().await.expect("service shuts down");
}

#[tokio::test]
async fn test_missing_handler_transitions_task_to_blocked() {
    let service = TaskExecutionService::in_memory().await.expect("service builds");
    let accepted = service
        .submit(TaskRequest::new("missing", "1", Vec::new()))
        .await
        .expect("request accepted");
    assert!(service.wait(accepted.id).await.is_err());
    let record = service
        .get(accepted.id)
        .await
        .expect("query succeeds")
        .expect("record remains");
    assert!(matches!(record.state, TaskState::Blocked { .. }));
    let retried = service
        .retry_blocked(accepted.id)
        .await
        .expect("blocked task can be requeued");
    assert_eq!(retried.state, TaskState::Queued);
    assert!(service.wait(accepted.id).await.is_err());
    let blocked_again = service
        .get(accepted.id)
        .await
        .expect("query succeeds")
        .expect("record remains");
    assert!(matches!(blocked_again.state, TaskState::Blocked { .. }));
}

#[tokio::test]
async fn test_running_task_cancellation_is_cooperative_and_terminal() {
    let service = qubit_task::TaskExecutionServiceBuilder::in_memory()
        .register_handler(Arc::new(CooperativeHandler))
        .expect("handler registration succeeds")
        .build()
        .await
        .expect("service builds");
    let accepted = service
        .submit(TaskRequest::new("cooperative", "1", Vec::new()))
        .await
        .expect("task accepted");
    loop {
        if service
            .get(accepted.id)
            .await
            .expect("query succeeds")
            .is_some_and(|record| matches!(record.state, TaskState::Running))
        {
            break;
        }
        tokio::task::yield_now().await;
    }
    let outcome = service
        .cancel(accepted.id)
        .await
        .expect("cancellation request succeeds");
    assert_eq!(outcome, qubit_task::service::CancelOutcome::CancellationRequested);
    let record = service.wait(accepted.id).await.expect("handler observes cancellation");
    assert_eq!(record.state, TaskState::Cancelled);
    service.shutdown().await.expect("service shuts down");
}

#[tokio::test]
async fn test_memory_store_is_idempotent_and_rejects_illegal_transitions() {
    let store = MemoryTaskStore::new(8);
    let id = TaskId::generate();
    let mut request = TaskRequest::new("echo", "1", Vec::new());
    request.idempotency_key = Some("request-1".into());
    let accepted = store.accept(id, request.clone()).await.expect("request accepted");
    assert!(matches!(accepted, AcceptOutcome::Accepted(_)));
    assert!(
        store
            .find_idempotent(request.clone())
            .await
            .expect("lookup succeeds")
            .is_some()
    );
    let mut conflict = request.clone();
    conflict.payload = b"different".to_vec();
    assert!(matches!(
        store.find_idempotent(conflict).await,
        Err(StoreError::IdempotencyConflict)
    ));
    let error = store
        .transition(TransitionCommand {
            id,
            expected_version: 0,
            expected_attempt: 0,
            state: TaskState::Succeeded,
            output: None,
            assigned_resources: Vec::new(),
            cancel_requested: false,
        })
        .await
        .expect_err("queued work cannot skip execution");
    assert!(matches!(error, StoreError::InvalidTransition));
}

#[tokio::test]
async fn test_task_state_and_record_resource_contracts() {
    let queued = TaskState::Queued;
    assert!(queued.allows_transition_to(&TaskState::Running));
    assert!(queued.allows_transition_to(&TaskState::Cancelled));
    assert!(!queued.allows_transition_to(&TaskState::Succeeded));
    assert!(
        TaskState::Blocked {
            reason: "needs repair".into()
        }
        .allows_transition_to(&TaskState::Queued)
    );
    assert!(!TaskState::Succeeded.allows_transition_to(&TaskState::Running));
    assert!(!TaskState::Running.is_terminal());
    assert!(TaskState::Cancelled.is_terminal());

    let store = MemoryTaskStore::new(2);
    let request = TaskRequest::new("echo", "1", Vec::new());
    let accepted = store
        .accept(TaskId::generate(), request.clone())
        .await
        .expect("accept succeeds");
    let record = match accepted {
        AcceptOutcome::Accepted(record) => record,
        _ => panic!("task was not accepted"),
    };
    assert_eq!(record.resource_request(), &request.resources);
}

#[tokio::test]
async fn test_memory_store_paginates_and_can_drop_terminal_history() {
    let store = MemoryTaskStore::new(0);
    let mut request = TaskRequest::new("echo", "1", Vec::new());
    request.correlation_key = Some("batch-a".into());
    let id = TaskId::generate();
    let accepted = store.accept(id, request.clone()).await.unwrap();
    let record = match accepted {
        AcceptOutcome::Accepted(record) => record,
        _ => panic!("first request is new"),
    };
    assert!(matches!(
        store.accept(id, request.clone()).await,
        Err(StoreError::DuplicateTask)
    ));
    assert_eq!(store.find_idempotent(request.clone()).await.unwrap(), None);
    assert_eq!(store.get(TaskId::generate()).await.unwrap(), None);

    let page = store
        .list(TaskQuery {
            correlation_key: Some("batch-a".into()),
            limit: 1,
            ..TaskQuery::default()
        })
        .await
        .unwrap();
    assert_eq!(page.records[0].id, id);
    assert!(page.next.is_none());

    let running = store
        .transition(TransitionCommand {
            id,
            expected_version: record.state_version,
            expected_attempt: record.attempt,
            state: TaskState::Running,
            output: None,
            assigned_resources: Vec::new(),
            cancel_requested: false,
        })
        .await
        .unwrap();
    let finished = store
        .transition(TransitionCommand {
            id,
            expected_version: running.state_version,
            expected_attempt: running.attempt,
            state: TaskState::Succeeded,
            output: None,
            assigned_resources: Vec::new(),
            cancel_requested: false,
        })
        .await
        .expect("transition returns final record even with zero history capacity");
    assert_eq!(finished.state, TaskState::Succeeded);
    assert_eq!(store.get(id).await.unwrap(), None);
    assert!(matches!(
        store.acquire_owner().await,
        Err(StoreError::UnsupportedCapability)
    ));
    assert!(matches!(
        store.scan_unfinished(None).await,
        Err(StoreError::UnsupportedCapability)
    ));
    assert!(matches!(
        store.release_owner(qubit_task::model::OwnerEpoch(1)).await,
        Err(StoreError::UnsupportedCapability)
    ));
}

#[test]
fn test_handler_registry_rejects_invalid_and_duplicate_descriptors() {
    struct InvalidHandler;
    impl TaskHandler for InvalidHandler {
        fn descriptor(&self) -> TaskHandlerDescriptor {
            TaskHandlerDescriptor {
                task_type: String::new(),
                version: "1".into(),
            }
        }
        fn run<'a>(
            &'a self,
            _payload: &'a [u8],
            _context: TaskContext,
        ) -> qubit_task::store::TaskFuture<'a, Result<TaskOutput, TaskRunError>> {
            Box::pin(async { Ok(TaskOutput::default()) })
        }
    }

    let mut registry = TaskHandlerRegistry::new();
    assert!(registry.register(Arc::new(InvalidHandler)).is_err());
    registry
        .register(Arc::new(EchoHandler))
        .expect("first handler registers");
    let duplicate = registry
        .register_with_source(Arc::new(EchoHandler), "second provider")
        .unwrap_err();
    assert!(duplicate.to_string().contains("direct registration"));
    assert!(registry.resolve("echo", "missing").is_none());
}

#[tokio::test]
async fn test_zero_capacity_queue_rejects_without_accepting() {
    let service = qubit_task::TaskExecutionServiceBuilder::in_memory()
        .queue_capacity(0)
        .build()
        .await
        .expect("service builds");
    let error = service
        .submit_local(|_| Ok(TaskOutput::default()))
        .await
        .expect_err("zero-capacity queue rejects work");
    assert!(matches!(error, qubit_task::service::TaskServiceError::QueueFull));
}

#[tokio::test]
async fn test_builder_requires_explicit_storage_and_recovery_capability() {
    let missing = qubit_task::TaskExecutionServiceBuilder::default()
        .build()
        .await
        .err()
        .expect("generic builder must not choose storage implicitly");
    assert!(matches!(
        missing,
        qubit_task::service::TaskServiceBuildError::MissingStore
    ));

    let recovery = qubit_task::TaskExecutionServiceBuilder::in_memory()
        .require_recovery(true)
        .build()
        .await
        .err()
        .expect("recovery cannot silently fall back to memory");
    assert!(matches!(
        recovery,
        qubit_task::service::TaskServiceBuildError::RecoveryRequired
    ));

    let mut handlers = TaskHandlerRegistry::new();
    handlers.register(Arc::new(EchoHandler)).expect("handler registers");
    let service = qubit_task::TaskExecutionServiceBuilder::in_memory()
        .handlers(handlers)
        .capacity(ResourceCapacity {
            cpu_slots: 1,
            ..ResourceCapacity::default()
        })
        .queue_capacity(4)
        .scan_budget(0)
        .max_attempts(0)
        .build()
        .await
        .expect("configured builder succeeds");
    service.shutdown().await.expect("service shuts down");
}

#[test]
fn test_memory_store_reports_non_recovery_capabilities() {
    let store = MemoryTaskStore::new(4);
    let capabilities = store.capabilities();
    assert!(!capabilities.persistent_history);
    assert!(!capabilities.restart_recovery);
}

#[test]
fn test_fair_fifo_policy_orders_fit_candidates_and_protects_starved_head() {
    let make_task = |name: &str, bypasses| QueuedTask {
        id: qubit_task::TaskId::generate(),
        request: TaskRequest::new(name, "1", Vec::new()),
        bypasses,
    };
    let head = make_task("gpu", 3);
    let small = make_task("cpu", 0);
    let mut head = head;
    head.request.resources.gpu_count = 1;
    let snapshot = QueueSnapshot {
        tasks: vec![head.clone(), small.clone()],
        scan_budget: 8,
    };
    let policy = FairFifoPolicy::new(3);
    let resources = ResourceSnapshot {
        capacity: ResourceCapacity {
            cpu_slots: 1,
            ..ResourceCapacity::default()
        },
        ..ResourceSnapshot::default()
    };
    let ordered = policy.order(&snapshot, &resources);
    assert_eq!(ordered.first(), Some(&head.id));
    let policy = FairFifoPolicy::new(10);
    assert_eq!(policy.order(&snapshot, &resources).first(), Some(&small.id));
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_sqlite_store_recovers_interrupted_running_task() {
    let path = std::env::temp_dir().join(format!("qubit-task-{}.sqlite", qubit_task::TaskId::generate()));
    let store = qubit_task::store::SqliteTaskStore::open(&path).expect("SQLite store opens");
    let accepted = store
        .accept(
            qubit_task::TaskId::generate(),
            TaskRequest::new("echo", "1", b"restart".to_vec()),
        )
        .await
        .expect("task accepted");
    let record = match accepted {
        qubit_task::model::AcceptOutcome::Accepted(record) => record,
        _ => panic!("first request is new"),
    };
    store
        .transition(qubit_task::model::TransitionCommand {
            id: record.id,
            expected_version: 0,
            expected_attempt: 0,
            state: TaskState::Running,
            output: None,
            assigned_resources: Vec::new(),
            cancel_requested: false,
        })
        .await
        .expect("running state persisted");
    drop(store);
    let service = qubit_task::TaskExecutionServiceBuilder::recoverable_sqlite(&path)
        .expect("recoverable service config")
        .register_handler(Arc::new(EchoHandler))
        .expect("handler registers")
        .build()
        .await
        .expect("recovery completes before service returns");
    let recovered = service.wait(record.id).await.expect("recovered task completes");
    assert_eq!(recovered.state, TaskState::Succeeded);
    assert_eq!(recovered.attempt, 2);
    service.shutdown().await.expect("service shuts down");
    drop(service);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("owner.lock"));
    let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
    let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_sqlite_store_idempotency_state_filters_and_cursor_queries() {
    use qubit_task::store::SqliteTaskStore;

    let path = std::env::temp_dir().join(format!("qubit-task-query-{}.sqlite", TaskId::generate()));
    let store = SqliteTaskStore::open(&path).expect("SQLite store opens");
    let mut request = TaskRequest::new("echo", "1", Vec::new());
    request.correlation_key = Some("query-batch".into());
    request.idempotency_key = Some("query-key".into());
    let first_id = TaskId::generate();
    let first = store.accept(first_id, request.clone()).await.unwrap();
    assert!(matches!(first, AcceptOutcome::Accepted(_)));
    assert!(matches!(
        store.accept(TaskId::generate(), request.clone()).await.unwrap(),
        AcceptOutcome::Existing(_)
    ));
    let mut conflicting = request.clone();
    conflicting.payload = b"different".to_vec();
    assert!(matches!(
        store.accept(TaskId::generate(), conflicting).await,
        Err(StoreError::IdempotencyConflict)
    ));
    assert!(matches!(
        store.accept(first_id, TaskRequest::new("other", "1", Vec::new())).await,
        Err(StoreError::Failure(_))
    ));

    let second = store
        .accept(TaskId::generate(), TaskRequest::new("echo", "1", Vec::new()))
        .await
        .unwrap();
    let second_record = match second {
        AcceptOutcome::Accepted(record) => record,
        _ => panic!("task is new"),
    };
    let page = store
        .list(TaskQuery {
            limit: 1,
            ..TaskQuery::default()
        })
        .await
        .unwrap();
    assert_eq!(page.records.len(), 1);
    assert!(page.next.is_some());
    let filtered = store
        .list(TaskQuery {
            limit: 8,
            states: vec![TaskState::Queued],
            correlation_key: Some("query-batch".into()),
            ..TaskQuery::default()
        })
        .await
        .unwrap();
    assert_eq!(filtered.records.len(), 1);
    let after = store
        .list(TaskQuery {
            limit: 8,
            after: Some(first_id),
            ..TaskQuery::default()
        })
        .await
        .unwrap();
    assert!(after.records.iter().all(|record| record.id > first_id));

    assert!(matches!(
        store
            .transition(TransitionCommand {
                id: second_record.id,
                expected_version: 99,
                expected_attempt: 0,
                state: TaskState::Cancelled,
                output: None,
                assigned_resources: Vec::new(),
                cancel_requested: false,
            })
            .await,
        Err(StoreError::Conflict)
    ));
    assert!(matches!(
        store
            .transition(TransitionCommand {
                id: second_record.id,
                expected_version: 0,
                expected_attempt: 0,
                state: TaskState::Succeeded,
                output: None,
                assigned_resources: Vec::new(),
                cancel_requested: false,
            })
            .await,
        Err(StoreError::InvalidTransition)
    ));

    let owner = store.acquire_owner().await.unwrap();
    let unfinished = store.scan_unfinished(None).await.unwrap();
    assert_eq!(unfinished.tasks.len(), 2);
    let cursor = unfinished.tasks.iter().map(|task| task.record.id).min().unwrap();
    assert_eq!(store.scan_unfinished(Some(cursor)).await.unwrap().tasks.len(), 1);
    store.release_owner(owner).await.unwrap();
    assert!(store.acquire_owner().await.is_err());
    drop(store);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("owner.lock"));
    let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
    let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_sqlite_store_maps_corrupt_records_and_terminal_states() {
    use qubit_task::store::SqliteTaskStore;

    let path = std::env::temp_dir().join(format!("qubit-task-state-kinds-{}.sqlite", TaskId::generate()));
    let store = SqliteTaskStore::open(&path).expect("SQLite store opens");
    let states = [
        TaskState::Failed {
            category: "business".into(),
            message: "failed".into(),
        },
        TaskState::Panicked {
            message: "panic".into(),
        },
        TaskState::Cancelled,
    ];
    for state in states {
        let accepted = store
            .accept(TaskId::generate(), TaskRequest::new("echo", "1", Vec::new()))
            .await
            .unwrap();
        let record = match accepted {
            AcceptOutcome::Accepted(record) => record,
            _ => panic!("task is new"),
        };
        let updated = if state == TaskState::Cancelled {
            store
                .transition(TransitionCommand {
                    id: record.id,
                    expected_version: 0,
                    expected_attempt: 0,
                    state: state.clone(),
                    output: None,
                    assigned_resources: Vec::new(),
                    cancel_requested: false,
                })
                .await
                .unwrap()
        } else {
            let running = store
                .transition(TransitionCommand {
                    id: record.id,
                    expected_version: 0,
                    expected_attempt: 0,
                    state: TaskState::Running,
                    output: None,
                    assigned_resources: Vec::new(),
                    cancel_requested: false,
                })
                .await
                .unwrap();
            store
                .transition(TransitionCommand {
                    id: record.id,
                    expected_version: running.state_version,
                    expected_attempt: running.attempt,
                    state: state.clone(),
                    output: None,
                    assigned_resources: Vec::new(),
                    cancel_requested: false,
                })
                .await
                .unwrap()
        };
        assert_eq!(updated.state, state);
        assert!(updated.finished_at_ms.is_some());
    }
    let bad_path = path.with_extension("corrupt.sqlite");
    let bad_store = SqliteTaskStore::open(&bad_path).expect("second SQLite store opens");
    let accepted = bad_store
        .accept(TaskId::generate(), TaskRequest::new("bad", "1", Vec::new()))
        .await
        .unwrap();
    let bad_record = match accepted {
        AcceptOutcome::Accepted(record) => record,
        _ => panic!("task is new"),
    };
    drop(bad_store);
    rusqlite::Connection::open(&bad_path)
        .unwrap()
        .execute(
            "UPDATE tasks SET record_json='not-json' WHERE id=?1",
            [bad_record.id.to_string()],
        )
        .unwrap();
    let reopened = SqliteTaskStore::open(&bad_path).unwrap();
    assert!(matches!(reopened.get(bad_record.id).await, Err(StoreError::Failure(_))));
    drop(reopened);
    drop(store);
    for file in [&path, &bad_path] {
        let _ = std::fs::remove_file(file);
        let _ = std::fs::remove_file(file.with_extension("owner.lock"));
        let _ = std::fs::remove_file(file.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(file.with_extension("sqlite-shm"));
    }
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_recovery_blocks_tasks_without_a_registered_handler() {
    let path = std::env::temp_dir().join(format!("qubit-task-missing-handler-{}.sqlite", TaskId::generate()));
    let store = qubit_task::store::SqliteTaskStore::open(&path).expect("SQLite store opens");
    let accepted = store
        .accept(TaskId::generate(), TaskRequest::new("missing", "1", Vec::new()))
        .await
        .expect("task is accepted");
    let record = match accepted {
        AcceptOutcome::Accepted(record) => record,
        _ => panic!("first request is new"),
    };
    drop(store);

    let service = qubit_task::TaskExecutionServiceBuilder::recoverable_sqlite(&path)
        .expect("recoverable service config")
        .build()
        .await
        .expect("recovery succeeds with a blocked record");
    let recovered = service
        .get(record.id)
        .await
        .expect("query succeeds")
        .expect("record remains available");
    assert!(matches!(recovered.state, TaskState::Blocked { .. }));
    service.shutdown().await.expect("service shuts down");
    drop(service);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("owner.lock"));
    let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
    let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
}

#[cfg(feature = "sqlite")]
#[test]
fn test_sqlite_store_enforces_one_process_owner() {
    let path = std::env::temp_dir().join(format!("qubit-task-owner-{}.sqlite", TaskId::generate()));
    let first = qubit_task::store::SqliteTaskStore::open(&path).expect("first store gets ownership");
    assert!(qubit_task::store::SqliteTaskStore::open(&path).is_err());
    drop(first);
    let second = qubit_task::store::SqliteTaskStore::open(&path).expect("ownership releases after drop");
    drop(second);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("owner.lock"));
    let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
    let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
}

#[cfg(feature = "inventory")]
struct TestStoreProvider;

#[cfg(feature = "inventory")]
impl qubit_spi::ProviderMetadata for TestStoreProvider {
    fn descriptor(&self) -> qubit_spi::ProviderDescriptor {
        qubit_spi::provider_descriptor!("test.task.store.memory")
    }
}

#[cfg(feature = "inventory")]
impl qubit_spi::ServiceProvider<qubit_task::spi::TaskStoreSpec> for TestStoreProvider {
    fn create_configured(
        &self,
        config: &qubit_task::spi::TaskStoreConfig,
    ) -> Result<Arc<dyn TaskStore>, qubit_spi::error::ProviderFailure<StoreError>> {
        match config {
            qubit_task::spi::TaskStoreConfig::Memory { history_capacity } => {
                Ok(Arc::new(MemoryTaskStore::new(*history_capacity)))
            }
            #[cfg(feature = "sqlite")]
            qubit_task::spi::TaskStoreConfig::Sqlite { .. } => Err(qubit_spi::error::ProviderFailure::unsupported(
                StoreError::Failure("test provider only accepts memory configuration".into()),
            )),
            qubit_task::spi::TaskStoreConfig::Custom(_) => Err(qubit_spi::error::ProviderFailure::unsupported(
                StoreError::Failure("test provider only accepts memory configuration".into()),
            )),
        }
    }
}

#[cfg(feature = "inventory")]
qubit_spi::submit_sync_provider! {
    inventory_entry = qubit_task::spi::task_store_providers::Entry;
    spec = qubit_task::spi::TaskStoreSpec;
    provider = TestStoreProvider;
}

#[cfg(feature = "inventory")]
#[test]
fn test_spi_inventory_discovers_builtin_and_linked_store_providers() {
    let registry = qubit_task::spi::discovered_task_store_registry().expect("inventory builds");
    let selection = qubit_spi::ProviderSelection::named("test.task.store.memory").expect("provider selection is valid");
    let store = registry
        .resolve_selected(&selection)
        .expect("linked provider resolves")
        .create_configured(&qubit_task::spi::TaskStoreConfig::Memory { history_capacity: 17 })
        .expect("provider creates a store");
    assert!(!store.capabilities().restart_recovery);
    let builtin = qubit_spi::ProviderSelection::named(qubit_task::spi::MEMORY_STORE_PROVIDER_ID)
        .expect("built-in provider ID is valid");
    assert!(registry.resolve_selected(&builtin).is_ok());
}

#[cfg(feature = "inventory")]
#[test]
fn test_spi_builtin_registries_construct_all_component_families() {
    use qubit_spi::ProviderSelection;
    use qubit_task::spi;

    let memory_config = spi::TaskStoreConfig::Memory { history_capacity: 5 };
    let memory_registry = spi::memory_store_registry();
    let memory = memory_registry
        .resolve_selected(&ProviderSelection::named(spi::MEMORY_STORE_PROVIDER_ID).unwrap())
        .unwrap()
        .create_configured(&memory_config)
        .unwrap();
    assert!(!memory.capabilities().restart_recovery);
    assert!(
        memory_registry
            .resolve_selected(&ProviderSelection::named(spi::MEMORY_STORE_PROVIDER_ID).unwrap())
            .unwrap()
            .create_configured(&spi::TaskStoreConfig::custom(17_u32))
            .is_err()
    );

    let policy = spi::scheduling_policy_registry()
        .resolve_selected(&ProviderSelection::named(spi::FAIR_FIFO_PROVIDER_ID).unwrap())
        .unwrap()
        .create_configured(&())
        .unwrap();
    assert_eq!(
        policy.order(&QueueSnapshot::default(), &ResourceSnapshot::default()),
        Vec::<TaskId>::new()
    );

    let capacity = ResourceCapacity {
        cpu_slots: 3,
        ..ResourceCapacity::default()
    };
    let engine = spi::task_execution_engine_registry()
        .resolve_selected(&ProviderSelection::named(spi::LOCAL_ENGINE_PROVIDER_ID).unwrap())
        .unwrap()
        .create_configured(&capacity)
        .unwrap();
    assert_eq!(engine.capacity().capacity.cpu_slots, 3);
    assert!(
        spi::task_handler_registry()
            .resolve_selected(&ProviderSelection::named("missing.handler.provider").unwrap())
            .is_err()
    );

    let custom = spi::TaskStoreConfig::custom(23_u32);
    assert_eq!(custom.downcast_ref::<u32>(), Some(&23));
    assert_eq!(custom.downcast_ref::<String>(), None);
}

#[cfg(all(feature = "inventory", feature = "sqlite"))]
#[test]
fn test_spi_sqlite_provider_requires_and_accepts_sqlite_configuration() {
    use qubit_spi::ProviderSelection;
    use qubit_task::spi;

    let registry = spi::discovered_task_store_registry().expect("store providers are discovered");
    let provider = registry
        .resolve_selected(&ProviderSelection::named(spi::SQLITE_STORE_PROVIDER_ID).unwrap())
        .unwrap();
    assert!(
        provider
            .create_configured(&spi::TaskStoreConfig::Memory { history_capacity: 1 })
            .is_err()
    );
    let path = std::env::temp_dir().join(format!("qubit-task-spi-{}.sqlite", TaskId::generate()));
    let store = provider
        .create_configured(&spi::TaskStoreConfig::Sqlite { path: path.clone() })
        .unwrap();
    assert!(store.capabilities().restart_recovery);
    drop(store);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("owner.lock"));
    let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
    let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
}

#[cfg(feature = "event-bus")]
#[tokio::test]
async fn test_event_bus_receives_status_changes_without_becoming_authoritative() {
    use qubit_event_bus::DeliveryError;
    use qubit_event_bus::EventBus;
    use qubit_event_bus::local::LocalEventBusConfig;
    use qubit_event_bus::model::SubscribeRequest;
    use qubit_event_bus::model::SubscriberId;
    use qubit_event_bus::model::Topic;

    let bus = EventBus::local(LocalEventBusConfig::default()).expect("local event bus starts");
    let topic = Topic::<qubit_task::event::TaskEvent>::new("task.lifecycle").expect("topic is valid");
    let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let count_ref = count.clone();
    let versions = Arc::new(std::sync::Mutex::new(Vec::new()));
    let versions_ref = versions.clone();
    let subscription = bus
        .subscribe(
            SubscribeRequest::new(
                SubscriberId::new("task-test").expect("subscriber ID is valid"),
                topic.clone(),
            ),
            move |delivery| {
                versions_ref
                    .lock()
                    .expect("versions lock")
                    .push(delivery.payload().state_version);
                count_ref.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                Ok::<(), DeliveryError>(())
            },
        )
        .expect("topic subscription succeeds");
    let service = qubit_task::TaskExecutionServiceBuilder::in_memory()
        .event_bus(bus.clone())
        .event_bus_buffer_capacity(std::num::NonZeroUsize::new(256).expect("nonzero capacity"))
        .build()
        .await
        .expect("service builds with bus");
    let id = service
        .submit_local(|_| Ok(TaskOutput::default()))
        .await
        .expect("task accepted");
    assert_eq!(
        service.wait(id).await.expect("task completes").state,
        TaskState::Succeeded
    );
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while service.notification_stats().expect("notification counters").enqueued < 3 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("terminal notification enqueued");
    let enqueued_before_shutdown = service.notification_stats().expect("notification counters").enqueued;
    assert!(
        enqueued_before_shutdown >= 3,
        "queued, running, and terminal events enqueued"
    );
    service
        .shutdown()
        .await
        .expect("service shuts down after publication drain");
    let after_shutdown = service.notification_stats().expect("notification counters");
    assert_eq!(after_shutdown.enqueued, enqueued_before_shutdown);
    assert_eq!(after_shutdown.accepted, enqueued_before_shutdown);
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while count.load(std::sync::atomic::Ordering::Acquire) < 3 {
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("service publishes queued, running, and terminal events");
    bus.wait_for_idle(&topic, Some(std::time::Duration::from_secs(2)))
        .expect("status events drain");
    assert!(
        count.load(std::sync::atomic::Ordering::Acquire) >= 3,
        "received {} task events",
        count.load(std::sync::atomic::Ordering::Acquire)
    );
    let versions = versions.lock().expect("versions lock");
    assert_eq!(versions.as_slice(), &[0, 1, 2]);
    subscription.cancel().expect("subscription is cancelled");
    bus.shutdown(qubit_event_bus::spi::ShutdownMode::Immediate)
        .expect("event bus shuts down");
}

#[cfg(feature = "event-bus")]
#[tokio::test]
async fn test_event_bus_publish_failure_does_not_change_task_result() {
    use qubit_event_bus::EventBus;
    use qubit_event_bus::local::LocalEventBusConfig;

    let bus = EventBus::local(LocalEventBusConfig::default()).expect("local event bus starts");
    bus.shutdown(qubit_event_bus::spi::ShutdownMode::Immediate)
        .expect("event bus shuts down before notification");
    let service = qubit_task::TaskExecutionServiceBuilder::in_memory()
        .event_bus(bus)
        .build()
        .await
        .expect("service builds with a stopped event bus");
    let id = service
        .submit_local(|_| Ok(TaskOutput::default()))
        .await
        .expect("task is accepted");
    let finished = service
        .wait(id)
        .await
        .expect("task succeeds despite notification failure");
    assert_eq!(finished.state, TaskState::Succeeded);
    service.shutdown().await.expect("service shuts down");
    let stats = service.notification_stats().expect("notification counters");
    assert!(stats.publish_error >= 3);
}

#[cfg(feature = "event-bus")]
#[tokio::test]
async fn test_event_bus_stats_are_absent_without_a_bus() {
    let service = qubit_task::TaskExecutionService::in_memory()
        .await
        .expect("service builds");
    assert!(service.notification_stats().is_none());
    service.shutdown().await.expect("service shuts down");
}
