// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync;
use tokio::test as tokio_test;
use tokio::time;

use crate::handler::TaskContext;
use crate::handler::TaskHandler;
use crate::handler::TaskHandlerDescriptor;
use crate::handler::TaskRunOutcome;
use crate::handler::TaskRunResult;
use crate::model::AcceptOutcome;
use crate::model::OwnerEpoch;
use crate::model::RecoveryPage;
use crate::model::StoreCapabilities;
use crate::model::TaskCursor;
use crate::model::TaskId;
use crate::model::TaskOutput;
use crate::model::TaskPage;
use crate::model::TaskQuery;
use crate::model::TaskRecord;
use crate::model::TaskRequest;
use crate::model::TaskState;
use crate::model::TaskStateCounts;
use crate::model::TaskSummary;
use crate::model::TransitionCommand;
use crate::service::TaskServiceError;
use crate::service::task_execution_service_builder::TaskExecutionServiceBuilder;
use crate::store::LegacyTaskStore as TaskStore;
use crate::store::MemoryTaskStore;
use crate::store::StoreError;
use crate::store::TaskFuture;

struct ShutdownStore {
    inner: MemoryTaskStore,
    panic_counts: AtomicBool,
    panic_release: bool,
    count_barrier: sync::Barrier,
    events: Mutex<Vec<&'static str>>,
    entered: sync::Notify,
    write_gate: sync::Semaphore,
}
impl ShutdownStore {
    /// Uses a fresh namespace with observable owner release and write barriers.
    fn new(panic_release: bool) -> Self {
        Self {
            inner: MemoryTaskStore::new(16),
            panic_counts: AtomicBool::new(false),
            panic_release,
            count_barrier: sync::Barrier::new(2),
            events: Mutex::new(vec![]),
            entered: sync::Notify::new(),
            write_gate: sync::Semaphore::new(0),
        }
    }
}
impl TaskStore for ShutdownStore {
    fn capabilities(&self) -> StoreCapabilities {
        StoreCapabilities {
            persistent_history: true,
            restart_recovery: true,
        }
    }

    fn accept<'a>(&'a self, id: TaskId, request: TaskRequest) -> TaskFuture<'a, Result<AcceptOutcome, StoreError>> {
        self.inner.accept(id, request)
    }

    fn get_by_idempotency_key<'a>(&'a self, key: &'a str) -> TaskFuture<'a, Result<Option<TaskRecord>, StoreError>> {
        self.inner.get_by_idempotency_key(key)
    }

    fn get_summary_by_idempotency_key<'a>(
        &'a self,
        key: &'a str,
    ) -> TaskFuture<'a, Result<Option<TaskSummary>, StoreError>> {
        self.inner.get_summary_by_idempotency_key(key)
    }

    fn transition<'a>(&'a self, command: TransitionCommand) -> TaskFuture<'a, Result<TaskSummary, StoreError>> {
        Box::pin(async move {
            if command.expected_attempt == 1 && matches!(command.state, TaskState::Succeeded) {
                self.events.lock().push("write entered");
                self.entered.notify_one();
                self.write_gate.acquire().await.expect("write gate").forget();
                let result = self.inner.transition(command).await;
                self.events.lock().push("write finished");
                result
            } else {
                self.inner.transition(command).await
            }
        })
    }

    fn get_summary<'a>(&'a self, id: TaskId) -> TaskFuture<'a, Result<Option<TaskSummary>, StoreError>> {
        self.inner.get_summary(id)
    }

    fn get<'a>(&'a self, id: TaskId) -> TaskFuture<'a, Result<Option<TaskRecord>, StoreError>> {
        self.inner.get(id)
    }

    fn list<'a>(&'a self, query: TaskQuery) -> TaskFuture<'a, Result<TaskPage, StoreError>> {
        self.inner.list(query)
    }

    fn count_states<'a>(&'a self) -> TaskFuture<'a, Result<TaskStateCounts, StoreError>> {
        Box::pin(async move {
            if self.panic_counts.load(Ordering::Acquire) {
                // Ensure both scheduler and coordinator entered the failing count
                // before either latches a fault that could short-circuit the other.
                self.count_barrier.wait().await;
                panic!("injected shutdown count panic");
            }
            self.inner.count_states().await
        })
    }

    fn prune_terminal_before<'a>(
        &'a self,
        _accepted_before_ms: u64,
        _max_rows: std::num::NonZeroUsize,
    ) -> TaskFuture<'a, Result<usize, StoreError>> {
        Box::pin(async { Err(StoreError::UnsupportedCapability) })
    }

    fn acquire_owner<'a>(&'a self) -> TaskFuture<'a, Result<OwnerEpoch, StoreError>> {
        Box::pin(async { Ok(OwnerEpoch(1)) })
    }

    fn has_unfinished_over_limit<'a>(&'a self, limit: usize) -> TaskFuture<'a, Result<bool, StoreError>> {
        {
            let _ = limit;
            Box::pin(async { Ok(false) })
        }
    }

    fn scan_unfinished<'a>(&'a self, cursor: Option<TaskCursor>) -> TaskFuture<'a, Result<RecoveryPage, StoreError>> {
        {
            let _ = cursor;
            Box::pin(async {
                Ok(RecoveryPage {
                    tasks: vec![],
                    next: None,
                })
            })
        }
    }

    fn release_owner<'a>(&'a self, epoch: OwnerEpoch) -> TaskFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            assert_eq!(epoch, OwnerEpoch(1));
            self.events.lock().push("release");
            assert!(!self.panic_release, "injected owner release panic");
            Ok(())
        })
    }
}

struct TestHandler {
    retry: bool,
}
impl TaskHandler for TestHandler {
    fn descriptor(&self) -> TaskHandlerDescriptor {
        TaskHandlerDescriptor {
            task_type: "finalizer".into(),
            version: "1".into(),
        }
    }
    fn run<'a>(&'a self, _payload: &'a [u8], _context: TaskContext) -> TaskFuture<'a, TaskRunResult> {
        Box::pin(async move {
            if self.retry {
                Err(crate::model::TaskRunError {
                    category: "retry".into(),
                    message: "retry".into(),
                    retryable: true,
                })
            } else {
                Ok(TaskRunOutcome::Succeeded(TaskOutput::default()))
            }
        })
    }
}

#[tokio_test]
async fn test_shutdown_count_panic_reaches_shared_result() {
    let store = Arc::new(ShutdownStore::new(false));
    let service = TaskExecutionServiceBuilder::in_memory()
        .runtime_handle(tokio::runtime::Handle::current())
        .store(store.clone())
        .build()
        .await
        .expect("service");
    store.panic_counts.store(true, Ordering::Release);
    let (first, second) = tokio::join!(
        service.shutdown_until(time::Instant::now() + Duration::from_secs(1)),
        service.shutdown_until(time::Instant::now() + Duration::from_secs(1))
    );
    assert!(
        matches!(&first, Err(TaskServiceError::StoreUnavailable(message)) if message.contains("count panic")),
        "{first:?}"
    );
    assert_eq!(
        first.expect_err("fault").to_string(),
        second.expect_err("same fault").to_string()
    );
    assert_eq!(*store.events.lock(), vec!["release"]);
}

#[tokio_test]
async fn test_shutdown_owner_release_panic_is_shared_and_not_retried() {
    let store = Arc::new(ShutdownStore::new(true));
    let service = TaskExecutionServiceBuilder::in_memory()
        .runtime_handle(tokio::runtime::Handle::current())
        .store(store.clone())
        .build()
        .await
        .expect("service");
    let (first, second) = tokio::join!(
        service.shutdown_until(time::Instant::now() + Duration::from_secs(1)),
        service.shutdown_until(time::Instant::now() + Duration::from_secs(1))
    );
    assert!(
        matches!(&first, Err(TaskServiceError::StoreUnavailable(message)) if message.contains("owner release panic")),
        "{first:?}"
    );
    assert_eq!(
        first.expect_err("fault").to_string(),
        second.expect_err("same fault").to_string()
    );
    assert_eq!(*store.events.lock(), vec!["release"]);
}

#[tokio_test]
async fn test_shutdown_fault_retains_owner_until_finalizer_write_finishes() {
    let store = Arc::new(ShutdownStore::new(true));
    let service = TaskExecutionServiceBuilder::in_memory()
        .runtime_handle(tokio::runtime::Handle::current())
        .store(store.clone())
        .register_handler(Arc::new(TestHandler { retry: false }))
        .expect("handler")
        .build()
        .await
        .expect("service");
    service
        .submit(TaskRequest::new("finalizer", "1", vec![]).with_idempotency_key("finalizer"))
        .await
        .expect("accepted");
    time::timeout(Duration::from_secs(1), store.entered.notified())
        .await
        .expect("finalizer writing");
    store.panic_counts.store(true, Ordering::Release);
    assert!(matches!(
        service
            .shutdown_until(time::Instant::now() + Duration::from_millis(30))
            .await,
        Err(TaskServiceError::ShutdownTimedOut)
    ));
    assert_eq!(*store.events.lock(), vec!["write entered"]);
    store.write_gate.add_permits(1);
    let result = service
        .shutdown_until(time::Instant::now() + Duration::from_secs(1))
        .await;
    assert!(
        matches!(result, Err(TaskServiceError::StoreUnavailable(ref message))
        if message.contains("count panic") && message.contains("owner release panic")),
        "{result:?}"
    );
    assert_eq!(*store.events.lock(), vec!["write entered", "write finished", "release"]);
}

#[tokio_test]
async fn test_shutdown_permanently_blocked_store_retains_owner() {
    let store = Arc::new(ShutdownStore::new(false));
    let service = TaskExecutionServiceBuilder::in_memory()
        .runtime_handle(tokio::runtime::Handle::current())
        .store(store.clone())
        .register_handler(Arc::new(TestHandler { retry: false }))
        .expect("handler")
        .build()
        .await
        .expect("service");
    service
        .submit(TaskRequest::new("finalizer", "1", vec![]).with_idempotency_key("blocked"))
        .await
        .expect("accepted");
    time::timeout(Duration::from_secs(1), store.entered.notified())
        .await
        .expect("write entered");
    store.panic_counts.store(true, Ordering::Release);
    assert!(matches!(
        service
            .shutdown_until(time::Instant::now() + Duration::from_millis(30))
            .await,
        Err(TaskServiceError::ShutdownTimedOut)
    ));
    assert_eq!(*store.events.lock(), vec!["write entered"]);
    assert!(service.last_store_error().is_some());
    // The gate intentionally remains closed. Runtime teardown cancels this
    // isolated test's futures; no ownership release is claimed for that case.
}

#[cfg(feature = "event-bus")]
mod notification_panic {
    use std::sync::Arc;
    use std::time::Duration;

    use qubit_event_bus::EventBus;
    use qubit_event_bus::error::SpiError;
    use qubit_event_bus::model::ProviderId;
    use qubit_event_bus::model::PublishAcknowledgement;
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
    use tokio::time;

    use crate::model::TaskRequest;
    use crate::service::task_execution_service_builder::TaskExecutionServiceBuilder;

    struct PanickingPublisher;
    impl EventBusSpi for PanickingPublisher {
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

        fn publish(&self, _message: OutboundMessage) -> Result<PublishAcknowledgement, SpiError> {
            panic!("injected notification publisher panic");
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

    #[tokio::test]
    async fn test_shutdown_provider_publish_panic_preserves_shared_result() {
        let bus = EventBus::from_spi(
            ProviderId::new("panic-publisher").expect("provider"),
            Arc::new(PanickingPublisher),
        )
        .expect("event bus");
        let service = TaskExecutionServiceBuilder::in_memory()
            .runtime_handle(tokio::runtime::Handle::current())
            .event_bus(bus)
            .build()
            .await
            .expect("service");
        service
            .submit(TaskRequest::new("unhandled", "1", vec![]).with_idempotency_key("notification"))
            .await
            .expect("accepted");
        let (first, second) = tokio::join!(
            service.shutdown_until(time::Instant::now() + Duration::from_secs(2)),
            service.shutdown_until(time::Instant::now() + Duration::from_secs(2))
        );
        first.expect("provider publication panic is isolated by the event bus");
        second.expect("same successful close result");
        assert!(service.last_store_error().is_none());
    }
}
// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::num::NonZeroUsize;
use std::sync::atomic::AtomicUsize;
use std::sync::mpsc;

use tokio::spawn;
use tokio::sync::Semaphore;
use tokio::task::yield_now;
use tokio::time::Instant;
use tokio::time::timeout;

use crate::service::LocalTaskOutcome;

struct DelayedCountsStore {
    inner: Arc<dyn TaskStore>,
    armed: AtomicBool,
    snapshots: AtomicUsize,
    observed: Semaphore,
    resume: Semaphore,
}
impl TaskStore for DelayedCountsStore {
    fn capabilities(&self) -> StoreCapabilities {
        self.inner.capabilities()
    }

    fn accept<'a>(&'a self, id: TaskId, request: TaskRequest) -> TaskFuture<'a, Result<AcceptOutcome, StoreError>> {
        self.inner.accept(id, request)
    }

    fn get_by_idempotency_key<'a>(&'a self, key: &'a str) -> TaskFuture<'a, Result<Option<TaskRecord>, StoreError>> {
        self.inner.get_by_idempotency_key(key)
    }

    fn get_summary_by_idempotency_key<'a>(
        &'a self,
        key: &'a str,
    ) -> TaskFuture<'a, Result<Option<TaskSummary>, StoreError>> {
        self.inner.get_summary_by_idempotency_key(key)
    }

    fn transition<'a>(&'a self, command: TransitionCommand) -> TaskFuture<'a, Result<TaskSummary, StoreError>> {
        self.inner.transition(command)
    }

    fn get_summary<'a>(&'a self, id: TaskId) -> TaskFuture<'a, Result<Option<TaskSummary>, StoreError>> {
        self.inner.get_summary(id)
    }

    fn get<'a>(&'a self, id: TaskId) -> TaskFuture<'a, Result<Option<TaskRecord>, StoreError>> {
        self.inner.get(id)
    }

    fn list<'a>(&'a self, _query: TaskQuery) -> TaskFuture<'a, Result<TaskPage, StoreError>> {
        self.inner.list(_query)
    }

    fn count_states<'a>(&'a self) -> TaskFuture<'a, Result<TaskStateCounts, StoreError>> {
        Box::pin(async move {
            let snapshot = self.inner.count_states().await?;
            if snapshot.running > 0 && self.armed.load(Ordering::SeqCst) {
                let ticket = self.snapshots.fetch_add(1, Ordering::SeqCst);
                println!("shutdown count snapshot {ticket}: {snapshot:?}");
                self.observed.add_permits(1);
                self.resume
                    .acquire()
                    .await
                    .expect("shutdown resumes delayed count snapshot")
                    .forget();
            }
            Ok(snapshot)
        })
    }

    fn prune_terminal_before<'a>(
        &'a self,
        _accepted_before_ms: u64,
        _max_rows: NonZeroUsize,
    ) -> TaskFuture<'a, Result<usize, StoreError>> {
        self.inner.prune_terminal_before(_accepted_before_ms, _max_rows)
    }

    fn acquire_owner<'a>(&'a self) -> TaskFuture<'a, Result<OwnerEpoch, StoreError>> {
        self.inner.acquire_owner()
    }

    fn has_unfinished_over_limit<'a>(&'a self, limit: usize) -> TaskFuture<'a, Result<bool, StoreError>> {
        self.inner.has_unfinished_over_limit(limit)
    }

    fn scan_unfinished<'a>(&'a self, cursor: Option<TaskCursor>) -> TaskFuture<'a, Result<RecoveryPage, StoreError>> {
        self.inner.scan_unfinished(cursor)
    }

    fn release_owner<'a>(&'a self, epoch: OwnerEpoch) -> TaskFuture<'a, Result<(), StoreError>> {
        self.inner.release_owner(epoch)
    }
}

#[tokio_test(flavor = "multi_thread", worker_threads = 2)]
async fn test_shutdown_drains_when_terminal_notification_precedes_count_return() {
    let (send, recv) = mpsc::channel();
    let store = Arc::new(DelayedCountsStore {
        inner: Arc::new(MemoryTaskStore::new(8)),
        armed: AtomicBool::new(false),
        snapshots: AtomicUsize::new(0),
        observed: Semaphore::new(0),
        resume: Semaphore::new(0),
    });
    let service = TaskExecutionServiceBuilder::in_memory()
        .store(store.clone())
        .build()
        .await
        .expect("service builds");
    let handle = service
        .submit_local(move |_| {
            recv.recv().expect("test releases local handler");
            LocalTaskOutcome::<u32, String>::Succeeded {
                value: 42,
                summary: TaskOutput::default(),
            }
        })
        .await
        .expect("local task is accepted");
    let id = handle.task_id();
    timeout(Duration::from_secs(5), async {
        loop {
            if matches!(
                service
                    .get_summary(id)
                    .await
                    .expect("running summary lookup succeeds")
                    .expect("task summary is retained")
                    .state,
                TaskState::Running
            ) {
                break;
            }
            yield_now().await;
        }
    })
    .await
    .expect("handler did not reach Running");
    store.armed.store(true, Ordering::SeqCst);
    let closing = service.clone();
    let shutdown = spawn(async move { closing.shutdown_until(Instant::now() + Duration::from_secs(5)).await });
    timeout(Duration::from_secs(5), async {
        store
            .observed
            .acquire_many(2)
            .await
            .expect("both count snapshots arrive")
            .forget();
    })
    .await
    .expect("scheduler and shutdown did not both acquire running snapshots");
    send.send(()).expect("local handler is waiting");
    let value = timeout(Duration::from_secs(5), handle.result())
        .await
        .expect("task did not finalize")
        .expect("local handle returns its result")
        .expect("local handler succeeds");
    assert_eq!(value, 42);
    // finish_attempt notifies changed before finalizing this public local handle.
    // Returning stale-but-consistent snapshots now deterministically exercises
    // the notification registration window, without timing sleeps.
    store.resume.add_permits(2);
    shutdown
        .await
        .expect("shutdown task completes")
        .expect("shutdown observes the final notification");
    let counts = store
        .inner
        .count_states()
        .await
        .expect("final store counts are available");
    assert_eq!(counts.running, 0);
    assert_eq!(counts.queued, 0);
    service
        .shutdown_until(Instant::now() + Duration::from_secs(5))
        .await
        .expect("service shuts down");
}
