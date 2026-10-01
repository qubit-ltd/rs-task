// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

#[cfg(feature = "event-bus")]
use qubit_event_bus::EventBus;
#[cfg(feature = "event-bus")]
use qubit_event_bus::local::LocalEventBusConfig;
use tokio::test as tokio_test;

use super::super::task_execution_service::TaskExecutionService;
use super::super::task_execution_service_builder::TaskExecutionServiceBuilder;
use crate::engine::LocalTaskExecutionEngine;
use crate::handler::TaskContext;
use crate::handler::TaskHandler;
use crate::handler::TaskHandlerDescriptor;
use crate::handler::TaskHandlerRegistry;
use crate::handler::TaskRunOutcome;
use crate::handler::TaskRunResult;
use crate::model::ResourceCapacity;
use crate::model::TaskId;
use crate::model::TaskOutput;
use crate::model::TaskQuery;
use crate::model::TaskRequest;
use crate::model::TaskRunError;
use crate::model::TaskState;
use crate::scheduling::FairFifoPolicy;
use crate::service::CancelOutcome;
use crate::service::LocalTaskOutcome;
use crate::service::TaskServiceError;
use crate::store::MemoryTaskStore;
use crate::store::StoreError;
use crate::store::TaskFuture;

struct Echo;

impl TaskHandler for Echo {
    fn descriptor(&self) -> TaskHandlerDescriptor {
        TaskHandlerDescriptor {
            task_type: "builder-test".into(),
            version: "1".into(),
        }
    }

    fn run<'a>(&'a self, _payload: &'a [u8], _context: TaskContext) -> TaskFuture<'a, TaskRunResult> {
        Box::pin(async { Ok(TaskRunOutcome::Succeeded(TaskOutput::default())) })
    }
}

struct RetryOnce(AtomicUsize);

impl TaskHandler for RetryOnce {
    fn descriptor(&self) -> TaskHandlerDescriptor {
        TaskHandlerDescriptor {
            task_type: "retry-once".into(),
            version: "1".into(),
        }
    }

    fn run<'a>(&'a self, _payload: &'a [u8], _context: TaskContext) -> TaskFuture<'a, TaskRunResult> {
        Box::pin(async move {
            if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
                Err(TaskRunError {
                    category: "test".into(),
                    message: "retry once".into(),
                    retryable: true,
                })
            } else {
                Ok(TaskRunOutcome::Succeeded(TaskOutput::default()))
            }
        })
    }
}

#[tokio_test]
async fn test_public_builder_and_service_lifecycle_contracts() {
    let mut registry = TaskHandlerRegistry::new();
    registry.register(Arc::new(Echo)).expect("Echo handler registers");
    registry
        .register(Arc::new(RetryOnce(AtomicUsize::new(0))))
        .expect("retry handler registers");
    let capacity = ResourceCapacity {
        cpu_slots: 1,
        ..ResourceCapacity::default()
    };
    let builder = TaskExecutionServiceBuilder::from_components(
        Arc::new(MemoryTaskStore::new(16)),
        Arc::new(LocalTaskExecutionEngine::new(capacity.clone())),
        Arc::new(FairFifoPolicy::default()),
    )
    .register_handler(Arc::new(Echo))
    .expect("Echo handler registers")
    .handlers(registry)
    .capacity(capacity)
    .queue_capacity(8)
    .max_running_tasks(NonZeroUsize::new(2).expect("two is nonzero"))
    .scan_budget(8)
    .max_attempts(2)
    .require_recovery(false);
    #[cfg(feature = "event-bus")]
    let builder = builder
        .event_bus(EventBus::local(LocalEventBusConfig::default()).expect("local event bus configures"))
        .event_bus_buffer_capacity(NonZeroUsize::new(4).expect("four is nonzero"));
    let service = builder.build().await.expect("service builds");
    #[cfg(feature = "event-bus")]
    assert!(service.notification_stats().is_some());
    assert!(!service.capabilities().store.restart_recovery);
    assert_eq!(service.last_store_error(), None);
    assert!(
        service
            .get(TaskId::generate())
            .await
            .expect("unknown task lookup succeeds")
            .is_none()
    );

    let accepted = service
        .submit(TaskRequest::new("builder-test", "1", Vec::new()).with_idempotency_key("builder-test-submit"))
        .await
        .expect("builder test task is accepted");
    assert!(matches!(
        service.wait(accepted.id).await.expect("accepted task finishes").state,
        TaskState::Succeeded
    ));
    assert!(service.get(accepted.id).await.expect("task lookup succeeds").is_some());
    assert_eq!(
        service
            .list(TaskQuery::default())
            .await
            .expect("task listing succeeds")
            .records
            .len(),
        1
    );
    assert_eq!(service.stats().await.expect("task stats succeed").terminal, 1);
    assert!(matches!(
        service
            .cancel(accepted.id)
            .await
            .expect("terminal task cancellation is observed"),
        CancelOutcome::AlreadyTerminal
    ));

    let local = service
        .submit_local(|_| LocalTaskOutcome::<u8, String>::Succeeded {
            value: 7,
            summary: TaskOutput::default(),
        })
        .await
        .expect("local task is accepted");
    assert_eq!(
        local.task_id(),
        service
            .get(local.task_id())
            .await
            .expect("local task lookup succeeds")
            .expect("local task remains retained")
            .id
    );
    assert!(format!("{local:?}").contains("LocalTaskHandle"));
    assert_eq!(
        local
            .result()
            .await
            .expect("local task result resolves")
            .expect("local task succeeds"),
        7
    );

    let retrying = service
        .submit(TaskRequest::new("retry-once", "1", Vec::new()).with_idempotency_key("builder-retry-once"))
        .await
        .expect("retrying task is accepted");
    let retried = service.wait(retrying.id).await.expect("retried task finishes");
    assert_eq!(retried.attempt, 2);
    assert!(matches!(retried.state, TaskState::Succeeded));

    let blocked = service
        .submit(TaskRequest::new("missing", "1", Vec::new()).with_idempotency_key("builder-missing-handler"))
        .await
        .expect("unknown-handler task is accepted");
    assert!(matches!(service.wait(blocked.id).await, Err(TaskServiceError::Blocked)));
    service
        .retry_blocked(blocked.id)
        .await
        .expect("blocked task retry is requested");
    assert!(matches!(service.wait(blocked.id).await, Err(TaskServiceError::Blocked)));
    assert!(matches!(
        service.cancel(blocked.id).await.expect("blocked task is cancelled"),
        CancelOutcome::CancelledBeforeStart
    ));
    assert!(matches!(
        service.cancel(TaskId::generate()).await,
        Err(TaskServiceError::Store(StoreError::NotFound))
    ));
    service.shutdown().await.expect("service shuts down");

    let memory_service = TaskExecutionService::in_memory()
        .await
        .expect("default in-memory service builds");
    memory_service.shutdown().await.expect("default service shuts down");
}
