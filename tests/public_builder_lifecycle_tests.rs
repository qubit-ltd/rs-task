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
use qubit_task::TaskExecutionService;
use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::engine::LocalTaskExecutionEngine;
use qubit_task::handler::TaskContext;
use qubit_task::handler::TaskHandler;
use qubit_task::handler::TaskHandlerDescriptor;
use qubit_task::handler::TaskHandlerRegistry;
use qubit_task::handler::TaskRunOutcome;
use qubit_task::model::ResourceCapacity;
use qubit_task::model::TaskId;
use qubit_task::model::TaskOutput;
use qubit_task::model::TaskRunError;
use qubit_task::model::TaskState;
use qubit_task::scheduling::FairFifoPolicy;
use qubit_task::store::MemoryTaskStore;
use tokio::test as tokio_test;

struct Echo;

impl TaskHandler for Echo {
    fn descriptor(&self) -> TaskHandlerDescriptor {
        TaskHandlerDescriptor {
            task_type: "builder-test".into(),
            version: "1".into(),
        }
    }

    fn run<'a>(
        &'a self,
        _payload: &'a [u8],
        _context: TaskContext,
    ) -> qubit_task::store::TaskFuture<'a, qubit_task::handler::TaskRunResult> {
        Box::pin(async { Ok(TaskRunOutcome::Succeeded(TaskOutput::default())) })
    }
}

struct RetryOnce(AtomicUsize);

impl TaskHandler for RetryOnce {
    fn descriptor(&self) -> qubit_task::handler::TaskHandlerDescriptor {
        qubit_task::handler::TaskHandlerDescriptor {
            task_type: "retry-once".into(),
            version: "1".into(),
        }
    }

    fn run<'a>(
        &'a self,
        _payload: &'a [u8],
        _context: TaskContext,
    ) -> qubit_task::store::TaskFuture<'a, qubit_task::handler::TaskRunResult> {
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
    registry.register(Arc::new(Echo)).unwrap();
    registry.register(Arc::new(RetryOnce(AtomicUsize::new(0)))).unwrap();
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
    .unwrap()
    .handlers(registry)
    .capacity(capacity)
    .queue_capacity(8)
    .max_running_tasks(NonZeroUsize::new(2).unwrap())
    .scan_budget(8)
    .max_attempts(2)
    .require_recovery(false);
    #[cfg(feature = "event-bus")]
    let builder = builder
        .event_bus(EventBus::local(LocalEventBusConfig::default()).unwrap())
        .event_bus_buffer_capacity(NonZeroUsize::new(4).unwrap());
    let service = builder.build().await.unwrap();
    #[cfg(feature = "event-bus")]
    assert!(service.notification_stats().is_some());
    assert!(!service.capabilities().store.restart_recovery);
    assert_eq!(service.last_store_error(), None);
    assert!(service.get(TaskId::generate()).await.unwrap().is_none());

    let accepted = service
        .submit(
            qubit_task::model::TaskRequest::new("builder-test", "1", Vec::new())
                .with_idempotency_key("builder-test-submit"),
        )
        .await
        .unwrap();
    assert!(matches!(
        service.wait(accepted.id).await.unwrap().state,
        TaskState::Succeeded
    ));
    assert!(service.get(accepted.id).await.unwrap().is_some());
    assert_eq!(
        service
            .list(qubit_task::model::TaskQuery::default())
            .await
            .unwrap()
            .records
            .len(),
        1
    );
    assert_eq!(service.stats().await.unwrap().terminal, 1);
    assert!(matches!(
        service.cancel(accepted.id).await.unwrap(),
        qubit_task::service::CancelOutcome::AlreadyTerminal
    ));

    let local = service
        .submit_local(|_| qubit_task::service::LocalTaskOutcome::<u8, String>::Succeeded {
            value: 7,
            summary: TaskOutput::default(),
        })
        .await
        .unwrap();
    assert_eq!(local.task_id(), service.get(local.task_id()).await.unwrap().unwrap().id);
    assert!(format!("{local:?}").contains("LocalTaskHandle"));
    assert_eq!(local.result().await.unwrap().unwrap(), 7);

    let retrying = service
        .submit(
            qubit_task::model::TaskRequest::new("retry-once", "1", Vec::new())
                .with_idempotency_key("builder-retry-once"),
        )
        .await
        .unwrap();
    let retried = service.wait(retrying.id).await.unwrap();
    assert_eq!(retried.attempt, 2);
    assert!(matches!(retried.state, TaskState::Succeeded));

    let blocked = service
        .submit(
            qubit_task::model::TaskRequest::new("missing", "1", Vec::new())
                .with_idempotency_key("builder-missing-handler"),
        )
        .await
        .unwrap();
    assert!(matches!(
        service.wait(blocked.id).await,
        Err(qubit_task::service::TaskServiceError::Blocked)
    ));
    service.retry_blocked(blocked.id).await.unwrap();
    assert!(matches!(
        service.wait(blocked.id).await,
        Err(qubit_task::service::TaskServiceError::Blocked)
    ));
    assert!(matches!(
        service.cancel(blocked.id).await.unwrap(),
        qubit_task::service::CancelOutcome::CancelledBeforeStart
    ));
    assert!(matches!(
        service.cancel(TaskId::generate()).await,
        Err(qubit_task::service::TaskServiceError::Store(
            qubit_task::store::StoreError::NotFound
        ))
    ));
    service.shutdown().await.unwrap();

    let memory_service = TaskExecutionService::in_memory().await.unwrap();
    memory_service.shutdown().await.unwrap();
}
