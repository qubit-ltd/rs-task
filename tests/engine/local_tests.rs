// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;

use tokio::test as tokio_test;
use tokio::time;

use crate::engine::EngineError;
use crate::engine::LocalTaskExecutionEngine;
use crate::engine::TaskExecutionEngine;
use crate::handler::TaskContext;
use crate::handler::TaskHandler;
use crate::handler::TaskHandlerDescriptor;
use crate::handler::TaskRunResult;
use crate::model::ResourceCapacity;
use crate::model::ResourceRequest;
use crate::model::TaskId;
use crate::model::TaskRequest;
use crate::model::TaskRunError;
use crate::model::TaskState;
use crate::service::TaskServiceError;
use crate::service::task_execution_service_builder::TaskExecutionServiceBuilder;
use crate::store::TaskFuture;

#[test]
fn test_memory_and_disk_quota_are_reserved_and_released_together() {
    let engine = LocalTaskExecutionEngine::new(ResourceCapacity {
        cpu_slots: 2,
        memory_bytes: Some(100),
        disk_bytes: Some(80),
        ..ResourceCapacity::default()
    });
    let first = engine
        .try_prepare(
            TaskId::generate(),
            ResourceRequest {
                memory_bytes: Some(60),
                disk_bytes: Some(40),
                ..ResourceRequest::default()
            },
        )
        .expect("quota fits");

    assert_eq!(engine.capacity().used_memory_bytes, 60);
    assert_eq!(engine.capacity().used_disk_bytes, 40);
    assert!(matches!(
        engine.try_prepare(
            TaskId::generate(),
            ResourceRequest {
                memory_bytes: Some(50),
                disk_bytes: Some(50),
                ..ResourceRequest::default()
            }
        ),
        Err(EngineError::TemporarilyUnavailable)
    ));

    drop(first);
    assert_eq!(engine.capacity().used_memory_bytes, 0);
    assert_eq!(engine.capacity().used_disk_bytes, 0);
}

#[test]
fn test_unconfigured_memory_or_disk_quota_is_unsatisfiable() {
    let engine = LocalTaskExecutionEngine::new(ResourceCapacity::default());

    assert!(matches!(
        engine.try_prepare(
            TaskId::generate(),
            ResourceRequest {
                memory_bytes: Some(1),
                ..ResourceRequest::default()
            }
        ),
        Err(EngineError::Unsatisfiable)
    ));
    assert!(matches!(
        engine.try_prepare(
            TaskId::generate(),
            ResourceRequest {
                disk_bytes: Some(1),
                ..ResourceRequest::default()
            }
        ),
        Err(EngineError::Unsatisfiable)
    ));
}

#[derive(Clone, Copy)]
enum HandlerBehavior {
    ConstructPanic,
    PollPanic,
    PanicCategory,
}

struct ProvenanceHandler(HandlerBehavior);

impl TaskHandler for ProvenanceHandler {
    fn descriptor(&self) -> TaskHandlerDescriptor {
        TaskHandlerDescriptor {
            task_type: "panic-provenance".into(),
            version: "1".into(),
        }
    }

    fn run<'a>(&'a self, _payload: &'a [u8], _context: TaskContext) -> TaskFuture<'a, TaskRunResult> {
        match self.0 {
            HandlerBehavior::ConstructPanic => panic!("panic while creating handler future"),
            HandlerBehavior::PollPanic => Box::pin(async { panic!("panic while polling handler future") }),
            HandlerBehavior::PanicCategory => Box::pin(async {
                Err(TaskRunError {
                    category: "panic".into(),
                    message: "application-selected category".into(),
                    retryable: false,
                })
            }),
        }
    }
}

async fn run_handler(behavior: HandlerBehavior) -> (TaskState, u32) {
    let service = TaskExecutionServiceBuilder::in_memory()
        .register_handler(Arc::new(ProvenanceHandler(behavior)))
        .expect("handler registration succeeds")
        .build()
        .await
        .expect("service builds");
    let accepted = service
        .submit(test_keyed(TaskRequest::new("panic-provenance", "1", Vec::new())))
        .await
        .expect("task is accepted");
    let result = time::timeout(std::time::Duration::from_secs(3), service.wait(accepted.id))
        .await
        .expect("task reaches a final state");
    let record = match result {
        Ok(record) => record,
        Err(TaskServiceError::Blocked) => service
            .get_summary(accepted.id)
            .await
            .expect("task query succeeds")
            .expect("task record remains retained"),
        Err(error) => panic!("task should reach a stored lifecycle state: {error}"),
    };
    service.shutdown().await.expect("service shuts down");
    (record.state, record.attempt)
}

#[tokio_test]
async fn test_handler_future_construction_panic_is_not_retried() {
    let (state, attempt) = run_handler(HandlerBehavior::ConstructPanic).await;
    assert!(matches!(state, TaskState::Panicked { .. }));
    assert_eq!(attempt, 1);
}

#[tokio_test]
async fn test_handler_future_poll_panic_is_not_retried() {
    let (state, attempt) = run_handler(HandlerBehavior::PollPanic).await;
    assert!(matches!(state, TaskState::Panicked { .. }));
    assert_eq!(attempt, 1);
}

#[tokio_test]
async fn test_application_error_category_panic_remains_failed() {
    let (state, attempt) = run_handler(HandlerBehavior::PanicCategory).await;
    assert!(matches!(state, TaskState::Failed { .. }));
    assert_eq!(attempt, 1);
}

fn test_keyed(mut request: TaskRequest) -> TaskRequest {
    static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(1);
    if request.idempotency_key.is_none() {
        request.idempotency_key = Some(format!(
            "test-request-{}",
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
    }
    request
}
