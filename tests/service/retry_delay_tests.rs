// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::time::Duration;

use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::engine::LocalTaskExecutionEngine;
use qubit_task::handler::TaskContext;
use qubit_task::handler::TaskHandler;
use qubit_task::handler::TaskHandlerDescriptor;
use qubit_task::handler::TaskRunOutcome;
use qubit_task::handler::TaskRunResult;
use qubit_task::model::ResourceCapacity;
use qubit_task::model::TaskOutput;
use qubit_task::model::TaskRecord;
use qubit_task::model::TaskRequest;
use qubit_task::model::TaskRunError;
use qubit_task::model::TaskState;
use qubit_task::scheduling::FairFifoPolicy;
use qubit_task::service::CancelOutcome;
use qubit_task::service::RetryPolicy;
use qubit_task::service::RetryPolicyError;
use qubit_task::store::MemoryTaskStore;
use qubit_task::store::TaskFuture;
use tokio::task;
use tokio::test as tokio_test;
use tokio::time;

#[test]
fn test_retry_policy_defaults_to_one_second_with_sixty_second_cap() {
    let policy = RetryPolicy::default();
    assert_eq!(policy.initial_delay(), Duration::from_secs(1));
    assert_eq!(policy.max_delay(), Duration::from_secs(60));
}

#[test]
fn test_retry_policy_doubles_then_caps_delay() {
    let policy = RetryPolicy::new(Duration::from_secs(1), Duration::from_secs(60)).expect("delay range is valid");
    assert_eq!(policy.delay_for_attempt(1), Duration::from_secs(1));
    assert_eq!(policy.delay_for_attempt(2), Duration::from_secs(2));
    assert_eq!(policy.delay_for_attempt(3), Duration::from_secs(4));
    assert_eq!(policy.delay_for_attempt(7), Duration::from_secs(60));
    assert_eq!(policy.delay_for_attempt(u32::MAX), Duration::from_secs(60));
}

#[test]
fn test_retry_policy_rejects_invalid_ranges() {
    assert_eq!(
        RetryPolicy::new(Duration::ZERO, Duration::from_secs(1)),
        Err(RetryPolicyError::ZeroInitialDelay)
    );
    assert_eq!(
        RetryPolicy::new(Duration::from_secs(2), Duration::from_secs(1)),
        Err(RetryPolicyError::MaximumBelowInitial)
    );
}

struct RetryOnce(std::sync::atomic::AtomicUsize);

impl TaskHandler for RetryOnce {
    fn descriptor(&self) -> TaskHandlerDescriptor {
        TaskHandlerDescriptor {
            task_type: "retry-delay".into(),
            version: "1".into(),
        }
    }
    fn run<'a>(&'a self, _: &'a [u8], _: TaskContext) -> TaskFuture<'a, TaskRunResult> {
        Box::pin(async move {
            if self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                Err(TaskRunError {
                    category: "temporary".into(),
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
async fn test_retry_is_persisted_and_waits_until_deadline() {
    use std::sync::atomic::Ordering;

    use qubit_task::store::TaskStore;
    let store = std::sync::Arc::new(MemoryTaskStore::new(4));
    let handler = std::sync::Arc::new(RetryOnce(std::sync::atomic::AtomicUsize::new(0)));
    let service = TaskExecutionServiceBuilder::from_components(
        store.clone(),
        std::sync::Arc::new(LocalTaskExecutionEngine::new(ResourceCapacity {
            cpu_slots: 1,
            ..Default::default()
        })),
        std::sync::Arc::new(FairFifoPolicy::default()),
    )
    .register_handler(handler.clone())
    .unwrap()
    .retry_policy(RetryPolicy::new(Duration::from_millis(250), Duration::from_millis(250)).unwrap())
    .max_attempts(2)
    .require_recovery(false)
    .build()
    .await
    .unwrap();
    let accepted = service
        .submit(test_keyed(TaskRequest::new("retry-delay", "1", Vec::new())))
        .await
        .unwrap();
    time::timeout(Duration::from_secs(1), async {
        loop {
            let record = store.get(accepted.id).await.unwrap().unwrap();
            if record.retry_not_before_ms.is_some() {
                break;
            }
            task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(handler.0.load(Ordering::SeqCst), 1);
    time::sleep(Duration::from_millis(80)).await;
    assert_eq!(
        handler.0.load(Ordering::SeqCst),
        1,
        "second attempt started before its deadline"
    );
    let finished = time::timeout(Duration::from_secs(2), service.wait(accepted.id))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(finished.attempt, 2);
    assert!(finished.retry_not_before_ms.is_none());
    service.shutdown().await.unwrap();
}

#[tokio_test]
async fn test_task_record_without_retry_deadline_deserializes_as_ready() {
    use qubit_task::model::AcceptOutcome;
    use qubit_task::model::TaskId;
    use qubit_task::model::TaskRequest;
    use qubit_task::store::MemoryTaskStore;
    use qubit_task::store::TaskStore;

    let store = MemoryTaskStore::new(1);
    let record = match store
        .accept(TaskId::generate(), TaskRequest::new("legacy-json", "1", Vec::new()))
        .await
        .unwrap()
    {
        AcceptOutcome::Accepted(record) => record,
        AcceptOutcome::Existing(_) => panic!("new task cannot already exist"),
    };
    let mut json = serde_json::to_value(record).unwrap();
    json.as_object_mut().unwrap().remove("retry_not_before_ms");
    let restored: TaskRecord = serde_json::from_value(json).unwrap();
    assert_eq!(restored.retry_not_before_ms, None);
}

#[tokio_test]
async fn test_delayed_retry_can_be_cancelled_before_its_next_attempt() {
    use std::sync::atomic::Ordering;

    use qubit_task::store::TaskStore;

    let store = std::sync::Arc::new(MemoryTaskStore::new(4));
    let handler = std::sync::Arc::new(RetryOnce(std::sync::atomic::AtomicUsize::new(0)));
    let service = TaskExecutionServiceBuilder::from_components(
        store.clone(),
        std::sync::Arc::new(LocalTaskExecutionEngine::new(ResourceCapacity {
            cpu_slots: 1,
            ..Default::default()
        })),
        std::sync::Arc::new(FairFifoPolicy::default()),
    )
    .register_handler(handler.clone())
    .unwrap()
    .retry_policy(RetryPolicy::new(Duration::from_millis(300), Duration::from_millis(300)).unwrap())
    .max_attempts(2)
    .require_recovery(false)
    .build()
    .await
    .unwrap();
    let accepted = service
        .submit(test_keyed(TaskRequest::new("retry-delay", "1", Vec::new())))
        .await
        .unwrap();
    time::timeout(Duration::from_secs(1), async {
        loop {
            if store
                .get(accepted.id)
                .await
                .unwrap()
                .unwrap()
                .retry_not_before_ms
                .is_some()
            {
                break;
            }
            task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        service.cancel(accepted.id).await.unwrap(),
        CancelOutcome::CancelledBeforeStart
    );
    assert!(matches!(
        service.wait(accepted.id).await.unwrap().state,
        TaskState::Cancelled
    ));
    time::sleep(Duration::from_millis(350)).await;
    assert_eq!(handler.0.load(Ordering::SeqCst), 1);
    service.shutdown().await.unwrap();
}

#[allow(dead_code)]
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
