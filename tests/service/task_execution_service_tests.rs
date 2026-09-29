// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::num::NonZeroUsize;
use std::sync::Arc;

use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::model::MAX_IDEMPOTENCY_KEY_BYTES;
use qubit_task::model::MAX_TASK_QUERY_LIMIT;
use qubit_task::model::TaskQuery;
use qubit_task::model::TaskRequest;
use qubit_task::service::TaskServiceError;
use qubit_task::store::MemoryTaskStore;
use qubit_task::store::StoreError;

/// Applies the history page limit at the service boundary.
#[tokio::test]
async fn test_task_service_query_limit() {
    let service = TaskExecutionServiceBuilder::in_memory()
        .build()
        .await
        .expect("service builds");
    assert!(matches!(
        service
            .list(TaskQuery {
                limit: MAX_TASK_QUERY_LIMIT + 1,
                ..TaskQuery::default()
            })
            .await,
        Err(TaskServiceError::Store(StoreError::InvalidRequest(
            "task history page limit exceeds 256"
        )))
    ));
    let page = service
        .list(TaskQuery {
            limit: MAX_TASK_QUERY_LIMIT,
            ..TaskQuery::default()
        })
        .await
        .expect("maximum page is accepted");
    assert!(page.records.is_empty());
    service.shutdown().await.expect("empty service shuts down");
}

/// Rejects idempotency lookup keys outside the documented byte limit.
#[tokio::test]
async fn test_task_service_idempotency_lookup_rejects_invalid_key_lengths() {
    let service = TaskExecutionServiceBuilder::in_memory()
        .build()
        .await
        .expect("service builds");

    for key in [String::new(), "k".repeat(MAX_IDEMPOTENCY_KEY_BYTES + 1)] {
        assert!(matches!(
            service.get_by_idempotency_key(&key).await,
            Err(TaskServiceError::InvalidRequest(message))
                if message == "idempotency key must contain between 1 and 256 bytes"
        ));
    }

    service.shutdown().await.expect("empty service shuts down");
}

/// Counts blocked service submissions against the memory store limit.
#[tokio::test]
async fn test_task_service_memory_limit_includes_blocked_records() {
    let store = MemoryTaskStore::with_limits(
        4,
        NonZeroUsize::new(64).expect("payload limit is positive"),
        NonZeroUsize::new(1).expect("record limit is positive"),
    );
    let service = TaskExecutionServiceBuilder::default()
        .store(Arc::new(store))
        .build()
        .await
        .expect("service builds with the limited store");
    let record = service
        .submit(TaskRequest::new("unregistered", "1", Vec::new()).with_idempotency_key("blocked-one"))
        .await
        .expect("first task is accepted");
    assert!(matches!(service.wait(record.id).await, Err(TaskServiceError::Blocked)));
    assert!(matches!(
        service
            .submit(TaskRequest::new("unregistered", "1", Vec::new()).with_idempotency_key("blocked-two"))
            .await,
        Err(TaskServiceError::Store(StoreError::UnfinishedRecordLimitExceeded {
            limit: 1
        }))
    ));
    service
        .shutdown()
        .await
        .expect("service shuts down with a blocked record");
}
