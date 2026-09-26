use std::num::NonZeroUsize;
use std::sync::Arc;

use qubit_task::TaskExecutionServiceBuilder;
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

/// Counts blocked service submissions against the memory store limit.
#[tokio::test]
async fn test_task_service_memory_limit_includes_blocked_records() {
    let store = MemoryTaskStore::with_limits(4, NonZeroUsize::new(64).unwrap(), NonZeroUsize::new(1).unwrap());
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
