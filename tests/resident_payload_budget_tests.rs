// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
// =============================================================================
use std::num::NonZeroUsize;

use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::model::AcceptOutcome;
use qubit_task::model::TaskId;
use qubit_task::model::TaskQuery;
use qubit_task::model::TaskRequest;
use qubit_task::model::TaskState;
use qubit_task::model::TransitionCommand;
use qubit_task::store::MemoryTaskStore;
use qubit_task::store::StoreError;
use qubit_task::store::TaskStore;

async fn accept(store: &MemoryTaskStore, key: &str, payload: Vec<u8>) -> qubit_task::model::TaskRecord {
    match store
        .accept(
            TaskId::generate(),
            TaskRequest::new("payload-budget", "1", payload).with_idempotency_key(key),
        )
        .await
        .expect("store accepts request")
    {
        AcceptOutcome::Accepted(record) => record,
        AcceptOutcome::Existing(_) => panic!("new key cannot be an existing request"),
    }
}

async fn cancel(store: &MemoryTaskStore, record: &qubit_task::model::TaskRecord) {
    store
        .transition(TransitionCommand {
            id: record.id,
            expected_version: record.state_version,
            expected_attempt: record.attempt,
            state: TaskState::Cancelled,
            retry_not_before_ms: None,
            output: None,
            assigned_resources: Vec::new(),
            cancel_requested: false,
        })
        .await
        .expect("queued task can be cancelled");
}

#[tokio::test]
async fn test_memory_store_rejects_over_budget_active_payload_without_eviction() {
    let store = MemoryTaskStore::with_payload_budget(8, NonZeroUsize::new(8).expect("budget is nonzero"));
    let retained = accept(&store, "active-key", vec![0; 8]).await;

    assert!(matches!(
        store
            .accept(
                TaskId::generate(),
                TaskRequest::new("payload-budget", "1", vec![1]).with_idempotency_key("rejected-key")
            )
            .await,
        Err(StoreError::CapacityExceeded { .. })
    ));
    assert_eq!(store.get(retained.id).await.unwrap().unwrap().id, retained.id);
    assert!(store.get_by_idempotency_key("rejected-key").await.unwrap().is_none());
}

#[tokio::test]
async fn test_memory_store_evicts_old_terminal_payload_and_its_key_to_admit_work() {
    let store = MemoryTaskStore::with_payload_budget(8, NonZeroUsize::new(8).expect("budget is nonzero"));
    let terminal = accept(&store, "terminal-key", vec![0; 8]).await;
    cancel(&store, &terminal).await;

    let accepted = accept(&store, "replacement-key", vec![1; 8]).await;

    assert!(store.get(terminal.id).await.unwrap().is_none());
    assert!(store.get_by_idempotency_key("terminal-key").await.unwrap().is_none());
    assert_eq!(store.get(accepted.id).await.unwrap().unwrap().id, accepted.id);
}

#[tokio::test]
async fn test_memory_store_preserves_terminal_history_when_active_payload_blocks_admission() {
    let store = MemoryTaskStore::with_payload_budget(8, NonZeroUsize::new(8).expect("budget is nonzero"));
    let terminal = accept(&store, "small-terminal-key", vec![0; 4]).await;
    cancel(&store, &terminal).await;
    let active = accept(&store, "active-four-key", vec![1; 4]).await;

    assert!(matches!(
        store
            .accept(
                TaskId::generate(),
                TaskRequest::new("payload-budget", "1", vec![2; 5]).with_idempotency_key("too-large-key")
            )
            .await,
        Err(StoreError::CapacityExceeded { .. })
    ));
    assert_eq!(store.get(terminal.id).await.unwrap().unwrap().id, terminal.id);
    assert_eq!(store.get(active.id).await.unwrap().unwrap().id, active.id);
}

#[tokio::test]
async fn test_memory_store_does_not_charge_identical_idempotent_accept_twice() {
    let store = MemoryTaskStore::with_payload_budget(8, NonZeroUsize::new(4).expect("budget is nonzero"));
    let id = TaskId::generate();
    let request = TaskRequest::new("payload-budget", "1", vec![7; 4]).with_idempotency_key("same-request");
    let first = store.accept(id, request.clone()).await.expect("first request accepted");
    let repeated = store
        .accept(TaskId::generate(), request)
        .await
        .expect("same request resolves to existing record");
    assert!(matches!(first, AcceptOutcome::Accepted(_)));
    assert!(matches!(repeated, AcceptOutcome::Existing(record) if record.id == id));

    let second = accept(&store, "next-request", Vec::new()).await;
    assert_eq!(store.get(second.id).await.unwrap().unwrap().id, second.id);
}

#[tokio::test]
async fn test_memory_store_rejects_payload_larger_than_budget_without_mutation() {
    let store = MemoryTaskStore::with_payload_budget(8, NonZeroUsize::new(3).expect("budget is nonzero"));
    let error = store
        .accept(
            TaskId::generate(),
            TaskRequest::new("payload-budget", "1", vec![9; 4]).with_idempotency_key("oversized"),
        )
        .await
        .expect_err("oversized request cannot fit even after all eviction");
    assert!(matches!(
        error,
        StoreError::CapacityExceeded {
            requested_bytes: 4,
            available_bytes: 3
        }
    ));
    assert!(store.get_by_idempotency_key("oversized").await.unwrap().is_none());
}

#[tokio::test]
async fn test_service_store_capacity_error_does_not_pause_future_admission() {
    let service =
        TaskExecutionServiceBuilder::in_memory_with_payload_budget(NonZeroUsize::new(4).expect("budget is nonzero"))
            .build()
            .await
            .expect("service builds");
    let first = service
        .submit(TaskRequest::new("unhandled", "1", vec![1; 4]).with_idempotency_key("first-payload"))
        .await
        .expect("first payload is retained");
    let error = service
        .submit(TaskRequest::new("unhandled", "1", vec![2]).with_idempotency_key("rejected-payload"))
        .await
        .expect_err("payload budget is full");
    assert!(matches!(
        error,
        qubit_task::service::TaskServiceError::Store(StoreError::CapacityExceeded { .. })
    ));
    assert_eq!(service.last_store_error(), None);

    assert!(matches!(
        service.wait(first.id).await,
        Err(qubit_task::service::TaskServiceError::Blocked)
    ));
    service.cancel(first.id).await.expect("blocked task is cancelled");
    let next = service
        .submit(TaskRequest::new("unhandled", "1", vec![3; 4]).with_idempotency_key("replacement-payload"))
        .await
        .expect("terminal payload is evicted and service remains healthy");
    assert_ne!(first.id, next.id);
    service.shutdown().await.expect("service shuts down");
}

#[tokio::test]
async fn test_memory_store_pruning_releases_payload_budget() {
    let store = MemoryTaskStore::with_payload_budget(8, NonZeroUsize::new(8).expect("budget is nonzero"));
    let terminal = accept(&store, "pruned-key", vec![4; 8]).await;
    cancel(&store, &terminal).await;
    assert_eq!(
        store
            .prune_terminal_before(u64::MAX, NonZeroUsize::new(1).expect("row limit is nonzero"))
            .await
            .expect("terminal row is pruned"),
        1
    );
    assert!(store.get_by_idempotency_key("pruned-key").await.unwrap().is_none());
    let replacement = accept(&store, "after-prune", vec![5; 8]).await;
    assert_eq!(store.get(replacement.id).await.unwrap().unwrap().id, replacement.id);
}

#[tokio::test]
async fn test_concurrent_accepts_cannot_exceed_payload_budget() {
    let store = MemoryTaskStore::with_payload_budget(8, NonZeroUsize::new(4).expect("budget is nonzero"));
    let first_store = &store;
    let second_store = &store;
    let (first, second) = tokio::join!(
        first_store.accept(
            TaskId::generate(),
            TaskRequest::new("payload-budget", "1", vec![1; 4]).with_idempotency_key("concurrent-one"),
        ),
        second_store.accept(
            TaskId::generate(),
            TaskRequest::new("payload-budget", "1", vec![2; 4]).with_idempotency_key("concurrent-two"),
        ),
    );
    assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
    let retained = store
        .list(TaskQuery {
            limit: 8,
            ..TaskQuery::default()
        })
        .await
        .unwrap();
    assert_eq!(retained.records.len(), 1);
    let detail = store.get(retained.records[0].id).await.unwrap().unwrap();
    assert_eq!(detail.request.payload.len(), 4);
}
