// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::num::NonZeroUsize;

use qubit_task::model::AcceptOutcome;
use qubit_task::model::MAX_TASK_QUERY_LIMIT;
use qubit_task::model::TaskId;
use qubit_task::model::TaskQuery;
use qubit_task::model::TaskRequest;
use qubit_task::model::TaskState;
use qubit_task::model::TransitionCommand;
use qubit_task::store::DEFAULT_MAX_UNFINISHED_RECORDS;
use qubit_task::store::MemoryTaskStore;
use qubit_task::store::StoreError;
use qubit_task::store::TaskStore;

/// Accepts a keyed zero-payload record in the supplied store.
async fn accept(store: &MemoryTaskStore, key: &str) -> Result<AcceptOutcome, StoreError> {
    store
        .accept(
            TaskId::generate(),
            TaskRequest::new("memory-capacity", "1", Vec::new()).with_idempotency_key(key),
        )
        .await
}

/// Moves one record to the requested state using its current revision.
async fn transition(store: &MemoryTaskStore, id: TaskId, state: TaskState) -> Result<(), StoreError> {
    let record = store.get(id).await?.ok_or(StoreError::NotFound)?;
    store
        .transition(TransitionCommand {
            id,
            expected_version: record.state_version,
            expected_attempt: record.attempt,
            state,
            retry_not_before_ms: None,
            output: None,
            assigned_resources: Vec::new(),
            cancel_requested: false,
        })
        .await?;
    Ok(())
}

/// Rejects distinct zero-payload records at the configured nonterminal limit.
#[tokio::test]
async fn test_memory_store_unfinished_limit_rejects_zero_payload_records() {
    let store = MemoryTaskStore::with_limits(
        4,
        NonZeroUsize::new(64).expect("payload budget is positive"),
        NonZeroUsize::new(2).expect("record limit is positive"),
    );

    let first = match accept(&store, "memory-first").await.expect("first record is accepted") {
        AcceptOutcome::Accepted(record) => record,
        AcceptOutcome::Existing(_) => panic!("first key is new"),
    };
    match accept(&store, "memory-second")
        .await
        .expect("second record is accepted")
    {
        AcceptOutcome::Accepted(_) => {}
        AcceptOutcome::Existing(_) => panic!("second key is new"),
    }

    assert!(matches!(
        accept(&store, "memory-third").await,
        Err(StoreError::UnfinishedRecordLimitExceeded { limit: 2 })
    ));

    let replay = accept(&store, "memory-first")
        .await
        .expect("an idempotent replay remains available at capacity");
    assert!(matches!(replay, AcceptOutcome::Existing(record) if record.id == first.id));
}

/// Keeps blocked and requeued records charged until they become terminal.
#[tokio::test]
async fn test_memory_store_unfinished_limit_tracks_blocked_transitions() {
    let store = MemoryTaskStore::with_limits(
        0,
        NonZeroUsize::new(64).expect("payload budget is positive"),
        NonZeroUsize::new(1).expect("record limit is positive"),
    );
    let first = match accept(&store, "memory-blocked").await.expect("record is accepted") {
        AcceptOutcome::Accepted(record) => record,
        AcceptOutcome::Existing(_) => panic!("key is new"),
    };

    transition(
        &store,
        first.id,
        TaskState::Blocked {
            reason: "handler missing".into(),
        },
    )
    .await
    .expect("queued record becomes blocked");
    assert!(matches!(
        accept(&store, "memory-while-blocked").await,
        Err(StoreError::UnfinishedRecordLimitExceeded { limit: 1 })
    ));

    transition(&store, first.id, TaskState::Queued)
        .await
        .expect("blocked record is requeued");
    assert!(matches!(
        accept(&store, "memory-while-requeued").await,
        Err(StoreError::UnfinishedRecordLimitExceeded { limit: 1 })
    ));

    transition(&store, first.id, TaskState::Cancelled)
        .await
        .expect("queued record becomes terminal");
    assert!(matches!(
        accept(&store, "memory-after-terminal").await,
        Ok(AcceptOutcome::Accepted(_))
    ));
}

#[tokio::test]
async fn test_memory_summary_reads_preserve_large_payload_and_lifecycle_metadata() {
    let store = MemoryTaskStore::new(8);
    let request =
        TaskRequest::new("large-summary", "v1", vec![9; 1024 * 1024]).with_idempotency_key("large-summary-key");
    let accepted = match store.accept(TaskId::generate(), request).await.unwrap() {
        AcceptOutcome::Accepted(record) => record,
        AcceptOutcome::Existing(_) => unreachable!(),
    };
    let summary = store.get_summary(accepted.id).await.unwrap().unwrap();
    assert_eq!(summary.request.task_type, "large-summary");
    assert_eq!(
        store
            .list(TaskQuery {
                limit: 1,
                ..TaskQuery::default()
            })
            .await
            .unwrap()
            .records[0],
        summary
    );
    let running = store
        .transition(TransitionCommand {
            id: accepted.id,
            expected_version: summary.state_version,
            expected_attempt: summary.attempt,
            state: TaskState::Running,
            retry_not_before_ms: None,
            output: None,
            assigned_resources: vec!["cpu-0".into()],
            cancel_requested: false,
        })
        .await
        .unwrap();
    assert!(matches!(running.state, TaskState::Running));
    assert_eq!(running.state_version, summary.state_version + 1);
    assert_eq!(
        store.get(accepted.id).await.unwrap().unwrap().request.payload,
        vec![9; 1024 * 1024]
    );
}

/// Applies the documented default cap to zero-payload nonterminal records.
#[tokio::test]
async fn test_memory_store_default_unfinished_limit() {
    let store = MemoryTaskStore::new(0);
    for _ in 0..DEFAULT_MAX_UNFINISHED_RECORDS {
        assert!(matches!(
            store
                .accept(TaskId::generate(), TaskRequest::new("default-cap", "1", Vec::new()))
                .await,
            Ok(AcceptOutcome::Accepted(_))
        ));
    }
    assert!(matches!(
        store
            .accept(TaskId::generate(), TaskRequest::new("default-cap", "1", Vec::new()))
            .await,
        Err(StoreError::UnfinishedRecordLimitExceeded {
            limit: DEFAULT_MAX_UNFINISHED_RECORDS
        })
    ));
}

/// Enforces the common history page cap while retaining cursor order.
#[tokio::test]
async fn test_memory_store_task_query_limit() {
    let store = MemoryTaskStore::new(300);
    for index in 0..=MAX_TASK_QUERY_LIMIT {
        store
            .accept(
                TaskId::generate(),
                TaskRequest::new("page", "1", index.to_le_bytes().to_vec()),
            )
            .await
            .expect("record is accepted");
    }
    assert!(matches!(
        store
            .list(TaskQuery {
                limit: MAX_TASK_QUERY_LIMIT + 1,
                ..TaskQuery::default()
            })
            .await,
        Err(StoreError::InvalidRequest("task history page limit exceeds 256"))
    ));
    let page = store
        .list(TaskQuery {
            limit: MAX_TASK_QUERY_LIMIT,
            ..TaskQuery::default()
        })
        .await
        .expect("maximum page is accepted");
    assert_eq!(page.records.len(), MAX_TASK_QUERY_LIMIT);
    assert!(page.next.is_some());
    let first = store
        .list(TaskQuery {
            limit: 0,
            ..TaskQuery::default()
        })
        .await
        .unwrap();
    assert_eq!(first.records.len(), 1);
}
