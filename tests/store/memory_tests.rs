// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::num::NonZeroUsize;

use tokio::test as tokio_test;

use crate::model::AcceptOutcome;
use crate::model::MAX_TASK_QUERY_LIMIT;
use crate::model::TaskId;
use crate::model::TaskQuery;
use crate::model::TaskRequest;
use crate::model::TaskState;
use crate::model::TransitionCommand;
use crate::model::next::ProgressCommand;
use crate::model::next::ResourceRequest as EncodedResourceRequest;
use crate::model::next::StartCommand;
use crate::model::next::StoredPayload;
use crate::model::next::StoredTaskRequest;
use crate::model::next::TaskId as EncodedTaskId;
use crate::model::next::TransitionCommand as EncodedTransitionCommand;
use crate::store::DEFAULT_MAX_UNFINISHED_RECORDS;
use crate::store::LegacyTaskStore as TaskStore;
use crate::store::MemoryTaskStore;
use crate::store::StoreError;
use crate::store::TaskStore as TypedTaskStore;

fn encoded_request(key: Option<&str>, bytes: Vec<u8>) -> StoredTaskRequest {
    let mut metadata = qubit_metadata::Metadata::new();
    metadata.insert("trace", "memory-test");
    StoredTaskRequest {
        kind_id: "memory.encoded".to_owned(),
        category: Some("integration".to_owned()),
        payload: StoredPayload {
            type_id: qubit_model_metadata::metadata::ModelIdBuf::parse("qubit_task.tests.Payload")
                .expect("model ID is valid"),
            schema_version: 2,
            codec_id: "qubit.bytes.json".to_owned(),
            bytes,
        },
        metadata,
        resource_limit: EncodedResourceRequest::default(),
        correlation_key: Some("correlation-1".to_owned()),
        idempotency_key: key.map(str::to_owned),
    }
}

fn encoded_id(value: u64) -> EncodedTaskId {
    EncodedTaskId::from_id(qubit_id::Id::new(value))
}

#[tokio_test]
async fn test_memory_store_accepts_and_reads_encoded_task_atomically() {
    let store = MemoryTaskStore::new(8);
    let request = encoded_request(Some("encoded-key"), vec![4, 5, 6]);
    let accepted = TypedTaskStore::accept_encoded(&store, encoded_id(90), request.clone())
        .await
        .expect("encoded request is accepted");
    assert!(accepted.created);
    assert_eq!(accepted.summary.state, TaskState::Queued);
    assert_eq!(accepted.summary.state_version, 0);
    assert_eq!(accepted.summary.progress, None);
    assert_eq!(accepted.summary.category.as_deref(), Some("integration"));

    let loaded = TypedTaskStore::get_encoded_task(&store, encoded_id(90))
        .await
        .expect("encoded task lookup succeeds")
        .expect("accepted task is retained");
    assert_eq!(loaded.request, request);
    assert_eq!(loaded.summary, accepted.summary);

    let replay = TypedTaskStore::accept_encoded(&store, encoded_id(91), request)
        .await
        .expect("identical idempotent request returns the retained task");
    assert!(!replay.created);
    assert_eq!(replay.summary.id, encoded_id(90));
}

#[tokio_test]
async fn test_memory_store_rejects_encoded_idempotency_conflicts_and_duplicate_ids() {
    let store = MemoryTaskStore::new(8);
    TypedTaskStore::accept_encoded(
        &store,
        encoded_id(92),
        encoded_request(Some("encoded-conflict"), vec![1]),
    )
    .await
    .expect("initial request is accepted");
    assert!(matches!(
        TypedTaskStore::accept_encoded(
            &store,
            encoded_id(93),
            encoded_request(Some("encoded-conflict"), vec![2])
        )
        .await,
        Err(StoreError::IdempotencyConflict)
    ));
    assert!(matches!(
        TypedTaskStore::accept_encoded(&store, encoded_id(92), encoded_request(None, vec![3])).await,
        Err(StoreError::DuplicateTask)
    ));
}

#[tokio_test]
async fn test_memory_store_encoded_acceptance_obeys_payload_and_unfinished_limits() {
    let store = MemoryTaskStore::with_limits(
        0,
        NonZeroUsize::new(2).expect("payload budget is positive"),
        NonZeroUsize::new(1).expect("record limit is positive"),
    );
    assert!(matches!(
        TypedTaskStore::accept_encoded(&store, encoded_id(95), encoded_request(None, vec![1, 2, 3])).await,
        Err(StoreError::CapacityExceeded {
            requested_bytes: 3,
            available_bytes: 2
        })
    ));
    TypedTaskStore::accept_encoded(&store, encoded_id(96), encoded_request(None, vec![1, 2]))
        .await
        .expect("payload at the configured byte limit is accepted");
    assert!(matches!(
        TypedTaskStore::accept_encoded(&store, encoded_id(97), encoded_request(None, Vec::new())).await,
        Err(StoreError::UnfinishedRecordLimitExceeded { limit: 1 })
    ));
}

#[tokio_test]
async fn test_memory_store_encoded_acceptance_evicts_terminal_legacy_payloads() {
    let store = MemoryTaskStore::with_payload_budget(1, NonZeroUsize::new(6).expect("payload budget is positive"));
    let terminal = TypedTaskStore::accept_encoded(&store, encoded_id(98), encoded_request(None, vec![1, 2, 3]))
        .await
        .expect("initial typed request is accepted");
    let started = TypedTaskStore::start_encoded(
        &store,
        StartCommand {
            id: terminal.summary.id,
            expected_state_version: terminal.summary.state_version,
            started_at_ms: 1,
        },
    )
    .await
    .expect("typed task starts");
    TypedTaskStore::transition_encoded(
        &store,
        EncodedTransitionCommand {
            id: started.id,
            expected_state_version: started.state_version,
            expected_attempt: started.attempt,
            state: TaskState::Succeeded,
            cancel_requested: false,
            cancel_error: None,
            output: None,
            finished_at_ms: Some(2),
        },
    )
    .await
    .expect("typed task becomes terminal");

    TypedTaskStore::accept_encoded(&store, encoded_id(99), encoded_request(None, vec![4, 5, 6, 7]))
        .await
        .expect("typed acceptance evicts the terminal typed payload");
    assert!(
        TypedTaskStore::get_encoded_task(&store, terminal.summary.id)
            .await
            .expect("typed lookup succeeds")
            .is_none()
    );
}

#[tokio_test]
async fn test_memory_store_idempotency_keys_are_global_across_request_apis() {
    let store = MemoryTaskStore::new(8);
    TypedTaskStore::accept_encoded(&store, encoded_id(100), encoded_request(Some("global-key"), Vec::new()))
        .await
        .expect("encoded request is accepted");
    assert!(matches!(
        store
            .accept(
                TaskId::generate(),
                TaskRequest::new("legacy", "1", Vec::new()).with_idempotency_key("global-key"),
            )
            .await,
        Err(StoreError::IdempotencyConflict)
    ));
}

#[tokio_test]
async fn test_memory_store_progress_rejects_queued_task_without_changing_state_version() {
    let store = MemoryTaskStore::new(8);
    let accepted = TypedTaskStore::accept_encoded(&store, encoded_id(94), encoded_request(None, Vec::new()))
        .await
        .expect("encoded request is accepted");
    let result = TypedTaskStore::update_progress(
        &store,
        ProgressCommand::new(accepted.summary.id, 1, 1, None, Vec::new(), 10),
    )
    .await;
    assert!(matches!(result, Err(StoreError::Conflict)));
    let after = TypedTaskStore::get_encoded_task(&store, accepted.summary.id)
        .await
        .expect("task lookup succeeds")
        .expect("task remains retained");
    assert_eq!(after.summary.state_version, accepted.summary.state_version);
    assert_eq!(after.summary.progress, None);
}

#[tokio_test]
async fn test_memory_store_start_and_progress_versions_are_independent() {
    let store = MemoryTaskStore::new(8);
    let accepted = TypedTaskStore::accept_encoded(&store, encoded_id(98), encoded_request(None, Vec::new()))
        .await
        .expect("encoded request is accepted");
    let started = TypedTaskStore::start_encoded(
        &store,
        StartCommand {
            id: accepted.summary.id,
            expected_state_version: accepted.summary.state_version,
            started_at_ms: 20,
        },
    )
    .await
    .expect("queued task starts");
    assert_eq!(started.state, TaskState::Running);
    assert_eq!(started.state_version, 1);
    assert_eq!(started.attempt, 1);
    assert_eq!(started.started_at_ms, Some(20));

    let first = TypedTaskStore::update_progress(
        &store,
        ProgressCommand::new(started.id, started.attempt, 1, None, Vec::new(), 21),
    )
    .await
    .expect("first progress snapshot is accepted");
    assert_eq!(
        first.progress.as_ref().map(|progress| progress.progress_version),
        Some(1)
    );
    assert_eq!(first.state_version, started.state_version);
    assert!(matches!(
        TypedTaskStore::update_progress(
            &store,
            ProgressCommand::new(started.id, started.attempt, 1, None, Vec::new(), 22)
        )
        .await,
        Err(StoreError::Conflict)
    ));
    let second = TypedTaskStore::update_progress(
        &store,
        ProgressCommand::new(started.id, started.attempt, 2, None, Vec::new(), 23),
    )
    .await
    .expect("newer progress version is accepted");
    assert_eq!(
        second.progress.as_ref().map(|progress| progress.progress_version),
        Some(2)
    );
    assert_eq!(second.state_version, started.state_version);
    assert!(matches!(
        TypedTaskStore::start_encoded(
            &store,
            StartCommand {
                id: started.id,
                expected_state_version: started.state_version,
                started_at_ms: 24,
            }
        )
        .await,
        Err(StoreError::Conflict)
    ));

    let queued = TypedTaskStore::transition_encoded(
        &store,
        EncodedTransitionCommand {
            id: second.id,
            expected_state_version: second.state_version,
            expected_attempt: second.attempt,
            state: TaskState::Queued,
            cancel_requested: false,
            cancel_error: None,
            output: None,
            finished_at_ms: None,
        },
    )
    .await
    .expect("retryable task returns to queue");
    assert!(
        queued.progress.is_some(),
        "previous attempt remains visible until restart"
    );

    let restarted = TypedTaskStore::start_encoded(
        &store,
        StartCommand {
            id: queued.id,
            expected_state_version: queued.state_version,
            started_at_ms: 24,
        },
    )
    .await
    .expect("next attempt starts");
    assert_eq!(restarted.attempt, 2);
    assert_eq!(restarted.progress, None, "new attempt clears old progress");
    let retry_progress = TypedTaskStore::update_progress(
        &store,
        ProgressCommand::new(restarted.id, restarted.attempt, 1, None, Vec::new(), 25),
    )
    .await
    .expect("progress version restarts at one for the new attempt");
    assert_eq!(
        retry_progress
            .progress
            .as_ref()
            .map(|progress| progress.progress_version),
        Some(1)
    );
}

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
#[tokio_test]
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
#[tokio_test]
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

#[tokio_test]
async fn test_memory_summary_reads_preserve_large_payload_and_lifecycle_metadata() {
    let store = MemoryTaskStore::new(8);
    let request =
        TaskRequest::new("large-summary", "v1", vec![9; 1024 * 1024]).with_idempotency_key("large-summary-key");
    let accepted = match store
        .accept(TaskId::generate(), request)
        .await
        .expect("large record is accepted")
    {
        AcceptOutcome::Accepted(record) => record,
        AcceptOutcome::Existing(_) => panic!("summary key is new"),
    };
    let summary = store
        .get_summary(accepted.id)
        .await
        .expect("summary lookup succeeds")
        .expect("accepted summary is retained");
    assert_eq!(summary.request.task_type, "large-summary");
    assert_eq!(
        store
            .list(TaskQuery {
                limit: 1,
                ..TaskQuery::default()
            })
            .await
            .expect("summary page lookup succeeds")
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
        .expect("record transitions to running");
    assert!(matches!(running.state, TaskState::Running));
    assert_eq!(running.state_version, summary.state_version + 1);
    assert_eq!(
        store
            .get(accepted.id)
            .await
            .expect("record lookup succeeds")
            .expect("record is retained")
            .request
            .payload,
        vec![9; 1024 * 1024]
    );
}

/// Applies the documented default cap to zero-payload nonterminal records.
#[tokio_test]
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
#[tokio_test]
async fn test_memory_store_task_query_limit() {
    let store = MemoryTaskStore::new(300);
    for index in 0..=MAX_TASK_QUERY_LIMIT {
        let accepted = store
            .accept(
                TaskId::generate(),
                TaskRequest::new("page", "1", index.to_le_bytes().to_vec()),
            )
            .await
            .expect("record is accepted");
        assert!(matches!(accepted, AcceptOutcome::Accepted(_)));
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
        .expect("zero limit selects the default one-row page");
    assert_eq!(first.records.len(), 1);
}
