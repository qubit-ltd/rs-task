// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::num::NonZeroUsize;

use tokio::test as tokio_test;

use crate::model::MAX_TASK_QUERY_LIMIT;
use crate::model::TaskOutput;
use crate::model::TaskState;
use crate::model::TaskStateKind;
use crate::model::typed::ProgressCommand;
use crate::model::typed::ResourceRequest as EncodedResourceRequest;
use crate::model::typed::StartCommand;
use crate::model::typed::StoredPayload;
use crate::model::typed::StoredTaskRequest;
use crate::model::typed::TaskId as EncodedTaskId;
use crate::model::typed::TransitionCommand as EncodedTransitionCommand;
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
            type_id: qubit_model_id::ModelIdBuf::parse("qubit_task.tests.Payload").expect("model ID is valid"),
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
            retry_not_before_ms: None,
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
            retry_not_before_ms: None,
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

/// Prunes only old typed terminal tasks and releases their payload and key.
#[tokio_test]
async fn test_memory_store_prunes_typed_terminal_records_and_releases_capacity() {
    let store = MemoryTaskStore::with_limits(
        8,
        NonZeroUsize::new(3).expect("payload budget is positive"),
        NonZeroUsize::new(2).expect("record limit is positive"),
    );
    let old = TypedTaskStore::accept_encoded(&store, encoded_id(110), encoded_request(Some("pruned-key"), vec![1, 2]))
        .await
        .expect("old task is accepted");
    let kept = TypedTaskStore::accept_encoded(&store, encoded_id(111), encoded_request(None, vec![3]))
        .await
        .expect("second task fits the payload budget");
    let running = TypedTaskStore::start_encoded(
        &store,
        StartCommand {
            id: old.summary.id,
            expected_state_version: old.summary.state_version,
            started_at_ms: 1,
        },
    )
    .await
    .expect("old task starts");
    TypedTaskStore::transition_encoded(
        &store,
        EncodedTransitionCommand {
            id: running.id,
            expected_state_version: running.state_version,
            expected_attempt: running.attempt,
            retry_not_before_ms: None,
            state: TaskState::Succeeded,
            cancel_requested: false,
            cancel_error: None,
            output: Some(TaskOutput {
                summary: b"done".to_vec(),
            }),
            finished_at_ms: Some(10),
        },
    )
    .await
    .expect("old task becomes terminal");
    let kept_running = TypedTaskStore::start_encoded(
        &store,
        StartCommand {
            id: kept.summary.id,
            expected_state_version: kept.summary.state_version,
            started_at_ms: 2,
        },
    )
    .await
    .expect("kept task starts");
    TypedTaskStore::transition_encoded(
        &store,
        EncodedTransitionCommand {
            id: kept_running.id,
            expected_state_version: kept_running.state_version,
            expected_attempt: kept_running.attempt,
            retry_not_before_ms: None,
            state: TaskState::Cancelled,
            cancel_requested: false,
            cancel_error: None,
            output: None,
            finished_at_ms: Some(20),
        },
    )
    .await
    .expect("kept task becomes terminal");

    assert_eq!(
        TypedTaskStore::prune_terminal_before(&store, 10, NonZeroUsize::new(1).expect("prune batch size is positive"),)
            .await
            .expect("prune before the exact finish time succeeds"),
        0,
        "the cutoff is exclusive"
    );
    assert_eq!(
        TypedTaskStore::prune_terminal_before(&store, 11, NonZeroUsize::new(1).expect("prune batch size is positive"),)
            .await
            .expect("old terminal task is pruned"),
        1
    );
    assert!(
        TypedTaskStore::get_encoded_task(&store, old.summary.id)
            .await
            .expect("pruned task lookup succeeds")
            .is_none()
    );
    assert!(
        TypedTaskStore::get_encoded_task(&store, kept.summary.id)
            .await
            .expect("kept task lookup succeeds")
            .is_some()
    );
    let replacement = TypedTaskStore::accept_encoded(
        &store,
        encoded_id(112),
        encoded_request(Some("pruned-key"), vec![4, 5, 6]),
    )
    .await
    .expect("pruning releases both the idempotency key and payload budget");
    assert!(replacement.created);
}

/// Keeps ready scans and retry deadlines consistent with queued task state.
#[tokio_test]
async fn test_memory_store_ready_scan_and_retry_deadline_respect_retry_time() {
    let store = MemoryTaskStore::new(8);
    let accepted = TypedTaskStore::accept_encoded(&store, encoded_id(113), encoded_request(None, Vec::new()))
        .await
        .expect("typed task is accepted");
    let running = TypedTaskStore::start_encoded(
        &store,
        StartCommand {
            id: accepted.summary.id,
            expected_state_version: accepted.summary.state_version,
            started_at_ms: 1,
        },
    )
    .await
    .expect("typed task starts");
    let queued = TypedTaskStore::transition_encoded(
        &store,
        EncodedTransitionCommand {
            id: running.id,
            expected_state_version: running.state_version,
            expected_attempt: running.attempt,
            retry_not_before_ms: Some(50),
            state: TaskState::Queued,
            cancel_requested: false,
            cancel_error: None,
            output: None,
            finished_at_ms: None,
        },
    )
    .await
    .expect("retry is queued with a deadline");

    assert_eq!(
        TypedTaskStore::next_retry_deadline(&store, 49)
            .await
            .expect("next retry deadline is read"),
        Some(50)
    );
    assert_eq!(
        TypedTaskStore::next_retry_deadline(&store, 50)
            .await
            .expect("due deadline is excluded"),
        None
    );
    let waiting =
        TypedTaskStore::list_ready_queued(&store, None, NonZeroUsize::new(1).expect("page size is positive"), 49)
            .await
            .expect("ready scan succeeds before the deadline");
    assert!(waiting.records.is_empty());
    let ready =
        TypedTaskStore::list_ready_queued(&store, None, NonZeroUsize::new(1).expect("page size is positive"), 50)
            .await
            .expect("ready scan includes the task at the deadline");
    assert_eq!(ready.records.len(), 1);
    assert_eq!(ready.records[0].id, queued.id);
    let history = TypedTaskStore::list_encoded(
        &store,
        crate::model::typed::TaskQuery {
            states: vec![TaskStateKind::Queued],
            category: Some("integration".to_owned()),
            correlation_key: Some("correlation-1".to_owned()),
            limit: 1,
            ..crate::model::typed::TaskQuery::default()
        },
    )
    .await
    .expect("typed history filters by state, category, and correlation key");
    assert_eq!(history.records.len(), 1);
    assert_eq!(history.records[0].id, queued.id);
}

/// Rejects invalid typed transitions without mutating lifecycle state.
#[tokio_test]
async fn test_memory_store_rejects_invalid_typed_output_and_retry_deadline() {
    let store = MemoryTaskStore::new(8);
    let accepted = TypedTaskStore::accept_encoded(&store, encoded_id(114), encoded_request(None, Vec::new()))
        .await
        .expect("typed task is accepted");
    let output_error = TypedTaskStore::transition_encoded(
        &store,
        EncodedTransitionCommand {
            id: accepted.summary.id,
            expected_state_version: 0,
            expected_attempt: 0,
            retry_not_before_ms: None,
            state: TaskState::Queued,
            cancel_requested: false,
            cancel_error: None,
            output: Some(TaskOutput { summary: Vec::new() }),
            finished_at_ms: None,
        },
    )
    .await;
    assert!(matches!(
        output_error,
        Err(StoreError::InvalidRequest(
            "task output can only be stored with the succeeded state"
        ))
    ));

    let deadline_error = TypedTaskStore::transition_encoded(
        &store,
        EncodedTransitionCommand {
            id: accepted.summary.id,
            expected_state_version: 0,
            expected_attempt: 0,
            retry_not_before_ms: Some(5),
            state: TaskState::Running,
            cancel_requested: false,
            cancel_error: None,
            output: None,
            finished_at_ms: None,
        },
    )
    .await;
    assert!(matches!(
        deadline_error,
        Err(StoreError::InvalidRequest(
            "retry deadline is only valid for queued tasks"
        ))
    ));
    let current = TypedTaskStore::get_encoded_task(&store, accepted.summary.id)
        .await
        .expect("task lookup succeeds")
        .expect("invalid transitions leave task retained");
    assert_eq!(current.summary.state, TaskState::Queued);
    assert_eq!(current.summary.state_version, 0);
}

/// Filters encoded history and traverses the continuation cursor across pages.
#[tokio_test]
async fn test_memory_store_encoded_history_filters_and_paginates() {
    let store = MemoryTaskStore::new(8);
    for (id, category, correlation) in [
        (120, "wanted", "history-correlation"),
        (121, "wanted", "history-correlation"),
        (122, "other", "history-correlation"),
    ] {
        let mut request = encoded_request(None, Vec::new());
        request.category = Some(category.to_owned());
        request.correlation_key = Some(correlation.to_owned());
        TypedTaskStore::accept_encoded(&store, encoded_id(id), request)
            .await
            .expect("encoded task is accepted");
    }

    let first = TypedTaskStore::list_encoded(
        &store,
        crate::model::typed::TaskQuery {
            category: Some("wanted".to_owned()),
            correlation_key: Some("history-correlation".to_owned()),
            limit: 1,
            ..crate::model::typed::TaskQuery::default()
        },
    )
    .await
    .expect("first filtered page succeeds");
    assert_eq!(first.records.len(), 1);
    let cursor = first.next.expect("another matching record remains");

    let second = TypedTaskStore::list_encoded(
        &store,
        crate::model::typed::TaskQuery {
            states: vec![TaskStateKind::Queued],
            category: Some("wanted".to_owned()),
            correlation_key: Some("history-correlation".to_owned()),
            after: Some(cursor),
            limit: 1,
        },
    )
    .await
    .expect("continuation page succeeds");
    assert_eq!(second.records.len(), 1);
    assert!(second.next.is_none());
    assert_ne!(first.records[0].id, second.records[0].id);
}

/// Applies the ready page limit, continuation cursor, and maximum page bound.
#[tokio_test]
async fn test_memory_store_ready_queue_paginates_and_rejects_oversized_limit() {
    let store = MemoryTaskStore::new(8);
    for id in [123, 124, 125] {
        TypedTaskStore::accept_encoded(&store, encoded_id(id), encoded_request(None, Vec::new()))
            .await
            .expect("queued task is accepted");
    }

    let first = TypedTaskStore::list_ready_queued(
        &store,
        None,
        NonZeroUsize::new(1).expect("page limit is positive"),
        u64::MAX,
    )
    .await
    .expect("first ready page succeeds");
    assert_eq!(first.records.len(), 1);
    let cursor = first.next.expect("more ready tasks remain");
    let second = TypedTaskStore::list_ready_queued(
        &store,
        Some(cursor),
        NonZeroUsize::new(1).expect("page limit is positive"),
        u64::MAX,
    )
    .await
    .expect("ready continuation page succeeds");
    assert_eq!(second.records.len(), 1);
    assert!(second.next.is_some());
    assert_ne!(first.records[0].id, second.records[0].id);

    assert!(matches!(
        TypedTaskStore::list_ready_queued(
            &store,
            None,
            NonZeroUsize::new(MAX_TASK_QUERY_LIMIT + 1).expect("oversized page limit is positive"),
            u64::MAX,
        )
        .await,
        Err(StoreError::InvalidRequest("ready task page limit exceeds 256"))
    ));
}
