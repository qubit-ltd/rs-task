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
use crate::model::legacy::AcceptOutcome;
use crate::model::legacy::TaskId;
use crate::model::legacy::TaskQuery;
use crate::model::legacy::TaskRequest;
use crate::model::legacy::TransitionCommand;
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

fn legacy_request(task_type: &str, payload: Vec<u8>, idempotency_key: Option<&str>) -> TaskRequest {
    let mut request = TaskRequest::new(task_type, "1", payload);
    request.idempotency_key = idempotency_key.map(str::to_owned);
    request
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
async fn test_memory_store_idempotency_keys_are_global_across_request_apis() {
    let store = MemoryTaskStore::new(8);
    TypedTaskStore::accept_encoded(&store, encoded_id(100), encoded_request(Some("global-key"), Vec::new()))
        .await
        .expect("encoded request is accepted");
    assert!(matches!(
        store
            .accept(
                TaskId::generate(),
                legacy_request("legacy", Vec::new(), Some("global-key")),
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
        crate::model::next::TaskQuery {
            states: vec![TaskStateKind::Queued],
            category: Some("integration".to_owned()),
            correlation_key: Some("correlation-1".to_owned()),
            limit: 1,
            ..crate::model::next::TaskQuery::default()
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

/// Makes legacy history eviction remove oldest terminal rows across both APIs.
#[tokio_test]
async fn test_memory_store_terminal_history_evicts_oldest_across_apis() {
    let store = MemoryTaskStore::new(1);
    let legacy_id = TaskId::generate();
    let legacy = store
        .accept(legacy_id, TaskRequest::new("legacy-history", "1", vec![1]))
        .await
        .expect("legacy task is accepted");
    assert!(matches!(legacy, AcceptOutcome::Accepted(_)));
    transition(&store, legacy_id, TaskState::Running)
        .await
        .expect("legacy task starts");
    transition(&store, legacy_id, TaskState::Succeeded)
        .await
        .expect("legacy task succeeds");

    let typed = TypedTaskStore::accept_encoded(&store, encoded_id(115), encoded_request(None, vec![2]))
        .await
        .expect("typed task is accepted");
    let running = TypedTaskStore::start_encoded(
        &store,
        StartCommand {
            id: typed.summary.id,
            expected_state_version: typed.summary.state_version,
            started_at_ms: 1,
        },
    )
    .await
    .expect("typed task starts");
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
            output: None,
            finished_at_ms: Some(2),
        },
    )
    .await
    .expect("typed task succeeds");

    let newest_legacy_id = TaskId::generate();
    let newest = store
        .accept(
            newest_legacy_id,
            TaskRequest::new("new-legacy-history", "1", Vec::new()),
        )
        .await
        .expect("new legacy task is accepted");
    assert!(matches!(newest, AcceptOutcome::Accepted(_)));
    transition(&store, newest_legacy_id, TaskState::Running)
        .await
        .expect("new legacy task starts");
    transition(&store, newest_legacy_id, TaskState::Succeeded)
        .await
        .expect("new legacy task succeeds and enforces shared history capacity");

    assert!(store.get(legacy_id).await.expect("legacy lookup succeeds").is_none());
    assert!(
        TypedTaskStore::get_encoded_task(&store, typed.summary.id)
            .await
            .expect("typed lookup succeeds")
            .is_none()
    );
    assert!(
        store
            .get(newest_legacy_id)
            .await
            .expect("newest legacy lookup succeeds")
            .is_some()
    );
}

/// Verifies legacy pruning uses an exclusive acceptance cutoff and cleans keys.
#[tokio_test]
async fn test_memory_store_prunes_legacy_terminal_records_and_releases_keys() {
    let store = MemoryTaskStore::new(8);
    let id = TaskId::generate();
    let accepted = store
        .accept(
            id,
            legacy_request("legacy-prune", Vec::new(), Some("legacy-pruned-key")),
        )
        .await
        .expect("legacy task is accepted");
    let AcceptOutcome::Accepted(accepted) = accepted else {
        panic!("task id is new")
    };
    transition(&store, id, TaskState::Running)
        .await
        .expect("legacy task starts");
    transition(&store, id, TaskState::Succeeded)
        .await
        .expect("legacy task succeeds");
    assert_eq!(
        TaskStore::prune_terminal_before(
            &store,
            accepted.accepted_at_ms,
            NonZeroUsize::new(1).expect("batch size is positive"),
        )
        .await
        .expect("exclusive cutoff leaves same-time records"),
        0
    );
    assert_eq!(
        TaskStore::prune_terminal_before(
            &store,
            accepted.accepted_at_ms + 1,
            NonZeroUsize::new(1).expect("batch size is positive"),
        )
        .await
        .expect("old terminal record is removed"),
        1
    );
    assert!(
        store
            .get_by_idempotency_key("legacy-pruned-key")
            .await
            .expect("key lookup succeeds")
            .is_none()
    );
    assert!(matches!(
        store
            .accept(
                TaskId::generate(),
                legacy_request("legacy-prune", Vec::new(), Some("legacy-pruned-key")),
            )
            .await,
        Ok(AcceptOutcome::Accepted(_))
    ));
}

/// Accepts a keyed zero-payload record in the supplied store.
async fn accept(store: &MemoryTaskStore, key: &str) -> Result<AcceptOutcome, StoreError> {
    store
        .accept(
            TaskId::generate(),
            legacy_request("memory-capacity", Vec::new(), Some(key)),
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
    let request = legacy_request("large-summary", vec![9; 1024 * 1024], Some("large-summary-key"));
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

/// Exercises legacy request replay, duplicate-ID, validation, and byte-limit errors.
#[tokio_test]
async fn test_memory_store_legacy_acceptance_rejects_invalid_duplicate_and_oversized_requests() {
    let store = MemoryTaskStore::with_limits(
        4,
        NonZeroUsize::new(2).expect("payload budget is positive"),
        NonZeroUsize::new(4).expect("record limit is positive"),
    );
    let id = TaskId::generate();
    let request = legacy_request("legacy", vec![1, 2], Some("legacy-replay"));
    let accepted = store
        .accept(id, request.clone())
        .await
        .expect("initial request fits configured limits");
    assert!(matches!(accepted, AcceptOutcome::Accepted(_)));
    assert!(matches!(
        store.accept(TaskId::generate(), request.clone()).await,
        Ok(AcceptOutcome::Existing(record)) if record.id == id
    ));

    assert!(matches!(
        store.accept(id, legacy_request("duplicate", Vec::new(), None)).await,
        Err(StoreError::DuplicateTask)
    ));
    assert!(matches!(
        store
            .accept(
                TaskId::generate(),
                legacy_request("legacy", vec![3], Some("legacy-replay")),
            )
            .await,
        Err(StoreError::IdempotencyConflict)
    ));
    assert!(matches!(
        store
            .accept(
                TaskId::generate(),
                legacy_request("too-large", vec![3], None),
            )
            .await,
        Err(StoreError::CapacityExceeded {
            requested_bytes: 1,
            available_bytes: 0
        })
    ));

    let mut invalid = TaskRequest::new("", "1", Vec::new());
    invalid.task_type.clear();
    assert!(matches!(
        store.accept(TaskId::generate(), invalid).await,
        Err(StoreError::InvalidRequest(_))
    ));
}

/// Checks strict unfinished counts, owner epochs, and unsupported recovery scans.
#[tokio_test]
async fn test_memory_store_owner_and_unfinished_queries_follow_capabilities() {
    let store = MemoryTaskStore::new(4);
    let id = TaskId::generate();
    assert!(matches!(
        store.accept(id, TaskRequest::new("owner", "1", Vec::new())).await,
        Ok(AcceptOutcome::Accepted(_))
    ));
    assert!(TaskStore::has_unfinished_over_limit(&store, 0)
        .await
        .expect("unfinished count query succeeds"));
    assert!(!TaskStore::has_unfinished_over_limit(&store, 1)
        .await
        .expect("unfinished count query succeeds at the exact limit"));
    assert!(matches!(
        TaskStore::scan_unfinished(&store, None).await,
        Err(StoreError::UnsupportedCapability)
    ));

    let first = TypedTaskStore::acquire_owner(&store)
        .await
        .expect("first owner acquires the volatile store");
    assert_eq!(first.0, 1);
    assert!(matches!(
        TypedTaskStore::acquire_owner(&store).await,
        Err(StoreError::OwnerConflict)
    ));
    assert!(matches!(
        TypedTaskStore::release_owner(&store, crate::model::OwnerEpoch(first.0 + 1)).await,
        Err(StoreError::OwnerConflict)
    ));
    TypedTaskStore::release_owner(&store, first)
        .await
        .expect("current owner releases the store");
    let second = TypedTaskStore::acquire_owner(&store)
        .await
        .expect("released store can be acquired again");
    assert_eq!(second.0, first.0 + 1);
}

/// Filters legacy history and follows its cursor through the final page.
#[tokio_test]
async fn test_memory_store_legacy_history_filters_and_paginates() {
    let store = MemoryTaskStore::new(8);
    let first_id = TaskId::generate();
    let second_id = TaskId::generate();
    let third_id = TaskId::generate();
    for (id, kind, correlation) in [
        (first_id, "history-a", Some("wanted")),
        (second_id, "history-b", Some("wanted")),
        (third_id, "history-c", Some("other")),
    ] {
        let mut request = TaskRequest::new(kind, "1", Vec::new());
        request.correlation_key = correlation.map(str::to_owned);
        assert!(matches!(
            store.accept(id, request).await,
            Ok(AcceptOutcome::Accepted(_))
        ));
    }

    let first_page = store
        .list(TaskQuery {
            correlation_key: Some("wanted".to_owned()),
            limit: 1,
            ..TaskQuery::default()
        })
        .await
        .expect("filtered first page is available");
    assert_eq!(first_page.records.len(), 1);
    let cursor = first_page.next.expect("another matching record remains");
    let last_page = store
        .list(TaskQuery {
            states: vec![TaskStateKind::Queued],
            correlation_key: Some("wanted".to_owned()),
            after: Some(cursor),
            limit: 1,
        })
        .await
        .expect("filtered final page is available");
    assert_eq!(last_page.records.len(), 1);
    assert!(last_page.next.is_none());
    assert_ne!(last_page.records[0].id, first_page.records[0].id);
    assert!(store.get(third_id).await.expect("unmatched lookup succeeds").is_some());
}
