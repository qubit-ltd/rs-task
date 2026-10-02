// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::num::NonZeroUsize;

use qubit_task::model::ResourceRequest;
use qubit_task::model::StartCommand;
use qubit_task::model::StoredPayload;
use qubit_task::model::StoredTaskRequest;
use qubit_task::model::TaskId;
use qubit_task::model::TaskQuery;
use qubit_task::model::TaskState;
use qubit_task::model::TaskStateKind;
use qubit_task::model::TransitionCommand;
use qubit_task::store::StoreError;
use qubit_task::store::TaskStore;

/// Creates one persisted payload with an optional caller idempotency key.
fn request(idempotency_key: Option<&str>, bytes: &[u8]) -> StoredTaskRequest {
    StoredTaskRequest {
        kind_id: "contract.handler".into(),
        category: Some("contract".into()),
        payload: StoredPayload {
            type_id: qubit_model_metadata::metadata::ModelIdBuf::parse("contract.Payload").expect("valid model ID"),
            schema_version: 1,
            codec_id: "contract.bytes".into(),
            bytes: bytes.to_vec(),
        },
        metadata: qubit_metadata::Metadata::new(),
        resource_limit: ResourceRequest::default(),
        correlation_key: Some("trace-contract".into()),
        idempotency_key: idempotency_key.map(str::to_owned),
    }
}

/// Checks encoded acceptance, idempotency, lifecycle CAS, and typed reads.
pub async fn check_core_contract(store: &dyn TaskStore) {
    let id = TaskId::from_id(qubit_id::Id::new(101));
    let duplicate_id = TaskId::from_id(qubit_id::Id::new(102));
    let submitted = request(Some("contract-key"), &[1, 2, 3]);
    let accepted = store
        .accept_encoded(id, submitted.clone())
        .await
        .expect("encoded acceptance succeeds");
    assert!(accepted.created);
    assert_eq!(accepted.summary.id, id);
    assert_eq!(accepted.summary.state, TaskState::Queued);
    assert_eq!(accepted.summary.kind_id, "contract.handler");
    assert_eq!(accepted.summary.category.as_deref(), Some("contract"));

    let replay = store
        .accept_encoded(duplicate_id, submitted)
        .await
        .expect("identical idempotent request replays");
    assert!(!replay.created);
    assert_eq!(replay.summary.id, id);

    assert!(matches!(
        store
            .accept_encoded(duplicate_id, request(Some("contract-key"), &[9]))
            .await,
        Err(StoreError::IdempotencyConflict)
    ));

    let loaded = store
        .get_encoded_task(id)
        .await
        .expect("typed read succeeds")
        .expect("accepted task remains stored");
    assert_eq!(loaded.summary, accepted.summary);
    assert_eq!(loaded.request.payload.bytes, [1, 2, 3]);
    assert!(matches!(
        store
            .transition_encoded(TransitionCommand {
                id,
                expected_state_version: accepted.summary.state_version,
                expected_attempt: accepted.summary.attempt,
                retry_not_before_ms: None,
                state: TaskState::Succeeded,
                output: None,
                cancel_requested: false,
                cancel_error: None,
                finished_at_ms: Some(accepted.summary.accepted_at_ms),
            })
            .await,
        Err(StoreError::Conflict)
    ));

    let running = store
        .start_encoded(StartCommand {
            id,
            expected_state_version: accepted.summary.state_version,
            started_at_ms: accepted.summary.accepted_at_ms,
        })
        .await
        .expect("queued task starts");
    assert_eq!(running.state, TaskState::Running);
    assert_eq!(running.attempt, 1);
    assert!(matches!(
        store
            .start_encoded(StartCommand {
                id,
                expected_state_version: accepted.summary.state_version,
                started_at_ms: accepted.summary.accepted_at_ms,
            })
            .await,
        Err(StoreError::Conflict)
    ));

    let succeeded = store
        .transition_encoded(TransitionCommand {
            id,
            expected_state_version: running.state_version,
            expected_attempt: running.attempt,
            retry_not_before_ms: None,
            state: TaskState::Succeeded,
            output: None,
            cancel_requested: false,
            cancel_error: None,
            finished_at_ms: Some(running.accepted_at_ms),
        })
        .await
        .expect("running task reaches a terminal state");
    assert_eq!(succeeded.state, TaskState::Succeeded);
    assert!(matches!(
        store
            .transition_encoded(TransitionCommand {
                id,
                expected_state_version: running.state_version,
                expected_attempt: running.attempt.saturating_add(1),
                retry_not_before_ms: None,
                state: TaskState::Cancelled,
                output: None,
                cancel_requested: false,
                cancel_error: None,
                finished_at_ms: Some(running.accepted_at_ms),
            })
            .await,
        Err(StoreError::Conflict)
    ));

    let retry_id = TaskId::from_id(qubit_id::Id::new(103));
    let accepted = store
        .accept_encoded(retry_id, request(None, &[4, 5, 6]))
        .await
        .expect("retry candidate acceptance succeeds");
    let running = store
        .start_encoded(StartCommand {
            id: retry_id,
            expected_state_version: accepted.summary.state_version,
            started_at_ms: accepted.summary.accepted_at_ms,
        })
        .await
        .expect("retry candidate starts");
    let retry_not_before_ms = running.accepted_at_ms + 60_000;
    let queued = store
        .transition_encoded(TransitionCommand {
            id: retry_id,
            expected_state_version: running.state_version,
            expected_attempt: running.attempt,
            state: TaskState::Queued,
            cancel_requested: false,
            cancel_error: None,
            retry_not_before_ms: Some(retry_not_before_ms),
            finished_at_ms: None,
            output: None,
        })
        .await
        .expect("retry deadline is stored with the queued transition");
    assert_eq!(queued.retry_not_before_ms, Some(retry_not_before_ms));
    let page = store
        .list_encoded(TaskQuery {
            states: vec![TaskStateKind::Queued],
            after: None,
            limit: 256,
            ..TaskQuery::default()
        })
        .await
        .expect("queued retry candidate can be queried");
    let persisted = page
        .records
        .iter()
        .find(|summary| summary.id == retry_id)
        .expect("retry candidate appears in queue query");
    assert_eq!(persisted.retry_not_before_ms, Some(retry_not_before_ms));
    let restarted = store
        .start_encoded(StartCommand {
            id: retry_id,
            expected_state_version: queued.state_version,
            started_at_ms: retry_not_before_ms,
        })
        .await
        .expect("retry attempt starts after its deadline");
    assert_eq!(restarted.attempt, 2);
    assert_eq!(restarted.retry_not_before_ms, None);
}

/// Checks bounded terminal pruning, cutoff equality, bookkeeping, and key
/// reuse.
pub async fn check_terminal_prune_contract(store: &dyn TaskStore) {
    let terminal = [
        (201, TaskState::Succeeded, 100),
        (
            202,
            TaskState::Failed {
                category: "contract".into(),
                message: "failed".into(),
            },
            100,
        ),
        (
            203,
            TaskState::Panicked {
                message: "panicked".into(),
            },
            101,
        ),
        (204, TaskState::Cancelled, 102),
    ];

    for (raw_id, state, finished_at_ms) in terminal {
        let id = TaskId::from_id(qubit_id::Id::new(raw_id));
        let key = (raw_id == 201).then_some("reusable-prune-key");
        let accepted = store
            .accept_encoded(id, request(key, &[raw_id as u8]))
            .await
            .expect("terminal prune candidate is accepted");
        let running = store
            .start_encoded(StartCommand {
                id,
                expected_state_version: accepted.summary.state_version,
                started_at_ms: accepted.summary.accepted_at_ms,
            })
            .await
            .expect("terminal prune candidate starts");
        store
            .transition_encoded(TransitionCommand {
                id,
                expected_state_version: running.state_version,
                expected_attempt: running.attempt,
                retry_not_before_ms: None,
                state,
                output: None,
                cancel_requested: false,
                cancel_error: None,
                finished_at_ms: Some(finished_at_ms),
            })
            .await
            .expect("terminal prune candidate reaches its state");
    }

    let blocked_id = TaskId::from_id(qubit_id::Id::new(205));
    let blocked = store
        .accept_encoded(blocked_id, request(None, &[5]))
        .await
        .expect("blocked candidate is accepted");
    store
        .transition_encoded(TransitionCommand {
            id: blocked_id,
            expected_state_version: blocked.summary.state_version,
            expected_attempt: 0,
            retry_not_before_ms: None,
            state: TaskState::Blocked {
                reason: "manual review".into(),
            },
            output: None,
            cancel_requested: false,
            cancel_error: None,
            finished_at_ms: Some(1),
        })
        .await
        .expect("blocked candidate reaches its state");
    let queued_id = TaskId::from_id(qubit_id::Id::new(206));
    store
        .accept_encoded(queued_id, request(None, &[6]))
        .await
        .expect("queued candidate is accepted");

    let one = NonZeroUsize::new(1).expect("non-zero prune batch");
    assert_eq!(
        store
            .prune_terminal_before(102, one)
            .await
            .expect("first prune succeeds"),
        1
    );
    assert!(
        store
            .get_encoded_task(TaskId::from_id(qubit_id::Id::new(201)))
            .await
            .expect("pruned record can be read")
            .is_none()
    );
    for id in [202, 203, 204, 205, 206] {
        assert!(
            store
                .get_encoded_task(TaskId::from_id(qubit_id::Id::new(id)))
                .await
                .expect("retained record can be read")
                .is_some()
        );
    }
    assert_eq!(
        store
            .prune_terminal_before(102, one)
            .await
            .expect("second prune succeeds"),
        1
    );
    assert!(
        store
            .get_encoded_task(TaskId::from_id(qubit_id::Id::new(202)))
            .await
            .expect("second pruned record can be read")
            .is_none()
    );
    assert_eq!(
        store
            .prune_terminal_before(102, one)
            .await
            .expect("third prune succeeds"),
        1
    );
    assert_eq!(
        store
            .prune_terminal_before(102, one)
            .await
            .expect("empty prune succeeds"),
        0
    );

    for id in [204, 205, 206] {
        assert!(
            store
                .get_encoded_task(TaskId::from_id(qubit_id::Id::new(id)))
                .await
                .expect("boundary or non-terminal record can be read")
                .is_some()
        );
    }
    let reused = store
        .accept_encoded(
            TaskId::from_id(qubit_id::Id::new(207)),
            request(Some("reusable-prune-key"), &[7]),
        )
        .await
        .expect("pruned idempotency key can be reused");
    assert!(reused.created);
    assert_eq!(reused.summary.id, TaskId::from_id(qubit_id::Id::new(207)));
}

/// Confirms typed terminal pruning releases the memory-store payload budget.
pub async fn check_memory_prune_reclaims_payload(store: &dyn TaskStore) {
    let id = TaskId::from_id(qubit_id::Id::new(301));
    let accepted = store
        .accept_encoded(id, request(Some("capacity-prune-key"), &[1, 2, 3, 4]))
        .await
        .expect("payload fills the memory budget");
    let running = store
        .start_encoded(StartCommand {
            id,
            expected_state_version: accepted.summary.state_version,
            started_at_ms: accepted.summary.accepted_at_ms,
        })
        .await
        .expect("capacity test task starts");
    store
        .transition_encoded(TransitionCommand {
            id,
            expected_state_version: running.state_version,
            expected_attempt: running.attempt,
            retry_not_before_ms: None,
            state: TaskState::Succeeded,
            output: None,
            cancel_requested: false,
            cancel_error: None,
            finished_at_ms: Some(10),
        })
        .await
        .expect("capacity test task completes");
    assert_eq!(
        store
            .prune_terminal_before(11, NonZeroUsize::new(1).expect("non-zero prune batch"))
            .await
            .expect("capacity test task is pruned"),
        1
    );
    let replacement = store
        .accept_encoded(
            TaskId::from_id(qubit_id::Id::new(302)),
            request(Some("capacity-prune-key"), &[5, 6, 7, 8]),
        )
        .await
        .expect("pruning releases payload budget and idempotency key");
    assert!(replacement.created);
}
