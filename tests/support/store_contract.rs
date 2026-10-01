use qubit_task::model::ResourceRequest;
use qubit_task::model::StartCommand;
use qubit_task::model::StoredPayload;
use qubit_task::model::StoredTaskRequest;
use qubit_task::model::TaskId;
use qubit_task::model::TaskState;
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
                state: TaskState::Cancelled,
                output: None,
                cancel_requested: false,
                cancel_error: None,
                finished_at_ms: Some(running.accepted_at_ms),
            })
            .await,
        Err(StoreError::Conflict)
    ));
}
