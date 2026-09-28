// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use qubit_task::model::AcceptOutcome;
use qubit_task::model::TaskId;
use qubit_task::model::TaskRequest;
use qubit_task::model::TaskState;
use qubit_task::model::TransitionCommand;
use qubit_task::store::StoreError;
use qubit_task::store::TaskStore;

/// Checks idempotency, revision, and summary-read behavior common to stores.
pub async fn check_core_contract(store: &dyn TaskStore) {
    let id = TaskId::generate();
    let request = TaskRequest::new("contract", "1", vec![1, 2, 3]).with_idempotency_key("contract-key");
    let accepted = store.accept(id, request.clone()).await.expect("accept succeeds");
    let record = match accepted {
        AcceptOutcome::Accepted(record) => record,
        AcceptOutcome::Existing(_) => panic!("first accept is new"),
    };

    let replay = store
        .accept(TaskId::generate(), request)
        .await
        .expect("identical request replays");
    assert!(matches!(replay, AcceptOutcome::Existing(existing) if existing.id == id));
    assert_eq!(
        store.get_summary(id).await.expect("summary read succeeds").unwrap().id,
        id
    );
    assert_eq!(
        store
            .get_summary_by_idempotency_key("contract-key")
            .await
            .expect("idempotency summary read succeeds")
            .unwrap()
            .id,
        id
    );

    let command = TransitionCommand {
        id,
        expected_version: record.state_version,
        expected_attempt: record.attempt,
        state: TaskState::Running,
        retry_not_before_ms: None,
        output: None,
        assigned_resources: Vec::new(),
        cancel_requested: false,
    };
    let running = store
        .transition(command.clone())
        .await
        .expect("valid transition succeeds");
    assert_eq!(running.state, TaskState::Running);
    assert!(matches!(store.transition(command).await, Err(StoreError::Conflict)));
}
