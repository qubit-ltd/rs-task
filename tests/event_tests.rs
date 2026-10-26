// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use qubit_id::Id;
use qubit_metadata::Metadata;
use qubit_task::event::TaskEvent;
use qubit_task::model::TaskState;
use qubit_task::model::typed::ResourceRequest;
use qubit_task::model::typed::TaskId;
use qubit_task::model::typed::TaskSummary;
use serde_json::from_slice;
use serde_json::to_vec;

#[test]
fn test_typed_task_event_round_trips_with_schema_version() {
    let event = TaskEvent {
        schema_version: 1,
        task_id: TaskId::from_id(Id::new(42)),
        state_version: 3,
        state: TaskState::Succeeded,
        correlation_key: Some("batch-a".into()),
    };
    let encoded = to_vec(&event).unwrap();
    let decoded: TaskEvent = from_slice(&encoded).unwrap();
    assert_eq!(decoded.schema_version, 1);
    assert_eq!(decoded.task_id, event.task_id);
    assert_eq!(decoded.state_version, 3);
    assert_eq!(decoded.correlation_key.as_deref(), Some("batch-a"));
}

#[test]
fn test_typed_task_event_captures_committed_summary_fields() {
    let summary = TaskSummary {
        id: TaskId::from_id(Id::new(43)),
        kind_id: "invoice.generate".into(),
        category: Some("billing".into()),
        payload_type_id: "example.Invoice".into(),
        payload_schema_version: 2,
        payload_codec_id: "example.json".into(),
        metadata: Metadata::default(),
        resource_limit: ResourceRequest::default(),
        correlation_key: Some("trace-2".into()),
        idempotency_key: None,
        state: TaskState::Queued,
        cancel_requested: false,
        cancel_error: None,
        state_version: 5,
        attempt: 1,
        retry_not_before_ms: None,
        accepted_at_ms: 10,
        started_at_ms: None,
        finished_at_ms: None,
        progress: None,
        output: None,
    };

    let event = TaskEvent::from(&summary);

    assert_eq!(event.schema_version, 1);
    assert_eq!(event.task_id, summary.id);
    assert_eq!(event.state_version, 5);
    assert_eq!(event.state, TaskState::Queued);
    assert_eq!(event.correlation_key.as_deref(), Some("trace-2"));
}
