// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use qubit_id::Id;
use qubit_metadata::Metadata;
use qubit_task::model::TaskState;
use qubit_task::model::typed::ResourceRequest;
use qubit_task::model::typed::TaskCursor;
use qubit_task::model::typed::TaskId;
use qubit_task::model::typed::TaskSummary;

#[test]
fn test_cursor_from_summary_preserves_acceptance_position() {
    let id = TaskId::from_id(Id::new(91));
    let summary = TaskSummary {
        id,
        kind_id: "counter.add".into(),
        category: None,
        payload_type_id: "qubit_task.tests.Counter".into(),
        payload_schema_version: 1,
        payload_codec_id: "qubit_task.tests.u32_le".into(),
        metadata: Metadata::default(),
        resource_limit: ResourceRequest::default(),
        correlation_key: None,
        idempotency_key: None,
        state: TaskState::Queued,
        cancel_requested: false,
        cancel_error: None,
        state_version: 0,
        attempt: 0,
        retry_not_before_ms: None,
        accepted_at_ms: 123,
        started_at_ms: None,
        finished_at_ms: None,
        progress: None,
        output: None,
    };

    let cursor = TaskCursor::from(&summary);

    assert_eq!(cursor.accepted_at_ms, summary.accepted_at_ms);
    assert_eq!(cursor.id, id);
}
