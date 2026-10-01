// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use crate::model::MAX_TASK_QUERY_LIMIT;
use crate::model::TaskId;
use crate::model::TaskOutput;
use crate::model::TaskRecord;
use crate::model::TaskRequest;
use crate::model::TaskState;

/// Exposes the common public history query ceiling.
#[test]
fn test_task_query_limit_constant() {
    assert_eq!(MAX_TASK_QUERY_LIMIT, 256);
}

#[test]
fn test_task_summary_copies_lifecycle_without_payload() {
    let mut request = TaskRequest::new("summary", "v1", b"payload".to_vec());
    request.correlation_key = Some("trace-1".into());
    request.idempotency_key = Some("request-1".into());
    let record = TaskRecord {
        id: TaskId::generate(),
        request,
        state: TaskState::Blocked {
            reason: "operator".into(),
        },
        state_version: 7,
        attempt: 3,
        retry_not_before_ms: Some(13),
        accepted_at_ms: 11,
        started_at_ms: Some(12),
        finished_at_ms: Some(14),
        assigned_resources: vec!["cpu-0".into()],
        output: Some(TaskOutput {
            summary: b"done".to_vec(),
        }),
        cancel_requested: true,
    };
    let summary = record.summary();
    assert_eq!(summary.id, record.id);
    assert_eq!(summary.state, record.state);
    assert_eq!(summary.state_version, record.state_version);
    assert_eq!(summary.attempt, record.attempt);
    assert_eq!(summary.retry_not_before_ms, record.retry_not_before_ms);
    assert_eq!(summary.accepted_at_ms, record.accepted_at_ms);
    assert_eq!(summary.started_at_ms, record.started_at_ms);
    assert_eq!(summary.finished_at_ms, record.finished_at_ms);
    assert_eq!(summary.assigned_resources, record.assigned_resources);
    assert_eq!(summary.output, record.output);
    assert_eq!(summary.cancel_requested, record.cancel_requested);
    assert_eq!(summary.request.task_type, record.request.task_type);
    assert_eq!(summary.request.handler_version, record.request.handler_version);
    assert_eq!(summary.request.resources, record.request.resources);
    assert_eq!(summary.request.correlation_key, record.request.correlation_key);
    assert_eq!(summary.request.idempotency_key, record.request.idempotency_key);
}
