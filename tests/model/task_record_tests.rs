use qubit_task::model::MAX_TASK_QUERY_LIMIT;
use qubit_task::model::TaskId;
use qubit_task::model::TaskRecord;
use qubit_task::model::TaskRequest;
use qubit_task::model::TaskState;

/// Exposes the common public history query ceiling.
#[test]
fn test_task_query_limit_constant() {
    assert_eq!(MAX_TASK_QUERY_LIMIT, 256);
}

#[test]
fn test_task_summary_copies_lifecycle_without_payload() {
    let request = TaskRequest::new("large", "v1", vec![0x5a; 16 * 1024 * 1024]);
    let record = TaskRecord {
        id: TaskId::generate(),
        request,
        state: TaskState::Blocked {
            reason: "operator".into(),
        },
        state_version: 7,
        attempt: 3,
        retry_not_before_ms: None,
        accepted_at_ms: 11,
        started_at_ms: Some(12),
        finished_at_ms: None,
        assigned_resources: vec!["cpu-0".into()],
        output: None,
        cancel_requested: false,
    };
    let summary = record.summary();
    assert_eq!(summary.id, record.id);
    assert_eq!(summary.state, record.state);
    assert_eq!(summary.state_version, record.state_version);
    assert_eq!(summary.attempt, record.attempt);
    assert_eq!(summary.accepted_at_ms, record.accepted_at_ms);
    assert_eq!(summary.request.task_type, record.request.task_type);
    assert_eq!(summary.request.resources, record.request.resources);
    assert_eq!(summary.request.correlation_key, record.request.correlation_key);
}
