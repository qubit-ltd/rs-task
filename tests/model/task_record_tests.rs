use qubit_task::model::MAX_TASK_QUERY_LIMIT;

/// Exposes the common public history query ceiling.
#[test]
fn test_task_query_limit_constant() {
    assert_eq!(MAX_TASK_QUERY_LIMIT, 256);
}
