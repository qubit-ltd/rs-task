use qubit_task::model::ResourceRequest;

#[test]
fn resource_descriptions_enforce_bounds_and_gpu_consistency() {
    let mut request = ResourceRequest {
        gpu_count: 1,
        gpu_labels: vec!["label".into()],
        ..ResourceRequest::default()
    };
    assert!(request.validate_limits().is_ok());
    request.gpu_labels = vec!["duplicate".into(), "duplicate".into()];
    assert!(request.validate_limits().is_err());
    request.gpu_labels = vec!["label".into()];

    request.gpu_count = 0;
    assert!(request.validate_limits().is_err());
    request.gpu_count = 1;
    request.gpu_labels = vec!["x".repeat(129)];
    assert!(request.validate_limits().is_err());
    request.gpu_labels = vec![String::new()];
    assert!(request.validate_limits().is_err());
    request.gpu_labels.clear();
    request.custom.insert(String::new(), 1);
    assert!(request.validate_limits().is_err());
    request.custom.clear();
    for index in 0..33 {
        request.custom.insert(format!("resource-{index}"), 1);
    }
    assert!(request.validate_limits().is_err());
}
