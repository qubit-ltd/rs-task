// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use qubit_task::model::RequestValidationField;
use qubit_task::model::RequestValidationRule;
use qubit_task::model::ResourceRequest;

#[test]
fn test_resource_descriptions_enforce_bounds_and_gpu_consistency() {
    let mut request = ResourceRequest {
        gpu_count: 1,
        gpu_labels: vec!["label".into()],
        ..ResourceRequest::default()
    };
    assert!(request.validate_limits().is_ok());
    request.gpu_labels = vec!["duplicate".into(), "duplicate".into()];
    let error = request.validate_limits().expect_err("duplicate labels are invalid");
    assert_eq!(error.field, RequestValidationField::GpuLabels);
    assert_eq!(error.rule, RequestValidationRule::Unique);
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
