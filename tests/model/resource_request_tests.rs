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
fn test_resource_request_validates_bounds_and_gpu_consistency() {
    let mut request = ResourceRequest {
        gpu_count: 1,
        gpu_labels: vec!["label".into()],
        ..ResourceRequest::default()
    };
    request
        .validate_limits()
        .expect("one unique GPU label is valid for one GPU");
    request.gpu_labels = vec!["duplicate".into(), "duplicate".into()];
    let error = request.validate_limits().expect_err("duplicate labels are invalid");
    assert_eq!(error.field, RequestValidationField::GpuLabels);
    assert_eq!(error.rule, RequestValidationRule::Unique);
    request.gpu_labels = vec!["label".into()];

    request.gpu_count = 0;
    let error = request.validate_limits().expect_err("labels require a GPU request");
    assert_eq!(error.field, RequestValidationField::GpuLabels);
    assert_eq!(error.rule, RequestValidationRule::RequiresGpu);
    request.gpu_count = 1;
    request.gpu_labels = vec!["x".repeat(129)];
    let error = request.validate_limits().expect_err("GPU label exceeds the byte limit");
    assert_eq!(error.field, RequestValidationField::GpuLabels);
    assert_eq!(error.rule, RequestValidationRule::MaxBytes);
    request.gpu_labels = vec![String::new()];
    let error = request.validate_limits().expect_err("GPU label cannot be empty");
    assert_eq!(error.field, RequestValidationField::GpuLabels);
    assert_eq!(error.rule, RequestValidationRule::Required);
    request.gpu_labels.clear();
    request.custom.insert(String::new(), 1);
    let error = request
        .validate_limits()
        .expect_err("custom resource name cannot be empty");
    assert_eq!(error.field, RequestValidationField::CustomResources);
    assert_eq!(error.rule, RequestValidationRule::Required);
    request.custom.clear();
    for index in 0..33 {
        request.custom.insert(format!("resource-{index}"), 1);
    }
    let error = request
        .validate_limits()
        .expect_err("custom resource count exceeds the entry limit");
    assert_eq!(error.field, RequestValidationField::CustomResources);
    assert_eq!(error.rule, RequestValidationRule::MaxEntries);
}
