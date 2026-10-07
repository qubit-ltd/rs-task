// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::collections::BTreeMap;

use super::legacy::RequestValidationError;
use super::legacy::RequestValidationField;
use super::legacy::RequestValidationRule;
use super::legacy::ResourceRequest;
use super::legacy::TaskRequest;

fn assert_error(
    result: Result<(), RequestValidationError>,
    field: RequestValidationField,
    rule: RequestValidationRule,
    expected_message: &str,
) {
    let error = result.expect_err("invalid request should be rejected");
    assert_eq!(error.field, field);
    assert_eq!(error.rule, rule);
    assert_eq!(error.message(), expected_message);
    assert_eq!(error.to_string(), expected_message);
    assert!(std::error::Error::source(&error).is_none());
}

#[test]
fn resource_validation_reports_each_entry_and_gpu_constraint() {
    let mut request = ResourceRequest::default();
    request.gpu_labels = vec![String::new()];
    assert_error(
        request.validate_limits(),
        RequestValidationField::GpuLabels,
        RequestValidationRule::Required,
        "GPU labels must not be empty",
    );

    request.gpu_labels = vec!["gpu".repeat(43)];
    assert_error(
        request.validate_limits(),
        RequestValidationField::GpuLabels,
        RequestValidationRule::MaxBytes,
        "GPU label exceeds the 128-byte limit",
    );

    request.gpu_labels = vec!["gpu".into(), "gpu".into()];
    assert_error(
        request.validate_limits(),
        RequestValidationField::GpuLabels,
        RequestValidationRule::Unique,
        "GPU labels must be unique",
    );

    request.gpu_labels = vec!["nvidia".into()];
    assert_error(
        request.validate_limits(),
        RequestValidationField::GpuLabels,
        RequestValidationRule::RequiresGpu,
        "GPU labels require at least one requested GPU",
    );

    request.gpu_count = 1;
    request.gpu_labels = vec!["gpu".into(); super::resource_request::MAX_RESOURCE_NAME_ENTRIES + 1];
    assert_error(
        request.validate_limits(),
        RequestValidationField::GpuLabels,
        RequestValidationRule::MaxEntries,
        "GPU labels exceed the 32-entry limit",
    );
}

#[test]
fn resource_validation_reports_custom_name_constraints() {
    let mut request = ResourceRequest::default();
    request.custom.insert(String::new(), 1);
    assert_error(
        request.validate_limits(),
        RequestValidationField::CustomResources,
        RequestValidationRule::Required,
        "custom resource names must not be empty",
    );

    request.custom.clear();
    request.custom.insert("license".repeat(19), 1);
    assert_error(
        request.validate_limits(),
        RequestValidationField::CustomResources,
        RequestValidationRule::MaxBytes,
        "custom resource name exceeds the 128-byte limit",
    );

    request.custom.clear();
    for index in 0..=super::resource_request::MAX_RESOURCE_NAME_ENTRIES {
        request.custom.insert(format!("resource-{index}"), 1);
    }
    assert_error(
        request.validate_limits(),
        RequestValidationField::CustomResources,
        RequestValidationRule::MaxEntries,
        "custom resources exceed the 32-entry limit",
    );
}

#[test]
fn task_validation_reports_required_identifiers_and_byte_limits() {
    let mut request = TaskRequest::new("", "1", Vec::new());
    assert_error(
        request.validate_limits(),
        RequestValidationField::TaskType,
        RequestValidationRule::Required,
        "task type and handler version must not be empty",
    );

    request.task_type = "resize".into();
    request.handler_version.clear();
    assert_error(
        request.validate_limits(),
        RequestValidationField::HandlerVersion,
        RequestValidationRule::Required,
        "task type and handler version must not be empty",
    );

    request = TaskRequest::new(
        "x".repeat(super::task_request::MAX_TASK_TYPE_BYTES + 1),
        "1",
        Vec::new(),
    );
    assert_error(
        request.validate_limits(),
        RequestValidationField::TaskType,
        RequestValidationRule::MaxBytes,
        "task type exceeds the 128-byte limit",
    );

    request = TaskRequest::new(
        "resize",
        "v".repeat(super::task_request::MAX_HANDLER_VERSION_BYTES + 1),
        Vec::new(),
    );
    assert_error(
        request.validate_limits(),
        RequestValidationField::HandlerVersion,
        RequestValidationRule::MaxBytes,
        "handler version exceeds the 64-byte limit",
    );

    request = TaskRequest::new("resize", "1", vec![0; super::MAX_TASK_PAYLOAD_BYTES + 1]);
    assert_error(
        request.validate_limits(),
        RequestValidationField::Payload,
        RequestValidationRule::MaxBytes,
        "payload exceeds the 16 MiB limit",
    );
}

#[test]
fn task_validation_reports_optional_key_and_metadata_limits() {
    let mut request = TaskRequest::new("resize", "1", Vec::new());
    request.correlation_key = Some("c".repeat(super::task_request::MAX_CORRELATION_KEY_BYTES + 1));
    assert_error(
        request.validate_limits(),
        RequestValidationField::CorrelationKey,
        RequestValidationRule::MaxBytes,
        "correlation key exceeds the 256-byte limit",
    );

    request.correlation_key = None;
    request.idempotency_key = Some("i".repeat(super::task_request::MAX_IDEMPOTENCY_KEY_BYTES + 1));
    assert_error(
        request.validate_limits(),
        RequestValidationField::IdempotencyKey,
        RequestValidationRule::MaxBytes,
        "idempotency key exceeds the 256-byte limit",
    );

    request.idempotency_key = None;
    request.metadata = (0..=super::task_request::MAX_TASK_METADATA_ENTRIES)
        .map(|index| (format!("key-{index}"), "value".to_owned()))
        .collect::<BTreeMap<_, _>>();
    assert_error(
        request.validate_limits(),
        RequestValidationField::Metadata,
        RequestValidationRule::MaxEntries,
        "metadata exceeds the 32-entry limit",
    );

    request.metadata.clear();
    request.metadata.insert(
        "k".repeat(super::task_request::MAX_TASK_METADATA_KEY_BYTES + 1),
        "v".into(),
    );
    assert_error(
        request.validate_limits(),
        RequestValidationField::Metadata,
        RequestValidationRule::MaxBytes,
        "metadata key exceeds the 128-byte limit",
    );

    request.metadata.clear();
    request.metadata.insert(
        "key".into(),
        "v".repeat(super::task_request::MAX_TASK_METADATA_VALUE_BYTES + 1),
    );
    assert_error(
        request.validate_limits(),
        RequestValidationField::Metadata,
        RequestValidationRule::MaxBytes,
        "metadata value exceeds the 4096-byte limit",
    );

    request.metadata.clear();
    for index in 0..super::task_request::MAX_TASK_METADATA_ENTRIES {
        request.metadata.insert(format!("key-{index:02}"), "v".repeat(512));
    }
    assert_error(
        request.validate_limits(),
        RequestValidationField::Metadata,
        RequestValidationRule::MaxBytes,
        "metadata exceeds the 16384-byte limit",
    );
}
