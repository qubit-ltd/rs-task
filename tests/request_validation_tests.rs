// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;

use qubit_task::TaskExecutionService;
use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::handler::TaskContext;
use qubit_task::handler::TaskHandler;
use qubit_task::handler::TaskHandlerDescriptor;
use qubit_task::handler::TaskRunOutcome;
use qubit_task::model::MAX_TASK_PAYLOAD_BYTES;
use qubit_task::model::ResourceCapacity;
use qubit_task::model::ResourceRequest;
use qubit_task::model::TaskOutput;
use qubit_task::model::TaskRequest;
use qubit_task::service::TaskServiceError;
use qubit_task::store::TaskFuture;

struct ValidationHandler;

impl TaskHandler for ValidationHandler {
    fn descriptor(&self) -> TaskHandlerDescriptor {
        TaskHandlerDescriptor {
            task_type: "validation".into(),
            version: "1".into(),
        }
    }

    fn run<'a>(
        &'a self,
        _payload: &'a [u8],
        _context: TaskContext,
    ) -> TaskFuture<'a, qubit_task::handler::TaskRunResult> {
        Box::pin(async { Ok(TaskRunOutcome::Succeeded(TaskOutput::default())) })
    }
}

async fn create_service() -> TaskExecutionService {
    TaskExecutionServiceBuilder::in_memory()
        .capacity(ResourceCapacity {
            cpu_slots: 2,
            ..ResourceCapacity::default()
        })
        .register_handler(Arc::new(ValidationHandler))
        .expect("validation handler registration succeeds")
        .build()
        .await
        .expect("validation service builds")
}

fn valid_request() -> TaskRequest {
    TaskRequest::new("validation", "1", b"payload".to_vec())
}

#[tokio::test]
async fn test_submit_rejects_empty_task_type() {
    let service = create_service().await;
    let mut request = valid_request();
    request.task_type.clear();

    let result = service.submit(request).await;

    assert!(matches!(
        result,
        Err(TaskServiceError::InvalidRequest(message))
            if message == "task type and handler version must not be empty"
    ));
    service.shutdown().await.expect("service shuts down");
}

#[tokio::test]
async fn test_submit_rejects_empty_handler_version() {
    let service = create_service().await;
    let mut request = valid_request();
    request.handler_version.clear();

    let result = service.submit(request).await;

    assert!(matches!(
        result,
        Err(TaskServiceError::InvalidRequest(message))
            if message == "task type and handler version must not be empty"
    ));
    service.shutdown().await.expect("service shuts down");
}

#[tokio::test]
async fn test_submit_rejects_payload_above_maximum_size() {
    let service = create_service().await;
    let mut request = valid_request();
    request.payload = vec![0; MAX_TASK_PAYLOAD_BYTES + 1];

    let result = service.submit(request).await;

    assert!(matches!(
        result,
        Err(TaskServiceError::InvalidRequest(message))
            if message == "payload exceeds the 16 MiB limit"
    ));
    service.shutdown().await.expect("service shuts down");
}

#[tokio::test]
async fn test_submit_rejects_empty_custom_resource_name() {
    let service = create_service().await;
    let mut request = valid_request();
    request.resources.custom.insert(String::new(), 1);

    let result = service.submit(request).await;

    assert!(matches!(
        result,
        Err(TaskServiceError::InvalidRequest(message))
            if message == "resource names and GPU labels must not be empty"
    ));
    service.shutdown().await.expect("service shuts down");
}

#[tokio::test]
async fn test_submit_rejects_empty_gpu_label() {
    let service = create_service().await;
    let mut request = valid_request();
    request.resources.gpu_labels.push(String::new());

    let result = service.submit(request).await;

    assert!(matches!(
        result,
        Err(TaskServiceError::InvalidRequest(message))
            if message == "resource names and GPU labels must not be empty"
    ));
    service.shutdown().await.expect("service shuts down");
}

#[tokio::test]
async fn test_submit_rejects_cpu_request_above_capacity() {
    let service = create_service().await;
    let mut request = valid_request();
    request.resources = ResourceRequest {
        cpu_slots: 3,
        ..ResourceRequest::default()
    };

    let result = service.submit(request).await;

    assert!(matches!(result, Err(TaskServiceError::Unsatisfiable)));
    service.shutdown().await.expect("service shuts down");
}

#[tokio::test]
async fn test_submit_accepts_valid_request() {
    let service = create_service().await;
    let record = service
        .submit(valid_request())
        .await
        .expect("valid request is accepted");

    assert_eq!(record.request.task_type, "validation");
    assert_eq!(record.request.handler_version, "1");
    assert_eq!(record.request.payload, b"payload");
    service.shutdown().await.expect("service shuts down");
}
