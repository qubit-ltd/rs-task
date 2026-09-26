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
use qubit_task::model::TaskQuery;
use qubit_task::model::TaskRequest;
use qubit_task::model::TaskRunError;
use qubit_task::model::TaskState;
use qubit_task::service::LocalTaskOutcome;
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

    let result = service.submit(test_keyed(request)).await;

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

    let result = service.submit(test_keyed(request)).await;

    assert!(matches!(
        result,
        Err(TaskServiceError::InvalidRequest(message))
            if message == "task type and handler version must not be empty"
    ));
    service.shutdown().await.expect("service shuts down");
}

#[tokio::test]
async fn test_submit_local_rejects_unsatisfiable_cpu_capacity_before_acceptance() {
    let service = TaskExecutionServiceBuilder::in_memory()
        .capacity(ResourceCapacity {
            cpu_slots: 0,
            ..ResourceCapacity::default()
        })
        .build()
        .await
        .expect("service builds");

    let result = service
        .submit_local(|_| LocalTaskOutcome::<(), std::io::Error>::Succeeded {
            value: (),
            summary: TaskOutput::default(),
        })
        .await;

    assert!(matches!(result, Err(TaskServiceError::Unsatisfiable)));
    assert_eq!(service.stats().await.expect("stats succeed").queued, 0);
    assert!(
        service
            .list(TaskQuery::default())
            .await
            .expect("history query succeeds")
            .records
            .is_empty()
    );
    service.shutdown().await.expect("service shuts down");
}

#[tokio::test]
async fn test_submit_rejects_payload_above_maximum_size() {
    let service = create_service().await;
    let mut request = valid_request();
    request.payload = vec![0; MAX_TASK_PAYLOAD_BYTES + 1];

    let result = service.submit(test_keyed(request)).await;

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

    let result = service.submit(test_keyed(request)).await;

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

    let result = service.submit(test_keyed(request)).await;

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

    let result = service.submit(test_keyed(request)).await;

    assert!(matches!(result, Err(TaskServiceError::Unsatisfiable)));
    service.shutdown().await.expect("service shuts down");
}

#[tokio::test]
async fn test_submit_accepts_valid_request() {
    let service = create_service().await;
    let record = service
        .submit(test_keyed(valid_request()))
        .await
        .expect("valid request is accepted");

    assert_eq!(record.request.task_type, "validation");
    assert_eq!(record.request.handler_version, "1");
    assert_eq!(record.request.payload, b"payload");
    service.shutdown().await.expect("service shuts down");
}

struct OversizedDiagnosticHandler;

impl TaskHandler for OversizedDiagnosticHandler {
    fn descriptor(&self) -> TaskHandlerDescriptor {
        TaskHandlerDescriptor {
            task_type: "oversized-diagnostic".into(),
            version: "1".into(),
        }
    }

    fn run<'a>(&'a self, _: &'a [u8], _: TaskContext) -> TaskFuture<'a, qubit_task::handler::TaskRunResult> {
        Box::pin(async {
            Err(TaskRunError {
                category: "c".repeat(129),
                message: "界".repeat(2_000),
                retryable: false,
            })
        })
    }
}

/// Creates a request whose only limit violation is its metadata key length.
fn request_with_oversized_metadata_key() -> TaskRequest {
    let mut request = TaskRequest::new("metadata-limit", "1", Vec::new());
    request.metadata.insert("k".repeat(129), "value".into());
    request
}

#[tokio::test]
async fn test_submit_rejects_oversized_metadata_before_accepting() {
    let service = TaskExecutionServiceBuilder::in_memory()
        .build()
        .await
        .expect("service builds");

    assert!(matches!(
        service.submit(test_keyed(request_with_oversized_metadata_key())).await,
        Err(TaskServiceError::InvalidRequest(_))
    ));
    assert_eq!(service.stats().await.expect("stats are available").queued, 0);
    service.shutdown().await.expect("service shuts down");
}

#[tokio::test]
async fn test_terminal_diagnostics_are_bounded_on_a_utf8_boundary() {
    let service = TaskExecutionServiceBuilder::in_memory()
        .register_handler(Arc::new(OversizedDiagnosticHandler))
        .expect("handler registers")
        .build()
        .await
        .expect("service builds");
    let accepted = service
        .submit(test_keyed(TaskRequest::new("oversized-diagnostic", "1", Vec::new())))
        .await
        .expect("task is accepted");
    let record = service.wait(accepted.id).await.expect("task reaches terminal state");

    let TaskState::Failed { category, message } = record.state else {
        panic!("oversized handler result is a failed task");
    };
    assert_eq!(category.len(), 128);
    assert_eq!(message.len(), 4_095);
    assert!(message.chars().all(|character| character == '界'));
    assert!(record.output.is_none());
    service.shutdown().await.expect("service shuts down");
}

#[tokio::test]
async fn test_exact_request_limits_are_accepted() {
    let service = TaskExecutionServiceBuilder::in_memory()
        .queue_capacity(16)
        .build()
        .await
        .expect("service builds");
    let mut request = TaskRequest::new("x".repeat(128), "v".repeat(64), Vec::new());
    request.correlation_key = Some("c".repeat(256));
    request.idempotency_key = Some("i".repeat(256));
    for index in 0..32 {
        request.metadata.insert(format!("k{index:03}"), "v".repeat(508));
    }
    assert_eq!(
        request
            .metadata
            .iter()
            .map(|(key, value)| key.len() + value.len())
            .sum::<usize>(),
        16_384
    );
    let accepted = service
        .submit(test_keyed(request))
        .await
        .expect("boundary request is accepted");
    assert_eq!(accepted.state, TaskState::Queued);
    assert!(service.wait(accepted.id).await.is_err());

    let mut metadata_boundary = TaskRequest::new("metadata", "1", Vec::new());
    metadata_boundary.metadata.insert("k".repeat(128), "v".repeat(4_096));
    let accepted = service
        .submit(test_keyed(metadata_boundary))
        .await
        .expect("metadata entry boundaries are accepted");
    assert!(service.wait(accepted.id).await.is_err());
    service.shutdown().await.expect("service shuts down");
}

#[tokio::test]
async fn test_each_request_limit_is_enforced_before_acceptance() {
    let service = TaskExecutionServiceBuilder::in_memory()
        .build()
        .await
        .expect("service builds");
    let mut requests = Vec::new();
    requests.push(TaskRequest::new("x".repeat(129), "1", Vec::new()));
    requests.push(TaskRequest::new("界".repeat(43), "1", Vec::new()));
    requests.push(TaskRequest::new("x", "v".repeat(65), Vec::new()));
    let mut request = TaskRequest::new("x", "1", Vec::new());
    request.correlation_key = Some("c".repeat(257));
    requests.push(request);
    let mut request = TaskRequest::new("x", "1", Vec::new());
    request.idempotency_key = Some("i".repeat(257));
    requests.push(request);
    let mut request = TaskRequest::new("x", "1", Vec::new());
    request.metadata.insert("k".repeat(129), "v".into());
    requests.push(request);
    let mut request = TaskRequest::new("x", "1", Vec::new());
    request.metadata.insert("k".into(), "v".repeat(4_097));
    requests.push(request);
    let mut request = TaskRequest::new("x", "1", Vec::new());
    for index in 0..33 {
        request.metadata.insert(format!("k{index}"), "v".into());
    }
    requests.push(request);
    let mut request = TaskRequest::new("x", "1", Vec::new());
    for index in 0..32 {
        request.metadata.insert(format!("k{index:02}"), "v".repeat(512));
    }
    requests.push(request);

    for request in requests {
        assert!(matches!(
            service.submit(test_keyed(request)).await,
            Err(TaskServiceError::InvalidRequest(_))
        ));
    }
    assert_eq!(service.stats().await.expect("stats are available").queued, 0);
    service.shutdown().await.expect("service shuts down");
}

#[allow(dead_code)]
fn test_keyed(mut request: qubit_task::model::TaskRequest) -> qubit_task::model::TaskRequest {
    static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(1);
    if request.idempotency_key.is_none() {
        request.idempotency_key = Some(format!(
            "test-request-{}",
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
    }
    request
}
