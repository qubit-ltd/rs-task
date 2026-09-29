// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Demonstrates exact idempotent replay and rejects different requests under
//! the same key, even while the waiting queue is full.

use std::num::NonZeroUsize;
use std::sync::Arc;

use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::handler::TaskContext;
use qubit_task::handler::TaskHandler;
use qubit_task::handler::TaskHandlerDescriptor;
use qubit_task::handler::TaskRunOutcome;
use qubit_task::handler::TaskRunResult;
use qubit_task::model::TaskOutput;
use qubit_task::model::TaskRequest;
use qubit_task::service::TaskServiceError;
use qubit_task::store::StoreError;
use qubit_task::store::TaskFuture;
use tokio::sync::Notify;
use tokio::sync::Semaphore;

struct GateHandler {
    entered: Arc<Notify>,
    permits: Arc<Semaphore>,
}

impl TaskHandler for GateHandler {
    fn descriptor(&self) -> TaskHandlerDescriptor {
        TaskHandlerDescriptor {
            task_type: "csv-import".into(),
            version: "1".into(),
        }
    }

    fn run<'a>(&'a self, _payload: &'a [u8], _context: TaskContext) -> TaskFuture<'a, TaskRunResult> {
        let entered = Arc::clone(&self.entered);
        let permits = Arc::clone(&self.permits);
        Box::pin(async move {
            entered.notify_one();
            let permit = permits.acquire().await.expect("gate stays open");
            permit.forget();
            Ok(TaskRunOutcome::Succeeded(TaskOutput::default()))
        })
    }
}

fn import_request(payload: &[u8], key: &str, tenant: &str) -> TaskRequest {
    let mut request = TaskRequest::new("csv-import", "1", payload.to_vec()).with_idempotency_key(key);
    request.correlation_key = Some(tenant.into());
    request
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let entered = Arc::new(Notify::new());
    let permits = Arc::new(Semaphore::new(0));
    let handler = GateHandler {
        entered: Arc::clone(&entered),
        permits: Arc::clone(&permits),
    };
    let service = TaskExecutionServiceBuilder::in_memory()
        .queue_capacity(1)
        .max_running_tasks(NonZeroUsize::MIN)
        .register_handler(Arc::new(handler))?
        .build()
        .await?;

    let request = import_request(br#"{"object":"imports/42.csv"}"#, "request-42", "tenant-7");
    let first = service.submit(request.clone()).await?;
    tokio::time::timeout(std::time::Duration::from_secs(1), entered.notified()).await?;

    let waiting = service
        .submit(import_request(br#"{"object":"imports/43.csv"}"#, "request-43", "tenant-7"))
        .await?;
    let full = service
        .submit(import_request(br#"{"object":"imports/44.csv"}"#, "request-44", "tenant-7"))
        .await;
    assert!(matches!(full, Err(TaskServiceError::QueueFull)));

    // Exact replays are checked before queue capacity and return the same ID.
    let replay = service.submit(request.clone()).await?;
    assert_eq!(replay.id, first.id);

    let payload_conflict = service
        .submit(import_request(br#"{"object":"imports/45.csv"}"#, "request-42", "tenant-7"))
        .await;
    assert!(matches!(payload_conflict, Err(TaskServiceError::Store(StoreError::IdempotencyConflict))));
    let correlation_conflict = service
        .submit(import_request(br#"{"object":"imports/42.csv"}"#, "request-42", "tenant-8"))
        .await;
    assert!(matches!(correlation_conflict, Err(TaskServiceError::Store(StoreError::IdempotencyConflict))));

    permits.add_permits(2);
    service.wait(first.id).await?;
    service.wait(waiting.id).await?;
    service.shutdown().await?;
    Ok(())
}
