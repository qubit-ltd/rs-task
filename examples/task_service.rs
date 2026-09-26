// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::handler::TaskContext;
use qubit_task::handler::TaskHandler;
use qubit_task::handler::TaskHandlerDescriptor;
use qubit_task::handler::TaskRunOutcome;
use qubit_task::model::TaskOutput;
use qubit_task::model::TaskRequest;
use qubit_task::service::LocalTaskOutcome;
use qubit_task::service::LocalTaskResultError;
use qubit_task::store::TaskFuture;

struct EchoV1;

impl TaskHandler for EchoV1 {
    fn descriptor(&self) -> TaskHandlerDescriptor {
        TaskHandlerDescriptor {
            task_type: "echo".into(),
            version: "1".into(),
        }
    }

    fn run<'a>(
        &'a self,
        payload: &'a [u8],
        _context: TaskContext,
    ) -> TaskFuture<'a, qubit_task::handler::TaskRunResult> {
        Box::pin(async move {
            Ok(TaskRunOutcome::Succeeded(TaskOutput {
                summary: format!("echoed {} bytes", payload.len()).into_bytes(),
            }))
        })
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_time().build()?;
    runtime.block_on(async {
        let service = TaskExecutionServiceBuilder::in_memory()
            .runtime_handle(tokio::runtime::Handle::current())
            .register_handler(Arc::new(EchoV1))?
            .build()
            .await?;

        let value_handle = service
            .submit_local(|_| LocalTaskOutcome::<u32, std::io::Error>::Succeeded {
                value: 42_u32,
                summary: TaskOutput {
                    summary: b"answer=42".to_vec(),
                },
            })
            .await?;
        let value = value_handle.result().await??;
        assert_eq!(value, 42);

        let started = Arc::new(AtomicBool::new(false));
        let handler_started = Arc::clone(&started);
        let cancel_handle = service
            .submit_local(move |context| {
                handler_started.store(true, Ordering::Release);
                while !context.is_cancelled() {
                    std::thread::sleep(Duration::from_millis(1));
                }
                LocalTaskOutcome::<(), std::io::Error>::Cancelled
            })
            .await?;
        while !started.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        service.cancel(cancel_handle.task_id()).await?;
        assert!(matches!(
            cancel_handle.result().await,
            Err(LocalTaskResultError::Cancelled)
        ));

        let request = TaskRequest::new("echo", "1", b"versioned work".to_vec())
            .with_idempotency_key("echo-versioned-work-2026-09-26");
        let accepted = service.submit(request).await?;
        let snapshot = service
            .get_summary(accepted.id)
            .await?
            .expect("accepted task remains queryable");
        assert_eq!(snapshot.request.task_type, "echo");
        let finished = service.wait(accepted.id).await?;
        assert!(matches!(finished.state, qubit_task::model::TaskState::Succeeded));
        assert_eq!(
            finished.output.expect("summary is persisted").summary,
            b"echoed 14 bytes"
        );

        service.shutdown().await?;
        Ok::<(), Box<dyn std::error::Error>>(())
    })
}
