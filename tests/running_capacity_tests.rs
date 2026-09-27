// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::handler::TaskContext;
use qubit_task::handler::TaskHandler;
use qubit_task::handler::TaskHandlerDescriptor;
use qubit_task::handler::TaskRunOutcome;
use qubit_task::handler::TaskRunResult;
use qubit_task::model::TaskOutput;
use qubit_task::model::TaskRequest;
use qubit_task::model::TaskState;
use qubit_task::store::TaskFuture;
use tokio::sync;
use tokio::test as tokio_test;
use tokio::time;

struct HoldingHandler {
    started: sync::mpsc::UnboundedSender<()>,
    release: Arc<sync::Semaphore>,
    active: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
}

impl TaskHandler for HoldingHandler {
    fn descriptor(&self) -> TaskHandlerDescriptor {
        TaskHandlerDescriptor {
            task_type: "hold".into(),
            version: "1".into(),
        }
    }

    fn run<'a>(&'a self, _payload: &'a [u8], _context: TaskContext) -> TaskFuture<'a, TaskRunResult> {
        Box::pin(async move {
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(active, Ordering::SeqCst);
            let _ = self.started.send(());
            let permit = self
                .release
                .clone()
                .acquire_owned()
                .await
                .expect("release semaphore remains open");
            drop(permit);
            self.active.fetch_sub(1, Ordering::SeqCst);
            Ok(TaskRunOutcome::Succeeded(TaskOutput::default()))
        })
    }
}

#[tokio_test]
async fn test_zero_cpu_tasks_obey_independent_running_limit() {
    let (started_tx, mut started_rx) = sync::mpsc::unbounded_channel();
    let release = Arc::new(sync::Semaphore::new(0));
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let handler = Arc::new(HoldingHandler {
        started: started_tx,
        release: release.clone(),
        active: active.clone(),
        peak: peak.clone(),
    });
    let service = TaskExecutionServiceBuilder::in_memory()
        .max_running_tasks(NonZeroUsize::new(2).unwrap())
        .register_handler(handler)
        .unwrap()
        .build()
        .await
        .unwrap();
    let mut ids = Vec::new();
    for _ in 0..4 {
        let mut request = TaskRequest::new("hold", "1", Vec::new());
        request.resources.cpu_slots = 0;
        ids.push(service.submit(test_keyed(request)).await.unwrap().id);
    }
    time::timeout(std::time::Duration::from_secs(2), async {
        started_rx.recv().await.unwrap();
        started_rx.recv().await.unwrap();
    })
    .await
    .unwrap();
    time::sleep(std::time::Duration::from_millis(100)).await;
    assert_eq!(peak.load(Ordering::SeqCst), 2);
    assert_eq!(active.load(Ordering::SeqCst), 2);
    assert_eq!(service.stats().await.unwrap().queued, 2);
    release.add_permits(4);
    for id in ids {
        time::timeout(std::time::Duration::from_secs(2), service.wait(id))
            .await
            .unwrap()
            .unwrap();
    }
    service.shutdown().await.unwrap();
}

#[tokio_test]
async fn test_zero_cpu_tasks_still_obey_max_running_tasks() {
    let (started_tx, mut started_rx) = sync::mpsc::unbounded_channel();
    let release = Arc::new(sync::Semaphore::new(0));
    let handler = Arc::new(HoldingHandler {
        started: started_tx,
        release: release.clone(),
        active: Arc::new(AtomicUsize::new(0)),
        peak: Arc::new(AtomicUsize::new(0)),
    });
    let service = TaskExecutionServiceBuilder::in_memory()
        .max_running_tasks(NonZeroUsize::new(1).unwrap())
        .register_handler(handler)
        .unwrap()
        .build()
        .await
        .unwrap();
    let mut first = TaskRequest::new("hold", "1", Vec::new());
    first.resources.cpu_slots = 0;
    let first = service.submit(test_keyed(first)).await.unwrap();
    let mut second = TaskRequest::new("hold", "1", Vec::new());
    second.resources.cpu_slots = 0;
    let second = service.submit(test_keyed(second)).await.unwrap();

    time::timeout(std::time::Duration::from_secs(2), started_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        service.get(second.id).await.unwrap().unwrap().state,
        TaskState::Queued
    ));
    release.add_permits(1);
    time::timeout(std::time::Duration::from_secs(2), service.wait(first.id))
        .await
        .unwrap()
        .unwrap();
    time::timeout(std::time::Duration::from_secs(2), started_rx.recv())
        .await
        .unwrap()
        .unwrap();
    release.add_permits(1);
    service.wait(second.id).await.unwrap();
    service.shutdown().await.unwrap();
}

struct PanicThenSucceed(AtomicUsize);

impl TaskHandler for PanicThenSucceed {
    fn descriptor(&self) -> TaskHandlerDescriptor {
        TaskHandlerDescriptor {
            task_type: "panic-once".into(),
            version: "1".into(),
        }
    }

    fn run<'a>(&'a self, _payload: &'a [u8], _context: TaskContext) -> TaskFuture<'a, TaskRunResult> {
        Box::pin(async move {
            if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
                panic!("injected panic");
            } else {
                Ok(TaskRunOutcome::Succeeded(TaskOutput::default()))
            }
        })
    }
}

#[tokio_test]
async fn test_running_permit_is_returned_after_panicked_attempt() {
    let handler = Arc::new(PanicThenSucceed(AtomicUsize::new(0)));
    let service = TaskExecutionServiceBuilder::in_memory()
        .max_running_tasks(NonZeroUsize::new(1).unwrap())
        .register_handler(handler)
        .unwrap()
        .build()
        .await
        .unwrap();
    let first = service
        .submit(test_keyed(TaskRequest::new("panic-once", "1", Vec::new())))
        .await
        .unwrap();
    let second = service
        .submit(test_keyed(TaskRequest::new("panic-once", "1", Vec::new())))
        .await
        .unwrap();
    assert!(matches!(
        service.wait(first.id).await.unwrap().state,
        TaskState::Panicked { .. }
    ));
    assert!(matches!(
        service.wait(second.id).await.unwrap().state,
        TaskState::Succeeded
    ));
    service.shutdown().await.unwrap();
}

#[allow(dead_code)]
fn test_keyed(mut request: TaskRequest) -> TaskRequest {
    static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(1);
    if request.idempotency_key.is_none() {
        request.idempotency_key = Some(format!(
            "test-request-{}",
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
    }
    request
}
