// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::time::Duration;

use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::TaskId;
use qubit_task::service::LocalTaskOutcome;
use qubit_task::model::ResourceSnapshot;
use qubit_task::model::TaskOutput;
use qubit_task::model::TaskState;
use qubit_task::scheduling::QueueSnapshot;
use qubit_task::scheduling::SchedulingPolicy;
use qubit_task::service::CancelOutcome;
use qubit_task::service::TaskServiceError;

struct HoldFirstOrder {
    held: AtomicBool,
    entered: mpsc::Sender<Vec<TaskId>>,
    release: Mutex<mpsc::Receiver<()>>,
}

impl SchedulingPolicy for HoldFirstOrder {
    fn order(&self, queue: &QueueSnapshot, _: &ResourceSnapshot) -> Vec<TaskId> {
        let ids: Vec<_> = queue.tasks.iter().map(|task| task.id).collect();
        if !self.held.swap(true, Ordering::AcqRel) {
            self.entered
                .send(ids.clone())
                .expect("test receives first policy snapshot");
            self.release
                .lock()
                .expect("release receiver lock")
                .recv()
                .expect("first policy call is released");
        }
        ids
    }
}

#[tokio::test]
async fn test_cancelling_shared_queue_task_releases_one_capacity_slot() {
    let (entered, first_snapshot) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let policy = HoldFirstOrder {
        held: AtomicBool::new(false),
        entered,
        release: Mutex::new(released),
    };
    let service = TaskExecutionServiceBuilder::in_memory()
        .policy(Arc::new(policy))
        .queue_capacity(3)
        .build()
        .await
        .expect("service builds");
    let a = service
        .submit_local(|_| LocalTaskOutcome::<(), std::io::Error>::Succeeded { value: (), summary: TaskOutput::default() })
        .await
        .expect("A accepted")
        .task_id();
    assert_eq!(
        first_snapshot
            .recv_timeout(Duration::from_secs(2))
            .expect("scheduler holds A"),
        vec![a]
    );
    let b = service
        .submit_local(|_| LocalTaskOutcome::<(), std::io::Error>::Succeeded { value: (), summary: TaskOutput::default() })
        .await
        .expect("B accepted")
        .task_id();
    service
        .submit_local(|_| LocalTaskOutcome::<(), std::io::Error>::Succeeded { value: (), summary: TaskOutput::default() })
        .await
        .expect("C accepted");
    assert_eq!(
        service.cancel(b).await.expect("B cancelled"),
        CancelOutcome::CancelledBeforeStart
    );
    let d = service
        .submit_local(|_| LocalTaskOutcome::<(), std::io::Error>::Succeeded { value: (), summary: TaskOutput::default() })
        .await;
    let e = service
        .submit_local(|_| LocalTaskOutcome::<(), std::io::Error>::Succeeded { value: (), summary: TaskOutput::default() })
        .await;
    release.send(()).expect("release scheduler");
    service.shutdown().await.expect("remaining tasks drain");
    assert!(d.is_ok(), "D uses B's released slot: {d:?}");
    assert!(
        matches!(e, Err(TaskServiceError::QueueFull)),
        "E must exceed capacity: {e:?}"
    );
    assert_eq!(
        service
            .get(b)
            .await
            .expect("B query succeeds")
            .expect("B retained")
            .state,
        TaskState::Cancelled
    );
}
