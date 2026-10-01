// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::sync::Semaphore;
use tokio::sync::mpsc;
use tokio::test as tokio_test;
use tokio::time;

use super::super::task_execution_service_builder::TaskExecutionServiceBuilder;
use crate::engine::EngineError;
use crate::engine::ExecutionHandle;
use crate::engine::LocalTaskExecutionEngine;
use crate::engine::PreparedExecution;
use crate::engine::TaskExecutionEngine;
use crate::handler::TaskContext;
use crate::handler::TaskHandler;
use crate::handler::TaskHandlerDescriptor;
use crate::handler::TaskRunOutcome;
use crate::handler::TaskRunResult;
use crate::model::ResourceCapacity;
use crate::model::ResourceRequest;
use crate::model::ResourceSnapshot;
use crate::model::TaskId;
use crate::model::TaskOutput;
use crate::model::TaskRequest;
use crate::model::TaskState;
use crate::scheduling::FairFifoPolicy;
use crate::scheduling::QueueSnapshot;
use crate::scheduling::QueuedTask;
use crate::scheduling::SchedulingPlan;
use crate::scheduling::SchedulingPolicy;
use crate::service::LocalTaskOutcome;
use crate::store::TaskFuture;

struct ObservePolicy {
    allow_later: AtomicBool,
    observed: mpsc::UnboundedSender<Vec<(TaskId, u32)>>,
}

impl SchedulingPolicy for ObservePolicy {
    fn order(&self, queue: &QueueSnapshot, _resources: &ResourceSnapshot) -> SchedulingPlan {
        let _ = self
            .observed
            .send(queue.tasks.iter().map(|task| (task.id, task.bypasses)).collect());
        let order = if self.allow_later.load(Ordering::Acquire) {
            queue.tasks.last().map(|task| vec![task.id]).unwrap_or_default()
        } else {
            Vec::new()
        };
        SchedulingPlan { order, barrier: None }
    }
}

async fn observe_until<T>(receiver: &mut mpsc::UnboundedReceiver<Vec<T>>, predicate: impl Fn(&[T]) -> bool) -> Vec<T> {
    time::timeout(Duration::from_secs(3), async {
        loop {
            let snapshot = receiver.recv().await.expect("scheduler observation channel stays open");
            if predicate(&snapshot) {
                return snapshot;
            }
        }
    })
    .await
    .expect("scheduler reaches the expected progress point")
}

fn local_success(_: TaskContext) -> LocalTaskOutcome<(), Infallible> {
    LocalTaskOutcome::Succeeded {
        value: (),
        summary: TaskOutput::default(),
    }
}

struct FailingActivationEngine {
    inner: LocalTaskExecutionEngine,
}

struct ObservingFairPolicy {
    inner: FairFifoPolicy,
    observed: mpsc::UnboundedSender<Vec<(TaskId, u32)>>,
}

struct BoundedSnapshotPolicy {
    inner: FairFifoPolicy,
    observed: mpsc::UnboundedSender<Vec<(TaskId, ResourceRequest)>>,
}

impl SchedulingPolicy for BoundedSnapshotPolicy {
    fn order(&self, queue: &QueueSnapshot, resources: &ResourceSnapshot) -> SchedulingPlan {
        let _ = self.observed.send(
            queue
                .tasks
                .iter()
                .map(|task| (task.id, task.resources.clone()))
                .collect(),
        );
        self.inner.order(queue, resources)
    }
}

impl SchedulingPolicy for ObservingFairPolicy {
    fn order(&self, queue: &QueueSnapshot, resources: &ResourceSnapshot) -> SchedulingPlan {
        let _ = self
            .observed
            .send(queue.tasks.iter().map(|task| (task.id, task.bypasses)).collect());
        self.inner.order(queue, resources)
    }
}

struct GatedHandler {
    started: mpsc::UnboundedSender<u8>,
    release: Arc<HashMap<u8, Arc<Semaphore>>>,
}

struct PayloadHandler {
    started: mpsc::UnboundedSender<Vec<u8>>,
    release_first: Arc<Semaphore>,
}

impl TaskHandler for PayloadHandler {
    fn descriptor(&self) -> TaskHandlerDescriptor {
        TaskHandlerDescriptor {
            task_type: "payload-copy".into(),
            version: "1".into(),
        }
    }

    fn run<'a>(&'a self, payload: &'a [u8], _context: TaskContext) -> TaskFuture<'a, TaskRunResult> {
        Box::pin(async move {
            self.started
                .send(payload.to_vec())
                .expect("payload receiver stays open");
            if payload.first() == Some(&b'P') {
                self.release_first
                    .acquire()
                    .await
                    .expect("first task gate stays open")
                    .forget();
            }
            Ok(TaskRunOutcome::Succeeded(TaskOutput::default()))
        })
    }
}

impl TaskHandler for GatedHandler {
    fn descriptor(&self) -> TaskHandlerDescriptor {
        TaskHandlerDescriptor {
            task_type: "fairness-gated".into(),
            version: "1".into(),
        }
    }

    fn run<'a>(&'a self, payload: &'a [u8], _context: TaskContext) -> TaskFuture<'a, TaskRunResult> {
        Box::pin(async move {
            let label = payload[0];
            self.started.send(label).expect("test event receiver stays open");
            self.release
                .get(&label)
                .expect("each test task has a dedicated release gate")
                .acquire()
                .await
                .expect("test release gate stays open")
                .forget();
            Ok(TaskRunOutcome::Succeeded(TaskOutput::default()))
        })
    }
}

impl TaskExecutionEngine for FailingActivationEngine {
    fn capacity(&self) -> ResourceSnapshot {
        self.inner.capacity()
    }

    fn try_prepare(&self, id: TaskId, request: ResourceRequest) -> Result<PreparedExecution, EngineError> {
        self.inner.try_prepare(id, request)
    }

    fn activate<'a>(
        &'a self,
        _prepared: PreparedExecution,
        _handler: Arc<dyn TaskHandler>,
        _payload: Vec<u8>,
        _context: TaskContext,
    ) -> TaskFuture<'a, Result<ExecutionHandle, EngineError>> {
        Box::pin(async { Err(EngineError::Closed) })
    }
}

#[tokio_test]
async fn test_cross_window_runnable_task_progresses() {
    let (started, mut starts) = mpsc::unbounded_channel();
    let release = Arc::new(
        b"PABs"
            .iter()
            .copied()
            .map(|label| (label, Arc::new(Semaphore::new(0))))
            .collect::<HashMap<_, _>>(),
    );
    let service = TaskExecutionServiceBuilder::in_memory()
        .capacity(ResourceCapacity {
            cpu_slots: 2,
            ..ResourceCapacity::default()
        })
        .scan_budget(2)
        .register_handler(Arc::new(GatedHandler {
            started,
            release: Arc::clone(&release),
        }))
        .expect("handler registration succeeds")
        .build()
        .await
        .expect("service builds");

    let mut preoccupier = TaskRequest::new("fairness-gated", "1", b"P".to_vec());
    preoccupier.resources.cpu_slots = 1;
    service
        .submit(test_keyed(preoccupier))
        .await
        .expect("preoccupier is accepted");
    assert_eq!(
        time::timeout(Duration::from_secs(3), starts.recv())
            .await
            .expect("preoccupier starts")
            .expect("handler event channel remains open"),
        b'P'
    );

    for label in b"AB".iter().copied() {
        let mut large = TaskRequest::new("fairness-gated", "1", vec![label]);
        large.resources.cpu_slots = 2;
        service.submit(test_keyed(large)).await.expect("large task is accepted");
    }
    let mut small = TaskRequest::new("fairness-gated", "1", b"s".to_vec());
    small.resources.cpu_slots = 1;
    service.submit(test_keyed(small)).await.expect("small task is accepted");

    assert_eq!(
        time::timeout(Duration::from_secs(3), starts.recv())
            .await
            .expect("later runnable task progresses beyond the blocked scan window")
            .expect("handler event channel remains open"),
        b's'
    );

    release[&b's'].add_permits(1);
    release[&b'P'].add_permits(1);
    for expected in b"AB" {
        let label = time::timeout(Duration::from_secs(3), starts.recv())
            .await
            .expect("large queued task starts after resources are returned")
            .expect("handler event channel remains open");
        assert_eq!(label, *expected);
        release[&label].add_permits(1);
    }
    service.shutdown().await.expect("service drains and shuts down");
}

#[tokio_test]
async fn test_empty_scheduler_rounds_do_not_increment_bypasses() {
    let (observed, mut observations) = mpsc::unbounded_channel();
    let policy = Arc::new(ObservePolicy {
        allow_later: AtomicBool::new(false),
        observed,
    });
    let service = TaskExecutionServiceBuilder::in_memory()
        .policy(policy.clone())
        .build()
        .await
        .expect("service builds");
    let handle = service.submit_local(local_success).await.expect("task is accepted");
    let task_id = handle.task_id();

    let snapshots = time::timeout(Duration::from_secs(3), async {
        let mut snapshots = Vec::new();
        while snapshots.len() < 3 {
            let snapshot = observations
                .recv()
                .await
                .expect("scheduler observation channel stays open");
            if snapshot.iter().any(|(id, _)| *id == task_id) {
                snapshots.push(snapshot);
            }
        }
        snapshots
    })
    .await
    .expect("scheduler completes repeated empty rounds");

    assert!(
        snapshots
            .iter()
            .all(|snapshot| { snapshot.iter().find(|(id, _)| *id == task_id).map(|(_, count)| *count) == Some(0) })
    );
    policy.allow_later.store(true, Ordering::Release);
    handle
        .result()
        .await
        .expect("task completes after scheduling is enabled");
    service.shutdown().await.expect("service shuts down");
}

#[tokio_test]
async fn test_only_a_successfully_started_later_task_counts_as_a_bypass() {
    let (observed, mut observations) = mpsc::unbounded_channel();
    let policy = Arc::new(ObservePolicy {
        allow_later: AtomicBool::new(false),
        observed,
    });
    let service = TaskExecutionServiceBuilder::in_memory()
        .policy(policy.clone())
        .build()
        .await
        .expect("service builds");
    let first = service
        .submit_local(local_success)
        .await
        .expect("first task is accepted");
    let second = service
        .submit_local(local_success)
        .await
        .expect("second task is accepted");
    let first_id = first.task_id();
    let second_id = second.task_id();

    observe_until(&mut observations, |snapshot| {
        snapshot.iter().any(|(id, _)| *id == first_id) && snapshot.iter().any(|(id, _)| *id == second_id)
    })
    .await;
    policy.allow_later.store(true, Ordering::Release);

    let after_start = observe_until(&mut observations, |snapshot| {
        snapshot.iter().any(|(id, bypasses)| *id == first_id && *bypasses > 0)
    })
    .await;
    assert_eq!(
        after_start
            .iter()
            .find(|(id, _)| *id == first_id)
            .map(|(_, count)| *count),
        Some(1),
        "one later activation increments only the earlier task, exactly once"
    );
    assert!(first.result().await.expect("first outcome arrives").is_ok());
    assert!(second.result().await.expect("second outcome arrives").is_ok());
    service.shutdown().await.expect("service shuts down");
}

#[tokio_test]
async fn test_failed_activation_does_not_count_as_a_bypass() {
    let (observed, mut observations) = mpsc::unbounded_channel();
    let policy = Arc::new(ObservePolicy {
        allow_later: AtomicBool::new(false),
        observed,
    });
    let capacity = ResourceCapacity {
        cpu_slots: 2,
        ..ResourceCapacity::default()
    };
    let engine = Arc::new(FailingActivationEngine {
        inner: LocalTaskExecutionEngine::new(capacity),
    });
    let service = TaskExecutionServiceBuilder::in_memory()
        .engine(engine)
        .policy(policy.clone())
        .build()
        .await
        .expect("service builds");
    let first = service
        .submit_local(local_success)
        .await
        .expect("first task is accepted");
    let second = service
        .submit_local(local_success)
        .await
        .expect("second task is accepted");
    let first_id = first.task_id();
    let second_id = second.task_id();

    observe_until(&mut observations, |snapshot| {
        snapshot.iter().any(|(id, _)| *id == first_id) && snapshot.iter().any(|(id, _)| *id == second_id)
    })
    .await;
    policy.allow_later.store(true, Ordering::Release);

    let after_failed_activation = observe_until(&mut observations, |snapshot| {
        snapshot.iter().any(|(id, _)| *id == first_id) && !snapshot.iter().any(|(id, _)| *id == second_id)
    })
    .await;
    assert_eq!(
        after_failed_activation
            .iter()
            .find(|(id, _)| *id == first_id)
            .map(|(_, count)| *count),
        Some(0),
        "an engine activation error must not advance the earlier task's bypass budget"
    );
    service
        .shutdown()
        .await
        .expect("service shuts down after activation errors");
}

#[test]
fn test_protected_head_stays_first_until_resources_are_returned() {
    let mut large = QueuedTask {
        id: TaskId::generate(),
        resources: TaskRequest::new("large", "1", Vec::new()).resources,
        retry_not_before_ms: None,
        bypasses: 2,
    };
    large.resources.cpu_slots = 2;
    let later = QueuedTask {
        id: TaskId::generate(),
        resources: TaskRequest::new("small", "1", Vec::new()).resources,
        retry_not_before_ms: None,
        bypasses: 0,
    };
    let policy = FairFifoPolicy::new(2);
    let queue = QueueSnapshot {
        tasks: vec![large.clone(), later],
        scan_budget: 8,
    };
    let constrained = ResourceSnapshot {
        capacity: ResourceCapacity {
            cpu_slots: 2,
            ..ResourceCapacity::default()
        },
        used_cpu_slots: 1,
        ..ResourceSnapshot::default()
    };
    let available = ResourceSnapshot {
        capacity: ResourceCapacity {
            cpu_slots: 2,
            ..ResourceCapacity::default()
        },
        ..ResourceSnapshot::default()
    };

    assert_eq!(policy.order(&queue, &constrained).order, vec![large.id]);
    assert_eq!(policy.order(&queue, &available).order, vec![large.id]);
}

#[tokio_test]
async fn test_protected_large_task_starts_before_small_tasks_after_resources_return() {
    let (observed, mut observations) = mpsc::unbounded_channel();
    let (started, mut starts) = mpsc::unbounded_channel();
    let release = Arc::new(
        (*b"PL012345678")
            .into_iter()
            .map(|label| (label, Arc::new(Semaphore::new(0))))
            .collect::<HashMap<_, _>>(),
    );
    let policy = Arc::new(ObservingFairPolicy {
        inner: FairFifoPolicy::default(),
        observed,
    });
    let service = TaskExecutionServiceBuilder::in_memory()
        .capacity(ResourceCapacity {
            cpu_slots: 2,
            ..ResourceCapacity::default()
        })
        .scan_budget(1)
        .policy(policy)
        .register_handler(Arc::new(GatedHandler {
            started,
            release: Arc::clone(&release),
        }))
        .expect("handler registration succeeds")
        .build()
        .await
        .expect("service builds");

    let mut preoccupier = TaskRequest::new("fairness-gated", "1", b"P".to_vec());
    preoccupier.resources.cpu_slots = 1;
    service
        .submit(test_keyed(preoccupier))
        .await
        .expect("preoccupier is accepted");
    assert_eq!(
        time::timeout(Duration::from_secs(3), starts.recv())
            .await
            .expect("preoccupier starts")
            .expect("handler event channel remains open"),
        b'P'
    );
    let mut large = TaskRequest::new("fairness-gated", "1", b"L".to_vec());
    large.resources.cpu_slots = 2;
    let large = service.submit(test_keyed(large)).await.expect("large task is accepted");
    observe_until(&mut observations, |snapshot| {
        snapshot.iter().any(|(id, _)| *id == large.id)
    })
    .await;
    for index in 0..9 {
        let mut small = TaskRequest::new("fairness-gated", "1", vec![b'0' + index]);
        small.resources.cpu_slots = 1;
        service.submit(test_keyed(small)).await.expect("small task is accepted");
    }

    let mut observed_bypasses = 0;
    let mut bypassed = HashMap::new();
    for _ in 0..8 {
        let label = time::timeout(Duration::from_secs(3), starts.recv())
            .await
            .expect("a later small task starts before protection threshold")
            .expect("handler event channel remains open");
        assert!((b'0'..=b'8').contains(&label));
        observe_until(&mut observations, |snapshot| {
            snapshot
                .iter()
                .find(|(id, _)| *id == large.id)
                .is_some_and(|(_, bypasses)| *bypasses > observed_bypasses)
        })
        .await;
        observed_bypasses += 1;
        assert!(
            bypassed.insert(label, ()).is_none(),
            "each bypass starts a distinct task"
        );
        release[&label].add_permits(1);
    }

    assert_eq!(observed_bypasses, 8);
    release[&b'P'].add_permits(1);
    let first_after_release = time::timeout(Duration::from_secs(3), starts.recv())
        .await
        .expect("a queued task starts after the preoccupier releases its slot")
        .expect("handler event channel remains open");
    assert_eq!(first_after_release, b'L', "the protected large task starts first");
    release[&b'L'].add_permits(1);
    let final_small_task = time::timeout(Duration::from_secs(3), starts.recv())
        .await
        .expect("remaining small task starts after the large task releases resources")
        .expect("handler event channel remains open");
    assert!((b'0'..=b'8').contains(&final_small_task));
    assert!(!bypassed.contains_key(&final_small_task));
    release[&final_small_task].add_permits(1);
    service.shutdown().await.expect("all queued tasks drain");
}

#[tokio_test]
async fn test_large_payloads_survive_bounded_resource_only_scheduler_snapshots() {
    let (observed, mut observations) = mpsc::unbounded_channel();
    let (started, mut starts) = mpsc::unbounded_channel();
    let release_first = Arc::new(Semaphore::new(0));
    let service = TaskExecutionServiceBuilder::in_memory()
        .capacity(ResourceCapacity {
            cpu_slots: 1,
            ..ResourceCapacity::default()
        })
        .queue_capacity(8)
        .scan_budget(1)
        .policy(Arc::new(BoundedSnapshotPolicy {
            inner: FairFifoPolicy::default(),
            observed,
        }))
        .register_handler(Arc::new(PayloadHandler {
            started,
            release_first: Arc::clone(&release_first),
        }))
        .expect("handler registration succeeds")
        .build()
        .await
        .expect("service builds");

    let first_payload = vec![b'P'];
    service
        .submit(test_keyed(TaskRequest::new("payload-copy", "1", first_payload.clone())))
        .await
        .expect("first task is accepted");
    assert_eq!(
        time::timeout(Duration::from_secs(3), starts.recv())
            .await
            .expect("first task starts")
            .expect("payload receiver remains open"),
        first_payload
    );

    let payloads = (0..4).map(|index| vec![b'A' + index; 256 * 1024]).collect::<Vec<_>>();
    let mut queued_ids = Vec::new();
    for payload in &payloads {
        queued_ids.push(
            service
                .submit(test_keyed(TaskRequest::new("payload-copy", "1", payload.clone())))
                .await
                .expect("large payload task is accepted")
                .id,
        );
    }
    let snapshot = observe_until(&mut observations, |candidate| {
        candidate.iter().any(|(id, _)| queued_ids.contains(id))
    })
    .await;
    assert_eq!(snapshot.len(), 1, "policy receives no more than scan_budget candidates");
    assert_eq!(snapshot[0].1.cpu_slots, 1, "policy receives resource demand only");

    release_first.add_permits(1);
    let mut received = Vec::new();
    for _ in &payloads {
        received.push(
            time::timeout(Duration::from_secs(5), starts.recv())
                .await
                .expect("queued task starts after resources are released")
                .expect("payload receiver remains open"),
        );
    }
    received.sort_by_key(|payload| payload[0]);
    let mut expected = payloads;
    expected.sort_by_key(|payload| payload[0]);
    assert_eq!(received, expected, "full payload bytes come from the task store");
    service.shutdown().await.expect("service drains and shuts down");
}

#[tokio_test]
async fn test_missing_handler_is_classified_without_bypass_counting() {
    let (observed, mut observations) = mpsc::unbounded_channel();
    let policy = Arc::new(ObservingFairPolicy {
        inner: FairFifoPolicy::default(),
        observed,
    });
    let service = TaskExecutionServiceBuilder::in_memory()
        .policy(policy)
        .build()
        .await
        .expect("service builds");
    let request = TaskRequest::new("missing-handler", "1", Vec::new());
    let record = service
        .submit(test_keyed(request))
        .await
        .expect("task is accepted for reporting");
    let task_id = record.id;
    time::timeout(Duration::from_secs(3), async {
        loop {
            let record = service
                .get(task_id)
                .await
                .expect("task record query succeeds")
                .expect("accepted task record remains stored");
            if matches!(record.state, TaskState::Blocked { .. }) {
                assert!(matches!(
                    record.state,
                    TaskState::Blocked { ref reason } if reason.contains("handler")
                ));
                break;
            }
            time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("missing handler is classified as Blocked");
    let snapshot = observe_until(&mut observations, |snapshot| {
        snapshot.iter().any(|(id, _)| *id == task_id)
    })
    .await;
    assert_eq!(
        snapshot.iter().find(|(id, _)| *id == task_id).map(|(_, count)| *count),
        Some(0)
    );
    service.shutdown().await.expect("blocked task service shuts down");
}

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
