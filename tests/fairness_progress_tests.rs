use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::engine::{
    EngineError, ExecutionHandle, LocalTaskExecutionEngine, PreparedExecution, TaskExecutionEngine,
};
use qubit_task::handler::{
    TaskContext, TaskHandler, TaskHandlerDescriptor, TaskRunOutcome, TaskRunResult,
};
use qubit_task::model::{
    ResourceCapacity, ResourceRequest, ResourceSnapshot, TaskId, TaskOutput, TaskRequest,
};
use qubit_task::scheduling::{FairFifoPolicy, QueueSnapshot, SchedulingPolicy};
use qubit_task::service::LocalTaskOutcome;
use tokio::sync::Semaphore;
use tokio::sync::mpsc;

struct ObservePolicy {
    allow_later: AtomicBool,
    observed: mpsc::UnboundedSender<Vec<(TaskId, u32)>>,
}

impl SchedulingPolicy for ObservePolicy {
    fn order(&self, queue: &QueueSnapshot, _resources: &ResourceSnapshot) -> Vec<TaskId> {
        let _ = self.observed.send(
            queue
                .tasks
                .iter()
                .map(|task| (task.id, task.bypasses))
                .collect(),
        );
        if self.allow_later.load(Ordering::Acquire) {
            queue
                .tasks
                .last()
                .map(|task| vec![task.id])
                .unwrap_or_default()
        } else {
            Vec::new()
        }
    }
}

async fn observe_until(
    receiver: &mut mpsc::UnboundedReceiver<Vec<(TaskId, u32)>>,
    predicate: impl Fn(&[(TaskId, u32)]) -> bool,
) -> Vec<(TaskId, u32)> {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let snapshot = receiver
                .recv()
                .await
                .expect("scheduler observation channel stays open");
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

impl SchedulingPolicy for ObservingFairPolicy {
    fn order(&self, queue: &QueueSnapshot, resources: &ResourceSnapshot) -> Vec<TaskId> {
        let _ = self.observed.send(
            queue
                .tasks
                .iter()
                .map(|task| (task.id, task.bypasses))
                .collect(),
        );
        self.inner.order(queue, resources)
    }
}

struct GatedHandler {
    started: mpsc::UnboundedSender<u8>,
    release: Arc<Semaphore>,
}

impl TaskHandler for GatedHandler {
    fn descriptor(&self) -> TaskHandlerDescriptor {
        TaskHandlerDescriptor {
            task_type: "fairness-gated".into(),
            version: "1".into(),
        }
    }

    fn run<'a>(
        &'a self,
        payload: &'a [u8],
        _context: TaskContext,
    ) -> qubit_task::store::TaskFuture<'a, TaskRunResult> {
        Box::pin(async move {
            self.started
                .send(payload[0])
                .expect("test event receiver stays open");
            self.release
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

    fn prepare<'a>(
        &'a self,
        id: TaskId,
        request: ResourceRequest,
    ) -> qubit_task::store::TaskFuture<'a, Result<PreparedExecution, EngineError>> {
        self.inner.prepare(id, request)
    }

    fn activate<'a>(
        &'a self,
        _prepared: PreparedExecution,
        _handler: Arc<dyn TaskHandler>,
        _payload: Vec<u8>,
        _context: TaskContext,
    ) -> qubit_task::store::TaskFuture<'a, Result<ExecutionHandle, EngineError>> {
        Box::pin(async { Err(EngineError::Closed) })
    }
}

#[tokio::test]
async fn empty_scheduler_rounds_do_not_increment_bypasses() {
    let (observed, mut observations) = mpsc::unbounded_channel();
    let policy = Arc::new(ObservePolicy {
        allow_later: AtomicBool::new(false),
        observed,
    });
    let service = TaskExecutionServiceBuilder::in_memory()
        .policy(policy)
        .build()
        .await
        .expect("service builds");
    let handle = service
        .submit_local(local_success)
        .await
        .expect("task is accepted");
    let task_id = handle.task_id();

    let snapshots = tokio::time::timeout(Duration::from_secs(3), async {
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

    assert!(snapshots.iter().all(|snapshot| {
        snapshot
            .iter()
            .find(|(id, _)| *id == task_id)
            .map(|(_, count)| *count)
            == Some(0)
    }));
}

#[tokio::test]
async fn only_a_successfully_started_later_task_counts_as_a_bypass() {
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
        snapshot.iter().any(|(id, _)| *id == first_id)
            && snapshot.iter().any(|(id, _)| *id == second_id)
    })
    .await;
    policy.allow_later.store(true, Ordering::Release);

    let after_start = observe_until(&mut observations, |snapshot| {
        snapshot
            .iter()
            .any(|(id, bypasses)| *id == first_id && *bypasses > 0)
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
    assert!(
        second
            .result()
            .await
            .expect("second outcome arrives")
            .is_ok()
    );
}

#[tokio::test]
async fn failed_activation_does_not_count_as_a_bypass() {
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
        snapshot.iter().any(|(id, _)| *id == first_id)
            && snapshot.iter().any(|(id, _)| *id == second_id)
    })
    .await;
    policy.allow_later.store(true, Ordering::Release);

    let after_failed_activation = observe_until(&mut observations, |snapshot| {
        snapshot.iter().any(|(id, _)| *id == first_id)
            && !snapshot.iter().any(|(id, _)| *id == second_id)
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
}

#[test]
fn protected_head_stays_first_until_resources_are_returned() {
    let mut large = qubit_task::scheduling::QueuedTask {
        id: TaskId::generate(),
        request: qubit_task::model::TaskRequest::new("large", "1", Vec::new()),
        bypasses: 2,
    };
    large.request.resources.cpu_slots = 2;
    let later = qubit_task::scheduling::QueuedTask {
        id: TaskId::generate(),
        request: qubit_task::model::TaskRequest::new("small", "1", Vec::new()),
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

    assert_eq!(policy.order(&queue, &constrained), vec![large.id]);
    assert_eq!(policy.order(&queue, &available), vec![large.id]);
}

#[tokio::test]
async fn protected_large_task_starts_before_small_tasks_after_resources_return() {
    let (observed, mut observations) = mpsc::unbounded_channel();
    let (started, mut starts) = mpsc::unbounded_channel();
    let release = Arc::new(Semaphore::new(0));
    let policy = Arc::new(ObservingFairPolicy {
        inner: FairFifoPolicy::default(),
        observed,
    });
    let service = TaskExecutionServiceBuilder::in_memory()
        .capacity(ResourceCapacity {
            cpu_slots: 2,
            ..ResourceCapacity::default()
        })
        .scan_budget(32)
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
        .submit(preoccupier)
        .await
        .expect("preoccupier is accepted");
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), starts.recv())
            .await
            .expect("preoccupier starts")
            .expect("handler event channel remains open"),
        b'P'
    );

    let mut large = TaskRequest::new("fairness-gated", "1", b"L".to_vec());
    large.resources.cpu_slots = 2;
    let large = service.submit(large).await.expect("large task is accepted");
    for index in 0..9 {
        let mut small = TaskRequest::new("fairness-gated", "1", vec![b'0' + index]);
        small.resources.cpu_slots = 1;
        service.submit(small).await.expect("small task is accepted");
    }

    for _ in 0..8 {
        let label = tokio::time::timeout(Duration::from_secs(3), starts.recv())
            .await
            .expect("a later small task starts before protection threshold")
            .expect("handler event channel remains open");
        assert!((b'0'..=b'8').contains(&label));
        assert_ne!(
            label, b'8',
            "the ninth small task must remain queued at the threshold"
        );
        release.add_permits(1);
    }

    observe_until(&mut observations, |snapshot| {
        snapshot
            .iter()
            .find(|(id, _)| *id == large.id)
            .is_some_and(|(_, bypasses)| *bypasses >= 8)
    })
    .await;
    release.add_permits(1);
    let first_after_release = tokio::time::timeout(Duration::from_secs(3), starts.recv())
        .await
        .expect("a queued task starts after the preoccupier releases its slot")
        .expect("handler event channel remains open");
    assert_eq!(
        first_after_release, b'L',
        "the protected large task starts first"
    );
}
