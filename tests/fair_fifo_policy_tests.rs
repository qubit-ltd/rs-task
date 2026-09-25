use std::collections::BTreeMap;

use qubit_task::TaskId;
use qubit_task::model::ResourceCapacity;
use qubit_task::model::ResourceSnapshot;
use qubit_task::model::TaskRequest;
use qubit_task::scheduling::FairFifoPolicy;
use qubit_task::scheduling::QueueSnapshot;
use qubit_task::scheduling::QueuedTask;
use qubit_task::scheduling::SchedulingPolicy;

fn queued_task(name: &str) -> QueuedTask {
    QueuedTask {
        id: TaskId::generate(),
        resources: TaskRequest::new(name, "1", Vec::new()).resources,
        bypasses: 0,
    }
}

fn queue(tasks: Vec<QueuedTask>, scan_budget: usize) -> QueueSnapshot {
    QueueSnapshot { tasks, scan_budget }
}

#[test]
fn test_fair_fifo_policy_protects_head_when_max_bypasses_is_zero() {
    let mut head = queued_task("head");
    head.resources.cpu_slots = 2;
    let later = queued_task("later");
    let snapshot = queue(vec![head.clone(), later], 8);
    let resources = ResourceSnapshot {
        capacity: ResourceCapacity {
            cpu_slots: 1,
            ..ResourceCapacity::default()
        },
        ..ResourceSnapshot::default()
    };

    let result = FairFifoPolicy::new(0).order(&snapshot, &resources);

    assert_eq!(result, vec![head.id]);
}

#[test]
fn test_fair_fifo_policy_respects_scan_budget() {
    let mut blocked = queued_task("blocked");
    blocked.resources.cpu_slots = 2;
    let blocked_id = blocked.id;
    let available = queued_task("available");
    let resources = ResourceSnapshot {
        capacity: ResourceCapacity {
            cpu_slots: 1,
            ..ResourceCapacity::default()
        },
        ..ResourceSnapshot::default()
    };

    let truncated = FairFifoPolicy::new(8).order(&queue(vec![blocked.clone(), available.clone()], 1), &resources);
    let within_budget = FairFifoPolicy::new(8).order(&queue(vec![blocked, available.clone()], 2), &resources);

    assert_eq!(truncated, vec![blocked_id]);
    assert_eq!(within_budget, vec![available.id, blocked_id]);
}

#[test]
fn test_fair_fifo_policy_checks_cpu_capacity_and_current_usage() {
    let mut task = queued_task("cpu");
    task.resources.cpu_slots = 2;
    let snapshot = queue(vec![task.clone()], 1);
    let sufficient = ResourceSnapshot {
        capacity: ResourceCapacity {
            cpu_slots: 3,
            ..ResourceCapacity::default()
        },
        used_cpu_slots: 1,
        ..ResourceSnapshot::default()
    };
    let insufficient = ResourceSnapshot {
        capacity: ResourceCapacity {
            cpu_slots: 3,
            ..ResourceCapacity::default()
        },
        used_cpu_slots: 2,
        ..ResourceSnapshot::default()
    };

    assert_eq!(FairFifoPolicy::new(8).order(&snapshot, &sufficient), vec![task.id]);
    assert!(FairFifoPolicy::new(8).order(&snapshot, &insufficient).is_empty());
}

#[test]
fn test_fair_fifo_policy_checks_gpu_labels_and_custom_resources() {
    let mut task = queued_task("gpu-and-license");
    task.resources.gpu_count = 1;
    task.resources.gpu_labels = vec!["cuda".to_owned()];
    task.resources.custom = BTreeMap::from([("license".to_owned(), 2)]);
    let snapshot = queue(vec![task.clone()], 1);
    let resources = ResourceSnapshot {
        capacity: ResourceCapacity {
            cpu_slots: 1,
            gpus: BTreeMap::from([
                ("gpu0".to_owned(), vec!["cuda".to_owned()]),
                ("gpu1".to_owned(), vec!["cuda".to_owned(), "fast".to_owned()]),
            ]),
            custom: BTreeMap::from([("license".to_owned(), 3)]),
        },
        used_gpus: vec!["gpu0".to_owned()],
        used_custom: BTreeMap::from([("license".to_owned(), 1)]),
        ..ResourceSnapshot::default()
    };
    assert_eq!(FairFifoPolicy::new(8).order(&snapshot, &resources), vec![task.id]);

    let mut wrong_label = resources.clone();
    wrong_label
        .capacity
        .gpus
        .insert("gpu1".to_owned(), vec!["rocm".to_owned()]);
    assert!(FairFifoPolicy::new(8).order(&snapshot, &wrong_label).is_empty());

    let mut unavailable_gpu = resources.clone();
    unavailable_gpu.used_gpus.push("gpu1".to_owned());
    assert!(FairFifoPolicy::new(8).order(&snapshot, &unavailable_gpu).is_empty());

    let mut insufficient_custom = resources;
    insufficient_custom.capacity.custom.insert("license".to_owned(), 2);
    assert!(FairFifoPolicy::new(8).order(&snapshot, &insufficient_custom).is_empty());
}

#[test]
fn test_fair_fifo_policy_surfaces_requests_that_exceed_total_capacity() {
    let mut too_many_gpus = queued_task("too-many-gpus");
    too_many_gpus.resources.gpu_count = 2;
    let mut unknown_gpu_label = queued_task("unknown-gpu-label");
    unknown_gpu_label.resources.gpu_count = 1;
    unknown_gpu_label.resources.gpu_labels = vec!["rocm".into()];
    let mut too_much_custom = queued_task("too-much-custom");
    too_much_custom.resources.custom.insert("license".into(), 2);
    let snapshot = queue(
        vec![
            too_many_gpus.clone(),
            unknown_gpu_label.clone(),
            too_much_custom.clone(),
        ],
        8,
    );
    let resources = ResourceSnapshot {
        capacity: ResourceCapacity {
            cpu_slots: 4,
            gpus: BTreeMap::from([("gpu0".into(), vec!["cuda".into()])]),
            custom: BTreeMap::from([("license".into(), 1)]),
        },
        ..ResourceSnapshot::default()
    };

    assert_eq!(
        FairFifoPolicy::new(8).order(&snapshot, &resources),
        vec![too_many_gpus.id, unknown_gpu_label.id, too_much_custom.id]
    );
}
