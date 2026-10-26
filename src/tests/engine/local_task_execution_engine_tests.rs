// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for local task resource reservation behavior.

use std::collections::BTreeMap;

use qubit_id::Id;

use crate::engine::EngineError;
use crate::engine::LocalTaskExecutionEngine;
use crate::model::ResourceCapacity;
use crate::model::typed::ResourceRequest;
use crate::model::typed::TaskId;

fn task_id(value: u64) -> TaskId {
    TaskId::from_id(Id::new(value))
}

fn request() -> ResourceRequest {
    ResourceRequest::default()
}

#[test]
fn test_exact_cpu_gpu_memory_disk_and_custom_capacity_can_be_reserved_and_reused() {
    let capacity = ResourceCapacity {
        cpu_slots: 2,
        memory_bytes: Some(64),
        disk_bytes: Some(128),
        gpus: BTreeMap::from([
            ("gpu-a".into(), vec!["cuda".into(), "large-memory".into()]),
            ("gpu-b".into(), vec!["cuda".into()]),
        ]),
        custom: BTreeMap::from([("license".into(), 3)]),
    };
    let engine = LocalTaskExecutionEngine::new(capacity);
    let request = ResourceRequest {
        cpu_slots: 2,
        gpu_count: 1,
        memory_bytes: Some(64),
        disk_bytes: Some(128),
        gpu_labels: vec!["large-memory".into()],
        custom: BTreeMap::from([("license".into(), 3)]),
    };

    let reservation = engine
        .try_prepare_typed(task_id(1), request.clone())
        .expect("request at every capacity boundary should be admitted");
    assert!(matches!(
        engine.try_prepare_typed(task_id(2), request.clone()),
        Err(EngineError::TemporarilyUnavailable)
    ));
    drop(reservation);
    assert!(engine.try_prepare_typed(task_id(3), request).is_ok());
}

#[test]
fn test_impossible_requests_are_distinguished_from_temporarily_reserved_capacity() {
    let engine = LocalTaskExecutionEngine::new(ResourceCapacity {
        cpu_slots: 1,
        memory_bytes: Some(10),
        disk_bytes: Some(20),
        gpus: BTreeMap::from([("gpu-0".into(), vec!["cuda".into()])]),
        custom: BTreeMap::from([("license".into(), 1)]),
    });

    let mut impossible = request();
    impossible.cpu_slots = 2;
    assert!(matches!(
        engine.try_prepare_typed(task_id(1), impossible),
        Err(EngineError::Unsatisfiable)
    ));

    let mut unavailable = request();
    unavailable.cpu_slots = 1;
    let reservation = engine
        .try_prepare_typed(task_id(2), unavailable.clone())
        .expect("first request should reserve the only CPU slot");
    assert!(matches!(
        engine.try_prepare_typed(task_id(3), unavailable),
        Err(EngineError::TemporarilyUnavailable)
    ));
    drop(reservation);
}

#[test]
fn test_typed_request_exceeding_each_resource_limit_is_unsatisfiable() {
    let engine = LocalTaskExecutionEngine::new(ResourceCapacity {
        cpu_slots: 2,
        memory_bytes: Some(10),
        disk_bytes: Some(20),
        gpus: BTreeMap::from([("gpu-0".into(), vec!["cuda".into()])]),
        custom: BTreeMap::from([("license".into(), 2)]),
    });
    let mut cases = Vec::new();

    let mut cpu = request();
    cpu.cpu_slots = 3;
    cases.push(cpu);

    let mut gpu_count = request();
    gpu_count.gpu_count = 2;
    cases.push(gpu_count);

    let mut gpu_label = request();
    gpu_label.gpu_count = 1;
    gpu_label.gpu_labels.push("rocm".into());
    cases.push(gpu_label);

    let mut memory = request();
    memory.memory_bytes = Some(11);
    cases.push(memory);

    let mut disk = request();
    disk.disk_bytes = Some(21);
    cases.push(disk);

    let mut unknown_custom = request();
    unknown_custom.custom.insert("missing".into(), 1);
    cases.push(unknown_custom);

    let mut excessive_custom = request();
    excessive_custom.custom.insert("license".into(), 3);
    cases.push(excessive_custom);

    for (index, request) in cases.into_iter().enumerate() {
        assert!(matches!(
            engine.try_prepare_typed(task_id(index as u64 + 10), request),
            Err(EngineError::Unsatisfiable)
        ));
    }
}

#[test]
fn test_omitted_memory_and_disk_quotas_consume_no_capacity() {
    let engine = LocalTaskExecutionEngine::new(ResourceCapacity {
        cpu_slots: 1,
        memory_bytes: None,
        disk_bytes: None,
        ..ResourceCapacity::default()
    });
    assert!(engine.try_prepare_typed(task_id(20), request()).is_ok());
}
