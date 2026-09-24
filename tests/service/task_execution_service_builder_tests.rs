// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for [`TaskExecutionServiceBuilder`](qubit_task::service::TaskExecutionServiceBuilder).

use std::time::Duration;

use qubit_executor::service::ExecutorServiceBuilderError;
use qubit_id::Id;
use qubit_task::service::TaskExecutionService;
use qubit_task::service::TaskExecutionServiceBuilder;
use qubit_task::service::TaskStatus;
use qubit_thread_pool::ThreadPool;

#[test]
fn test_task_execution_service_builder_builds_default_service() {
    let service = TaskExecutionServiceBuilder::default()
        .build()
        .expect("default builder should create service");

    assert!(!service.is_not_running());
    assert!(service.thread_pool_stats().maximum_pool_size > 0);
    service.shutdown();
    service.wait_termination();
}

#[test]
fn test_task_execution_service_builder_applies_thread_pool_builder() {
    let service = TaskExecutionService::builder()
        .thread_pool(
            ThreadPool::builder()
                .pool_size(2)
                .queue_capacity(3)
                .keep_alive(Duration::from_millis(25)),
        )
        .build()
        .expect("custom pool builder should create service");

    assert_eq!(service.thread_pool_stats().maximum_pool_size, 2);
    assert_eq!(service.thread_pool_stats().queued_tasks, 0);
    service.shutdown();
    service.wait_termination();
}

#[test]
fn test_task_execution_service_builder_returns_pool_build_error() {
    let result = TaskExecutionService::builder()
        .thread_pool(ThreadPool::builder().pool_size(0))
        .build();

    assert!(matches!(result, Err(ExecutorServiceBuilderError::ZeroMaximumPoolSize),));
}

#[test]
fn test_task_execution_service_builder_sets_completed_history_capacity() {
    let service = TaskExecutionService::builder()
        .completed_history_capacity(1)
        .build()
        .expect("service should be created");
    for id in 1..=2 {
        service
            .submit(Id::new(id), || Ok::<(), ()>(()))
            .expect("task should be accepted")
            .get()
            .expect("task should complete");
    }
    assert_eq!(service.status(Id::new(1)), None);
    assert_eq!(service.status(Id::new(2)), Some(TaskStatus::Succeeded));
    assert_eq!(service.stats().total, 1);
    service.shutdown();
    service.wait_termination();
}

#[test]
fn test_task_execution_service_builder_default_history_is_bounded() {
    let service = TaskExecutionService::new().expect("service should be created");
    for id in 0..=1024 {
        service
            .submit(Id::new(id), || Ok::<(), ()>(()))
            .expect("task should be accepted")
            .get()
            .expect("task should complete");
    }
    assert_eq!(service.status(Id::new(0)), None);
    assert_eq!(service.status(Id::new(1)), Some(TaskStatus::Succeeded));
    assert_eq!(service.status(Id::new(1024)), Some(TaskStatus::Succeeded));
    assert_eq!(service.stats().total, 1024);
    service.shutdown();
    service.wait_termination();
}
