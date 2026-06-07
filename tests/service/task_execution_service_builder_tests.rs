// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for [`TaskExecutionServiceBuilder`](qubit_task::service::TaskExecutionServiceBuilder).

use std::time::Duration;

use qubit_task::service::{
    ExecutorServiceBuilderError,
    TaskExecutionService,
    TaskExecutionServiceBuilder,
    ThreadPool,
};

#[test]
fn test_task_execution_service_builder_builds_default_service() {
    let service = TaskExecutionServiceBuilder::default()
        .build()
        .expect("default builder should create service");

    assert!(!service.is_not_running());
    assert!(service.thread_pool().maximum_pool_size() > 0);
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

    assert_eq!(service.thread_pool().maximum_pool_size(), 2);
    assert_eq!(service.thread_pool().stats().queued_tasks, 0);
    service.shutdown();
    service.wait_termination();
}

#[test]
fn test_task_execution_service_builder_returns_pool_build_error() {
    let result = TaskExecutionService::builder()
        .thread_pool(ThreadPool::builder().pool_size(0))
        .build();

    assert!(matches!(
        result,
        Err(ExecutorServiceBuilderError::ZeroMaximumPoolSize),
    ));
}
