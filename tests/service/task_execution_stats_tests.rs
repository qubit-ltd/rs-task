// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for [`TaskExecutionStats`](qubit_task::service::TaskExecutionStats).

use std::io;

use qubit_executor::TaskExecutionError;
use qubit_task::service::Id;
use qubit_task::service::TaskExecutionService;
use qubit_task::service::TaskExecutionStats;
use qubit_task::service::TaskStatus;

#[test]
fn test_task_execution_stats_default_is_empty() {
    let stats = TaskExecutionStats::default();

    assert_eq!(stats.total, 0);
    assert_eq!(stats.submitted, 0);
    assert_eq!(stats.running, 0);
    assert_eq!(stats.succeeded, 0);
    assert_eq!(stats.failed, 0);
    assert_eq!(stats.panicked, 0);
    assert_eq!(stats.cancelled, 0);
}

#[test]
fn test_task_execution_stats_counts_terminal_outcomes() {
    let service = TaskExecutionService::new().expect("service should be created");
    let succeeded = service
        .submit(Id::new(1), || Ok::<(), io::Error>(()))
        .expect("successful task should be accepted");
    let failed = service
        .submit(Id::new(2), || Err::<(), _>(io::Error::other("failed")))
        .expect("failing task should be accepted");
    let panicked = service
        .submit(Id::new(3), || -> Result<(), io::Error> { panic!("boom") })
        .expect("panicking task should be accepted");

    succeeded.get().expect("successful task should complete");
    assert!(matches!(failed.get(), Err(TaskExecutionError::Failed(_))));
    assert!(matches!(panicked.get(), Err(TaskExecutionError::Panicked)));

    let stats = service.stats();
    assert_eq!(stats.total, 3);
    assert_eq!(stats.succeeded, 1);
    assert_eq!(stats.failed, 1);
    assert_eq!(stats.panicked, 1);
    service.shutdown();
    service.wait_termination();
}

#[test]
fn test_task_execution_stats_count_only_retained_records() {
    let service = TaskExecutionService::builder()
        .completed_history_capacity(1)
        .build()
        .expect("service should be created");
    for id in 1..=2 {
        service
            .submit(Id::new(id), || Ok::<(), io::Error>(()))
            .expect("task should be accepted")
            .get()
            .expect("task should complete");
    }
    let stats = service.stats();
    assert_eq!(stats.total, 1);
    assert_eq!(stats.succeeded, 1);
    assert_eq!(
        stats.total,
        stats.submitted + stats.running + stats.succeeded + stats.failed + stats.panicked + stats.cancelled
    );
    assert_eq!(service.status(Id::new(1)), None);
    assert_eq!(service.status(Id::new(2)), Some(TaskStatus::Succeeded));
    service.shutdown();
    service.wait_termination();
}
