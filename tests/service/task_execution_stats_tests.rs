/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Tests for [`TaskExecutionStats`](qubit_task::service::TaskExecutionStats).

use std::io;

use qubit_executor::TaskExecutionError;
use qubit_task::service::{TaskExecutionService, TaskExecutionStats};

/// Creates a current-thread Tokio runtime for driving async termination APIs.
fn create_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime for stats tests")
}

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
        .submit(1, || Ok::<(), io::Error>(()))
        .expect("successful task should be accepted");
    let failed = service
        .submit(2, || Err::<(), _>(io::Error::other("failed")))
        .expect("failing task should be accepted");
    let panicked = service
        .submit(3, || -> Result<(), io::Error> { panic!("boom") })
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
    create_runtime().block_on(service.await_termination());
}
