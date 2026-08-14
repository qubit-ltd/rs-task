// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for [`TaskExecutionServiceError`](qubit_task::service::TaskExecutionServiceError).

use std::io;

use qubit_task::service::TaskExecutionService;
use qubit_task::service::TaskExecutionServiceError;

/// Returns a successful unit task result.
fn successful_unit_task() -> Result<(), io::Error> {
    Ok(())
}

#[test]
fn test_task_execution_service_error_formats_duplicate_task() {
    let error = TaskExecutionServiceError::DuplicateTask(17);

    assert_eq!(error.to_string(), "task 17 already exists");
}

#[test]
fn test_task_execution_service_error_formats_suspended_service() {
    let error = TaskExecutionServiceError::Suspended;

    assert_eq!(error.to_string(), "task execution service is suspended");
}

#[test]
fn test_task_execution_service_error_reports_rejected_submission() {
    let service =
        TaskExecutionService::new().expect("service should be created");
    service.shutdown();

    let error = match service
        .submit(1, successful_unit_task as fn() -> Result<(), io::Error>)
    {
        Ok(_) => panic!("shutdown service should reject new tasks"),
        Err(error) => error,
    };

    assert!(matches!(error, TaskExecutionServiceError::Rejected(_)));
    assert!(!error.to_string().is_empty());
    service.wait_termination();
}
