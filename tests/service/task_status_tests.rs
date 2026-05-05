/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Tests for [`TaskStatus`](qubit_task::service::TaskStatus).

use qubit_task::service::TaskStatus;

#[test]
fn test_task_status_is_active_for_in_flight_states() {
    assert!(TaskStatus::Submitted.is_active());
    assert!(TaskStatus::Running.is_active());
}

#[test]
fn test_task_status_is_not_active_for_terminal_states() {
    assert!(!TaskStatus::Succeeded.is_active());
    assert!(!TaskStatus::Failed.is_active());
    assert!(!TaskStatus::Panicked.is_active());
    assert!(!TaskStatus::Cancelled.is_active());
}
