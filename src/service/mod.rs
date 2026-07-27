// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Managed task execution services.
//!
//! This module contains task-oriented services that assign stable task IDs and
//! track execution status while delegating actual concurrency to
//! `qubit-executor and qubit-thread-pool`.

mod task_execution_service;
mod task_execution_service_builder;
mod task_execution_service_error;
mod task_execution_stats;
mod task_id;
mod task_status;

pub use task_execution_service::TaskExecutionService;
pub use task_execution_service_builder::TaskExecutionServiceBuilder;
pub use task_execution_service_error::TaskExecutionServiceError;
pub use task_execution_stats::TaskExecutionStats;
pub use task_id::TaskId;
pub use task_status::TaskStatus;
