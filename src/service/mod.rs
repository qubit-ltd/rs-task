// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Unified task submission, status, scheduling, and lifecycle facade.

mod task_execution_service;
mod task_execution_service_builder;

pub use task_execution_service::CancelOutcome;
pub use task_execution_service::TaskExecutionService;
pub use task_execution_service::TaskServiceCapabilities;
pub use task_execution_service::TaskServiceError;
pub use task_execution_service_builder::TaskExecutionServiceBuilder;
pub use task_execution_service_builder::TaskServiceBuildError;
