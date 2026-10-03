// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Unified task submission, status, scheduling, and lifecycle facade.

mod cancel_outcome;
#[cfg(feature = "event-bus")]
mod notification_stats;
mod owner_release_guard;
mod retry_policy;
mod retry_policy_error;
#[cfg(feature = "event-bus")]
mod task_event_publisher;
mod task_progress_reporter;
mod task_service_error;
mod typed_task_execution_service;
mod typed_task_execution_service_builder;

pub use cancel_outcome::CancelOutcome;
#[cfg(feature = "event-bus")]
pub use notification_stats::NotificationStats;
pub use retry_policy::RetryPolicy;
pub use retry_policy_error::RetryPolicyError;
pub use task_service_error::TaskServiceError;
pub use typed_task_execution_service::TypedTaskExecutionService as TaskExecutionService;
pub use typed_task_execution_service_builder::TypedTaskExecutionServiceBuilder as TaskExecutionServiceBuilder;
