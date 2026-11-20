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
mod notification_failure;
#[cfg(feature = "event-bus")]
mod notification_stats;
mod owner_release_guard;
#[cfg(test)]
mod owner_release_guard_tests;
mod retry_policy;
mod retry_policy_error;
#[cfg(feature = "event-bus")]
mod task_event_publisher;
mod task_execution_service;
mod task_execution_service_builder;
mod task_pages;
mod task_progress_error;
mod task_progress_ext;
mod task_progress_reporter;
mod task_route_error;
mod task_service_error;
mod wait_task_error;

pub use cancel_outcome::CancelOutcome;
#[cfg(feature = "event-bus")]
pub use notification_failure::NotificationFailure;
#[cfg(feature = "event-bus")]
pub use notification_stats::NotificationStats;
pub use retry_policy::RetryPolicy;
pub use retry_policy_error::RetryPolicyError;
pub use task_execution_service::TaskExecutionService;
pub use task_execution_service_builder::TaskExecutionServiceBuilder;
pub use task_pages::TaskPages;
pub use task_progress_error::TaskProgressError;
pub use task_progress_ext::TaskProgressExt;
pub use task_route_error::TaskRouteError;
pub use task_service_error::TaskServiceError;
pub use wait_task_error::WaitTaskError;
