// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Unified task submission, status, scheduling, and lifecycle facade.

#[cfg(test)]
mod admission_budget;
#[cfg(test)]
mod admission_gate;
mod cancel_outcome;
#[cfg(test)]
mod local_task_handle;
#[cfg(test)]
mod local_task_outcome;
#[cfg(test)]
mod local_task_result_error;
#[cfg(test)]
mod retry_policy;
#[cfg(test)]
mod retry_policy_error;
#[cfg(test)]
mod scheduler_queue;
#[cfg(all(feature = "event-bus", test))]
mod task_event_notification_stats;
#[cfg(all(feature = "event-bus", test))]
mod task_event_publisher;
#[cfg(test)]
mod task_execution_service;
#[cfg(test)]
mod task_execution_service_builder;
#[cfg(not(test))]
mod task_progress_reporter;
#[cfg(test)]
mod task_service_build_error;
#[cfg(test)]
mod task_service_capabilities;
mod task_service_error;
#[cfg(test)]
mod task_wait_registry;
#[cfg(not(test))]
mod typed_task_execution_service;
#[cfg(not(test))]
mod typed_task_execution_service_builder;

#[cfg(test)]
mod legacy_integration_tests;

pub use cancel_outcome::CancelOutcome;
#[cfg(test)]
pub use local_task_handle::LocalTaskHandle;
#[cfg(test)]
pub use local_task_outcome::LocalTaskOutcome;
#[cfg(test)]
pub use local_task_result_error::LocalTaskResultError;
#[cfg(test)]
pub use retry_policy::RetryPolicy;
#[cfg(test)]
pub use retry_policy_error::RetryPolicyError;
#[cfg(all(feature = "event-bus", test))]
pub use task_event_notification_stats::TaskEventNotificationStats;
#[cfg(test)]
pub(crate) use task_execution_service::TaskExecutionService;
#[cfg(test)]
pub(crate) use task_execution_service_builder::TaskExecutionServiceBuilder;
#[cfg(test)]
pub use task_service_build_error::TaskServiceBuildError;
#[cfg(test)]
pub use task_service_capabilities::TaskServiceCapabilities;
pub use task_service_error::TaskServiceError;
#[cfg(not(test))]
pub use typed_task_execution_service::TypedTaskExecutionService as TaskExecutionService;
#[cfg(not(test))]
pub use typed_task_execution_service_builder::TypedTaskExecutionServiceBuilder as TaskExecutionServiceBuilder;
