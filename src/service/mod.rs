// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Unified task submission, status, scheduling, and lifecycle facade.

mod admission_gate;
mod local_task_handle;
#[cfg(feature = "event-bus")]
mod task_event_notification_stats;
#[cfg(feature = "event-bus")]
mod task_event_publisher;
mod task_execution_service;
mod task_execution_service_builder;

#[cfg(feature = "event-bus")]
pub use task_event_notification_stats::TaskEventNotificationStats;
pub use task_execution_service::CancelOutcome;
pub use task_execution_service::TaskExecutionService;
pub use task_execution_service::TaskServiceCapabilities;
pub use task_execution_service::TaskServiceError;
pub use task_execution_service_builder::TaskExecutionServiceBuilder;
pub use task_execution_service_builder::TaskServiceBuildError;
pub use local_task_handle::LocalTaskHandle;
pub use local_task_handle::LocalTaskOutcome;
pub use local_task_handle::LocalTaskResultError;
