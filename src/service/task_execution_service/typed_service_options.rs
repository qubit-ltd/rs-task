// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
#[cfg(feature = "event-bus")]
use std::sync::Arc;
#[cfg(feature = "event-bus")]
use std::time::Duration;

#[cfg(feature = "event-bus")]
use qubit_event_bus::AsyncEventBus;

use crate::service::RetryPolicy;

/// Runtime limits and optional notification settings for the typed service.
pub(in crate::service) struct TypedServiceOptions {
    /// Maximum number of concurrent handler attempts.
    pub(in crate::service) max_running_tasks: usize,
    /// Maximum number of summaries scanned in one scheduler page.
    pub(in crate::service) scan_page_size: usize,
    /// Maximum successful starts allowed to bypass a resource-waiting task.
    pub(in crate::service) max_resource_bypasses: usize,
    /// Maximum number of attempts for retryable failures.
    pub(in crate::service) max_attempts: u32,
    /// Delay policy for retryable failures.
    pub(in crate::service) retry_policy: RetryPolicy,
    /// Optional durable lifecycle event bus.
    #[cfg(feature = "event-bus")]
    pub(in crate::service) event_bus: Option<Arc<AsyncEventBus>>,
    /// Maximum time to drain notifications during shutdown.
    #[cfg(feature = "event-bus")]
    pub(in crate::service) notification_shutdown_timeout: Duration,
}
