// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;

use tokio::sync::Notify;

/// Notification primitive and subscriber count for one task ID.
pub(in crate::service::task_wait_registry) struct Entry {
    /// Shared wakeup source for callers waiting on this task.
    pub(in crate::service::task_wait_registry) notify: Arc<Notify>,
    /// Number of live subscriptions keeping this entry registered.
    pub(in crate::service::task_wait_registry) subscribers: usize,
}
