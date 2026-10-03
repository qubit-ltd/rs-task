// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Process-local outcome counters for durable lifecycle publication.

use std::sync::atomic::AtomicU64;

use crate::service::NotificationStats;

#[derive(Default)]
pub(in crate::service::task_event_publisher) struct Counters {
    pub(in crate::service::task_event_publisher) queued: AtomicU64,
    pub(in crate::service::task_event_publisher) published: AtomicU64,
    pub(in crate::service::task_event_publisher) failed: AtomicU64,
}

impl Counters {
    pub(in crate::service::task_event_publisher) fn snapshot(&self) -> NotificationStats {
        use std::sync::atomic::Ordering;

        NotificationStats {
            queued: self.queued.load(Ordering::Relaxed),
            published: self.published.load(Ordering::Relaxed),
            dropped: 0,
            failed: self.failed.load(Ordering::Relaxed),
        }
    }
}
