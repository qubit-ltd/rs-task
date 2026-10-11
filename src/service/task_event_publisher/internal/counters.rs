// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Process-local outcome counters for durable lifecycle publication.

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use crate::service::NotificationStats;

/// Tracks lifecycle publication outcomes within the current process.
///
/// These counters are observational only; durable event state remains in the
/// outbox. Relaxed atomic ordering is sufficient because the counters do not
/// synchronize access to other data.
#[derive(Default)]
pub(in crate::service::task_event_publisher) struct Counters {
    /// Number of lifecycle writes signalled to the publisher in this process.
    pub(in crate::service::task_event_publisher) queued: AtomicU64,
    /// Number of events accepted by the bus and removed from the outbox.
    pub(in crate::service::task_event_publisher) published: AtomicU64,
    /// Number of publication or outbox operations that failed in this process.
    pub(in crate::service::task_event_publisher) failed: AtomicU64,
}

impl Counters {
    /// Reads the current process-local totals for the public statistics view.
    ///
    /// Each atomic is loaded independently, so the returned fields need not
    /// represent one simultaneous instant while updates are in progress.
    pub(in crate::service::task_event_publisher) fn snapshot(&self) -> NotificationStats {
        NotificationStats {
            queued: self.queued.load(Ordering::Relaxed),
            published: self.published.load(Ordering::Relaxed),
            dropped: 0,
            failed: self.failed.load(Ordering::Relaxed),
        }
    }
}
