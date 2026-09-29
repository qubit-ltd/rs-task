// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Atomic outcome counters shared with the event publication worker.

use std::sync::atomic::AtomicU64;

/// Atomic counters shared between the service thread and publisher worker.
#[derive(Default)]
pub(in crate::service::task_event_publisher) struct Counters {
    /// Events accepted into the bounded queue.
    pub(in crate::service::task_event_publisher) enqueued: AtomicU64,
    /// Events rejected because the queue was full.
    pub(in crate::service::task_event_publisher) queue_full: AtomicU64,
    /// Events rejected because the queue was closed.
    pub(in crate::service::task_event_publisher) queue_closed: AtomicU64,
    /// Events accepted by at least one reported destination.
    pub(in crate::service::task_event_publisher) accepted: AtomicU64,
    /// Events accepted by providers without destination details.
    pub(in crate::service::task_event_publisher) opaque_accepted: AtomicU64,
    /// Events without an accepting destination.
    pub(in crate::service::task_event_publisher) unaccepted: AtomicU64,
    /// Events with mixed accepted and rejected destinations.
    pub(in crate::service::task_event_publisher) partial_rejection: AtomicU64,
    /// Failed publish calls whose provider effect may already have occurred.
    pub(in crate::service::task_event_publisher) uncertain_publish: AtomicU64,
    /// Failed publish calls.
    pub(in crate::service::task_event_publisher) publish_error: AtomicU64,
}
