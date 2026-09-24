// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Admission counters for optional task lifecycle notifications.

/// Publication admission counters. Acceptance reports enqueue or provider
/// admission only; it does not report subscriber handler completion.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct TaskEventNotificationStats {
    /// Events placed in the publisher's bounded queue.
    pub enqueued: u64,
    /// Events dropped because the queue was full.
    pub queue_full: u64,
    /// Events dropped after the queue closed or its worker exited.
    pub queue_closed: u64,
    /// Events accepted by at least one reported destination.
    pub accepted: u64,
    /// Events accepted by a provider that does not expose destinations.
    pub opaque_accepted: u64,
    /// Events with no reported accepting destination.
    pub unaccepted: u64,
    /// Events with both accepted and rejected destinations.
    pub partial_rejection: u64,
    /// Calls to the event bus that returned an error.
    pub publish_error: u64,
    /// Publisher worker panics.
    pub worker_panicked: u64,
}
