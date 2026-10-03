// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
/// Process-local counters for task lifecycle notifications.
///
/// `queued` counts lifecycle writes signalled to the publisher during this
/// service process. Durable backlog remains in SQLite and can exceed this
/// value after restart. `dropped` remains zero because the outbox never drops
/// a committed event.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NotificationStats {
    pub queued: u64,
    pub published: u64,
    pub dropped: u64,
    pub failed: u64,
}
