// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
/// Counts task lifecycle notifications queued for delivery.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NotificationStats {
    pub queued: u64,
    pub published: u64,
    pub dropped: u64,
    pub failed: u64,
}
