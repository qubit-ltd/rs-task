// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Aggregate task counts suitable for service monitoring.
///
/// Terminal counts include every retained succeeded, failed, panicked, and
/// cancelled record.
///
/// # Examples
///
/// ```
/// use qubit_task::model::TaskStateCounts;
///
/// let counts = TaskStateCounts::default();
/// assert_eq!(counts.queued + counts.running + counts.blocked + counts.terminal, 0);
/// ```
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct TaskStateCounts {
    /// Number of retained tasks waiting for resources.
    pub queued: usize,
    /// Number of retained tasks currently executing.
    pub running: usize,
    /// Number of retained tasks requiring intervention.
    pub blocked: usize,
    /// Number of retained terminal task records.
    pub terminal: usize,
}
