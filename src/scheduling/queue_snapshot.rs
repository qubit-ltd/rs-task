// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use super::QueuedTask;

/// A bounded view of tasks eligible for scheduling.
///
/// # Examples
///
/// ```
/// use qubit_task::scheduling::QueueSnapshot;
///
/// let queue = QueueSnapshot::default();
/// assert!(queue.tasks.is_empty());
/// ```
#[derive(Debug, Clone, Default)]
pub struct QueueSnapshot {
    /// Tasks in accepted order with their bypass history.
    pub tasks: Vec<QueuedTask>,
    /// Maximum number of candidates the scheduler may inspect this cycle.
    pub scan_budget: usize,
}
