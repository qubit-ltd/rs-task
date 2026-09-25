// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Pluggable ordering policies for resource-constrained task queues.

mod fair_fifo;

pub use fair_fifo::FairFifoPolicy;
pub use fair_fifo::QueuedTask;

use crate::model::ResourceSnapshot;
use crate::model::TaskId;
use crate::store::TaskFuture;

/// A bounded view of tasks eligible for scheduling.
#[derive(Debug, Clone, Default)]
pub struct QueueSnapshot {
    /// Tasks in accepted order with their bypass history.
    pub tasks: Vec<QueuedTask>,
    /// Maximum number of candidates the scheduler may inspect this cycle.
    pub scan_budget: usize,
}

/// Ordering strategy extension point; implementations only select candidate
/// IDs.
///
/// The policy receives a bounded queue snapshot and current resource usage. It
/// does not mutate task state or reserve resources; the execution engine makes
/// the final atomic reservation decision.
///
/// # Examples
///
/// ```
/// use qubit_task::scheduling::{FairFifoPolicy, QueueSnapshot, SchedulingPolicy};
/// use qubit_task::model::ResourceSnapshot;
///
/// let policy = FairFifoPolicy::default();
/// let ordered = policy.order(&QueueSnapshot::default(), &ResourceSnapshot::default());
/// assert!(ordered.is_empty());
/// ```
pub trait SchedulingPolicy: Send + Sync {
    /// Returns candidate IDs in preferred order without changing task state.
    fn order(&self, queue: &QueueSnapshot, resources: &ResourceSnapshot) -> Vec<TaskId>;
}

/// Factory contract used by the application or SPI assembly.
pub trait SchedulingPolicyProvider: Send + Sync {
    /// Creates one scheduling policy instance.
    fn create(&self) -> Result<std::sync::Arc<dyn SchedulingPolicy>, String>;
}

/// Async unit type alias retained for component factory uniformity.
pub type SchedulingFuture<'a, T> = TaskFuture<'a, T>;
