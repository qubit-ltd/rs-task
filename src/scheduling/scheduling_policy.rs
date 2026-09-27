// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use super::QueueSnapshot;
use super::SchedulingPlan;
use crate::model::ResourceSnapshot;

/// Ordering strategy extension point; implementations only select candidate
/// IDs and may establish a barrier to preserve fairness.
///
/// The policy receives a bounded queue snapshot and current resource usage. It
/// does not mutate task state or reserve resources; the execution engine makes
/// the final atomic reservation decision.
///
/// # Examples
///
/// ```
/// use qubit_task::model::ResourceSnapshot;
/// use qubit_task::scheduling::FairFifoPolicy;
/// use qubit_task::scheduling::QueueSnapshot;
/// use qubit_task::scheduling::SchedulingPolicy;
///
/// let policy = FairFifoPolicy::default();
/// let plan = policy.order(&QueueSnapshot::default(), &ResourceSnapshot::default());
/// assert!(plan.order.is_empty());
/// assert!(plan.barrier.is_none());
/// ```
pub trait SchedulingPolicy: Send + Sync {
    /// Returns candidate IDs in preferred order and an optional fairness
    /// barrier without changing task state.
    ///
    /// # Parameters
    ///
    /// * `queue` - Bounded eligible queue snapshot.
    /// * `resources` - Current capacity and resource reservations.
    ///
    /// # Returns
    ///
    /// A candidate order. If `barrier` is set, the scheduler must stop scanning
    /// later candidates while that task is temporarily unable to start.
    #[must_use]
    fn order(&self, queue: &QueueSnapshot, resources: &ResourceSnapshot) -> SchedulingPlan;
}
