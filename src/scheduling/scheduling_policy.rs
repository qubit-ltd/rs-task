// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use super::QueueSnapshot;
use crate::model::ResourceSnapshot;
use crate::model::TaskId;

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
/// use qubit_task::model::ResourceSnapshot;
/// use qubit_task::scheduling::FairFifoPolicy;
/// use qubit_task::scheduling::QueueSnapshot;
/// use qubit_task::scheduling::SchedulingPolicy;
///
/// let policy = FairFifoPolicy::default();
/// let ordered = policy.order(&QueueSnapshot::default(), &ResourceSnapshot::default());
/// assert!(ordered.is_empty());
/// ```
pub trait SchedulingPolicy: Send + Sync {
    /// Returns candidate IDs in preferred order without changing task state.
    ///
    /// # Parameters
    ///
    /// * `queue` - Bounded eligible queue snapshot.
    /// * `resources` - Current capacity and resource reservations.
    ///
    /// # Returns
    ///
    /// Task identifiers in preferred scheduling order.
    #[must_use]
    fn order(&self, queue: &QueueSnapshot, resources: &ResourceSnapshot) -> Vec<TaskId>;
}
