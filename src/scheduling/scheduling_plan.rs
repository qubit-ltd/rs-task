// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use crate::model::TaskId;

/// Candidate order and optional fairness barrier returned by a scheduling
/// policy.
///
/// `order` contains unique task identifiers from the queue snapshot supplied
/// to the policy. When `barrier` is set, that identifier must occur in `order`.
/// The scheduler may start candidates before it, but stops considering later
/// candidates when the barrier is temporarily unavailable. It still handles a
/// barrier task that has become terminal, blocked, or permanently
/// unsatisfiable.
///
/// # Examples
///
/// ```
/// use qubit_task::scheduling::SchedulingPlan;
///
/// let plan = SchedulingPlan::default();
/// assert!(plan.order.is_empty());
/// assert!(plan.barrier.is_none());
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SchedulingPlan {
    /// Unique candidate identifiers from the supplied queue snapshot, in
    /// preferred order.
    pub order: Vec<TaskId>,
    /// Candidate that stops the scheduler from scanning later candidates while
    /// it remains temporarily unable to start. It must appear in `order`.
    pub barrier: Option<TaskId>,
}
