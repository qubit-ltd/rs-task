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
/// A barrier prevents the scheduler from considering tasks after the protected
/// task while it temporarily cannot start. The scheduler still handles a
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
    /// Candidate task identifiers in preferred order.
    pub order: Vec<TaskId>,
    /// Task that stops the scheduler from scanning later candidates while it
    /// remains temporarily unable to start.
    pub barrier: Option<TaskId>,
}
