// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use super::QueueSnapshot;
use super::QueuedTask;
use super::SchedulingPlan;
use super::SchedulingPolicy;
use crate::model::ResourceCapacity;
use crate::model::ResourceSnapshot;

/// FIFO-first scheduler with bounded bypass and a configurable starvation
/// limit.
///
/// # Examples
///
/// ```
/// use qubit_task::model::ResourceSnapshot;
/// use qubit_task::scheduling::QueueSnapshot;
/// use qubit_task::scheduling::SchedulingPolicy;
/// use qubit_task::scheduling::FairFifoPolicy;
///
/// let policy = FairFifoPolicy::new(8);
/// let plan = policy.order(&QueueSnapshot::default(), &ResourceSnapshot::default());
/// assert!(plan.order.is_empty());
/// assert!(plan.barrier.is_none());
/// ```
pub struct FairFifoPolicy {
    /// Successful bypass count after which a head task is protected.
    max_bypasses: u32,
}

impl FairFifoPolicy {
    /// Creates a policy that stops bypassing a head task after `max_bypasses`.
    ///
    /// # Parameters
    ///
    /// * `max_bypasses` - Maximum number of successful bypasses before the
    ///   earliest protected task is selected alone.
    ///
    /// # Returns
    ///
    /// A FIFO-first policy with the requested bypass limit.
    #[must_use]
    pub fn new(max_bypasses: u32) -> Self {
        Self { max_bypasses }
    }
}

impl Default for FairFifoPolicy {
    /// Creates a policy that permits eight successful bypasses.
    ///
    /// # Returns
    ///
    /// A policy with a bypass limit of eight.
    fn default() -> Self {
        Self::new(8)
    }
}

impl SchedulingPolicy for FairFifoPolicy {
    /// Orders eligible tasks, protecting the earliest task at its bypass limit.
    ///
    /// # Parameters
    ///
    /// * `queue` - Bounded queue snapshot and policy scan limit.
    /// * `resources` - Current total capacity and resource usage.
    ///
    /// # Returns
    ///
    /// Candidate IDs in preferred order with an optional fairness barrier. A
    /// task that cannot fit current usage but can fit total capacity remains
    /// queued; a task that can never fit total capacity is returned so the
    /// service can block it.
    fn order(&self, queue: &QueueSnapshot, resources: &ResourceSnapshot) -> SchedulingPlan {
        let candidates = queue.tasks.iter().take(queue.scan_budget.max(1)).collect::<Vec<_>>();
        let protected = candidates.iter().find(|task| task.bypasses >= self.max_bypasses);
        if let Some(task) = protected {
            return SchedulingPlan {
                order: vec![task.id],
                barrier: Some(task.id),
            };
        }
        let mut result = Vec::with_capacity(candidates.len());
        result.extend(
            candidates
                .iter()
                .filter(|task| likely_fits(task, resources))
                .map(|task| task.id),
        );
        result.extend(
            candidates
                .iter()
                .filter(|task| !can_fit_capacity(task, &resources.capacity))
                .map(|task| task.id),
        );
        SchedulingPlan {
            order: result,
            barrier: None,
        }
    }
}

/// Reports whether configured capacity can ever satisfy the task's request.
///
/// # Parameters
///
/// * `task` - Candidate task to evaluate.
/// * `capacity` - Configured total resource capacity.
///
/// # Returns
///
/// Whether the request can fit the configured capacity.
fn can_fit_capacity(task: &QueuedTask, capacity: &ResourceCapacity) -> bool {
    let request = &task.resources;
    request.cpu_slots <= capacity.cpu_slots
        && capacity
            .gpus
            .values()
            .filter(|labels| request.gpu_labels.iter().all(|label| labels.contains(label)))
            .count()
            >= request.gpu_count as usize
        && request
            .custom
            .iter()
            .all(|(name, amount)| capacity.custom.get(name).is_some_and(|limit| amount <= limit))
}

/// Reports whether currently unreserved resources appear sufficient for a task.
///
/// # Parameters
///
/// * `task` - Candidate task to evaluate.
/// * `resources` - Capacity and current reservations.
///
/// # Returns
///
/// Whether the request appears to fit the currently available resources.
fn likely_fits(task: &QueuedTask, resources: &ResourceSnapshot) -> bool {
    let request = &task.resources;
    request.cpu_slots <= resources.capacity.cpu_slots.saturating_sub(resources.used_cpu_slots)
        && resources
            .capacity
            .gpus
            .iter()
            .filter(|(id, labels)| {
                !resources.used_gpus.contains(id) && request.gpu_labels.iter().all(|label| labels.contains(label))
            })
            .count()
            >= request.gpu_count as usize
        && request.custom.iter().all(|(name, amount)| {
            resources
                .capacity
                .custom
                .get(name)
                .copied()
                .unwrap_or(0)
                .saturating_sub(resources.used_custom.get(name).copied().unwrap_or(0))
                >= *amount
        })
}
