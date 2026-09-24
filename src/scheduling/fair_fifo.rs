// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use super::QueueSnapshot;
use super::SchedulingPolicy;
use crate::model::ResourceCapacity;
use crate::model::ResourceSnapshot;
use crate::model::TaskId;
use crate::model::TaskRequest;

/// Queue entry metadata needed to keep a frequently bypassed request
/// progressing.
#[derive(Debug, Clone)]
pub struct QueuedTask {
    /// Stable task identity.
    pub id: TaskId,
    /// Resource requirements used to determine likely fit.
    pub request: TaskRequest,
    /// Number of scheduling cycles in which a later task started first.
    pub bypasses: u32,
}

/// FIFO-first scheduler with bounded bypass and a configurable starvation
/// limit.
pub struct FairFifoPolicy {
    max_bypasses: u32,
}

impl FairFifoPolicy {
    /// Creates a policy that stops bypassing a head task after `max_bypasses`.
    #[must_use]
    pub fn new(max_bypasses: u32) -> Self {
        Self { max_bypasses }
    }
}

impl Default for FairFifoPolicy {
    fn default() -> Self {
        Self::new(8)
    }
}

impl SchedulingPolicy for FairFifoPolicy {
    fn order(&self, queue: &QueueSnapshot, resources: &ResourceSnapshot) -> Vec<TaskId> {
        let candidates = queue.tasks.iter().take(queue.scan_budget.max(1)).collect::<Vec<_>>();
        let protected = candidates.iter().find(|task| task.bypasses >= self.max_bypasses);
        if let Some(task) = protected {
            return vec![task.id];
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
        result
    }
}

fn can_fit_capacity(task: &QueuedTask, capacity: &ResourceCapacity) -> bool {
    let request = &task.request.resources;
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

fn likely_fits(task: &QueuedTask, resources: &ResourceSnapshot) -> bool {
    let request = &task.request.resources;
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
