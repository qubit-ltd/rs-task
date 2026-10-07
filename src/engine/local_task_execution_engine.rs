// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
// Owns private resource-accounting and reservation-guard types.
mod internal;

use std::sync::Arc;

use internal::ResourceLedger;
use internal::release_reservation;
use parking_lot::Mutex;

use crate::engine::EngineError;
use crate::model::ResourceCapacity;

/// Single-process executor that atomically accounts for CPU, GPU, and custom
/// resources.
pub struct LocalTaskExecutionEngine {
    /// Total available resources.
    capacity: ResourceCapacity,
    /// Current totals and reservations, updated under one lock.
    ledger: Arc<Mutex<ResourceLedger>>,
}

impl LocalTaskExecutionEngine {
    /// Creates a local engine with explicit resource capacity.
    ///
    /// # Parameters
    ///
    /// * `capacity` - Maximum CPU, GPU, and custom resource capacity.
    ///
    /// # Returns
    ///
    /// A local engine with no resources reserved.
    #[must_use]
    pub fn new(capacity: ResourceCapacity) -> Self {
        Self {
            capacity,
            ledger: Arc::new(Mutex::new(ResourceLedger {
                next_token: Some(1),
                ..ResourceLedger::default()
            })),
        }
    }

    /// Reserves a typed task's resources in the same ledger as legacy tasks.
    pub fn try_prepare_typed(
        &self,
        _id: crate::model::next::TaskId,
        request: crate::model::next::ResourceRequest,
    ) -> Result<crate::engine::TypedResourceReservation, EngineError> {
        let matching_gpu_capacity = self
            .capacity
            .gpus
            .values()
            .filter(|labels| {
                request
                    .gpu_labels
                    .iter()
                    .all(|label| labels.contains(label))
            })
            .count();
        if request.cpu_slots > self.capacity.cpu_slots
            || request.gpu_count as usize > matching_gpu_capacity
            || request
                .memory_bytes
                .is_some_and(|value| value > self.capacity.memory_bytes.unwrap_or(0))
            || request
                .disk_bytes
                .is_some_and(|value| value > self.capacity.disk_bytes.unwrap_or(0))
            || request.custom.iter().any(|(name, value)| {
                self.capacity
                    .custom
                    .get(name)
                    .is_none_or(|limit| value > limit)
            })
        {
            return Err(EngineError::Unsatisfiable);
        }
        let mut ledger = self.ledger.lock();
        let available_gpus = self
            .capacity
            .gpus
            .iter()
            .filter(|(id, labels)| {
                !ledger.usage.gpus.contains(id)
                    && request
                        .gpu_labels
                        .iter()
                        .all(|label| labels.contains(label))
            })
            .map(|(id, _)| id.clone())
            .take(request.gpu_count as usize)
            .collect::<Vec<_>>();
        let available_custom = request.custom.iter().all(|(name, value)| {
            ledger
                .usage
                .custom
                .get(name)
                .copied()
                .unwrap_or(0)
                .checked_add(*value)
                .is_some_and(|total| total <= self.capacity.custom.get(name).copied().unwrap_or(0))
        });
        let memory = request.memory_bytes.unwrap_or(0);
        let disk = request.disk_bytes.unwrap_or(0);
        let available = ledger
            .usage
            .cpu
            .checked_add(request.cpu_slots)
            .is_some_and(|total| total <= self.capacity.cpu_slots)
            && ledger
                .usage
                .memory
                .checked_add(memory)
                .is_some_and(|total| total <= self.capacity.memory_bytes.unwrap_or(0))
            && ledger
                .usage
                .disk
                .checked_add(disk)
                .is_some_and(|total| total <= self.capacity.disk_bytes.unwrap_or(0))
            && available_gpus.len() == request.gpu_count as usize
            && available_custom;
        if !available {
            return Err(EngineError::TemporarilyUnavailable);
        }
        let Some(token) = ledger.next_token else {
            return Err(EngineError::ReservationTokenExhausted);
        };
        let Some(next_token) = token.checked_add(1) else {
            return Err(EngineError::ReservationTokenExhausted);
        };
        ledger.next_token = Some(next_token);
        ledger.usage.cpu += request.cpu_slots;
        ledger.usage.memory += memory;
        ledger.usage.disk += disk;
        ledger.usage.gpus.extend(available_gpus.clone());
        for (name, value) in &request.custom {
            *ledger.usage.custom.entry(name.clone()).or_default() += value;
        }
        ledger.allocations.insert(
            token,
            (
                request.cpu_slots,
                memory,
                disk,
                available_gpus,
                request.custom,
            ),
        );
        drop(ledger);
        let ledger = Arc::clone(&self.ledger);
        Ok(crate::engine::TypedResourceReservation::new(Box::new(
            move || release_reservation(token, &ledger),
        )))
    }
}
