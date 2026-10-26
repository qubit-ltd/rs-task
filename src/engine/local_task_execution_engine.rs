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

use parking_lot::Mutex;

use self::internal::ResourceLedger;
use self::internal::release_reservation;
use crate::engine::EngineError;
use crate::engine::TypedResourceReservation;
use crate::model::ResourceCapacity;
use crate::model::typed::ResourceRequest;
use crate::model::typed::TaskId;

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
    #[inline]
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
    ///
    /// The task identifier is accepted for API consistency but is not stored by
    /// this local engine. Capacity that cannot ever satisfy the request is
    /// reported separately from capacity that is currently reserved.
    ///
    /// # Parameters
    ///
    /// * `_id` - Identifier of the task requesting the reservation.
    /// * `request` - CPU, GPU, memory, disk, and custom resource requirements.
    ///
    /// # Returns
    ///
    /// A reservation guard that releases the accounted resources when dropped.
    ///
    /// # Errors
    ///
    /// Returns [`EngineError::Unsatisfiable`] when the configured capacity can
    /// never satisfy the request, [`EngineError::TemporarilyUnavailable`] when
    /// currently reserved resources prevent admission, or
    /// [`EngineError::ReservationTokenExhausted`] when no reservation token is
    /// available.
    pub fn try_prepare_typed(
        &self,
        _id: TaskId,
        request: ResourceRequest,
    ) -> Result<TypedResourceReservation, EngineError> {
        let matching_gpu_capacity = self
            .capacity
            .gpus
            .values()
            .filter(|labels| request.gpu_labels.iter().all(|label| labels.contains(label)))
            .count();
        if request.cpu_slots > self.capacity.cpu_slots
            || request.gpu_count as usize > matching_gpu_capacity
            || request
                .memory_bytes
                .is_some_and(|value| value > self.capacity.memory_bytes.unwrap_or(0))
            || request
                .disk_bytes
                .is_some_and(|value| value > self.capacity.disk_bytes.unwrap_or(0))
            || request
                .custom
                .iter()
                .any(|(name, value)| self.capacity.custom.get(name).is_none_or(|limit| value > limit))
        {
            return Err(EngineError::Unsatisfiable);
        }
        let mut ledger = self.ledger.lock();
        let available_gpus = self
            .capacity
            .gpus
            .iter()
            .filter(|(id, labels)| {
                !ledger.usage.gpus.contains(id) && request.gpu_labels.iter().all(|label| labels.contains(label))
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
        ledger
            .allocations
            .insert(token, (request.cpu_slots, memory, disk, available_gpus, request.custom));
        drop(ledger);
        let ledger = Arc::clone(&self.ledger);
        Ok(TypedResourceReservation::new(Box::new(move || {
            release_reservation(token, &ledger)
        })))
    }
}

#[cfg(test)]
mod tests {
    use super::LocalTaskExecutionEngine;
    use crate::engine::EngineError;
    use crate::model::ResourceCapacity;
    use crate::model::typed::ResourceRequest;
    use crate::model::typed::TaskId;

    fn task_id(value: u64) -> TaskId {
        TaskId::from_id(qubit_id::Id::new(value))
    }

    fn request() -> ResourceRequest {
        ResourceRequest::default()
    }

    #[test]
    fn test_exhausted_reservation_token_states_return_the_dedicated_error() {
        for next_token in [None, Some(u64::MAX)] {
            let engine = LocalTaskExecutionEngine::new(ResourceCapacity {
                cpu_slots: 1,
                ..ResourceCapacity::default()
            });
            engine.ledger.lock().next_token = next_token;

            assert!(matches!(
                engine.try_prepare_typed(task_id(30), request()),
                Err(EngineError::ReservationTokenExhausted)
            ));
            assert_eq!(engine.ledger.lock().usage.cpu, 0);
            assert!(engine.ledger.lock().allocations.is_empty());
        }
    }
}
