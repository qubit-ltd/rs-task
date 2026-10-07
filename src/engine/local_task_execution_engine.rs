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
        Ok(crate::engine::TypedResourceReservation::new(Box::new(move || {
            release_reservation(token, &ledger)
        })))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::LocalTaskExecutionEngine;
    use crate::engine::EngineError;
    use crate::model::ResourceCapacity;
    use crate::model::next::ResourceRequest;
    use crate::model::next::TaskId;

    fn task_id(value: u64) -> TaskId {
        TaskId::from_id(qubit_id::Id::new(value))
    }

    fn request() -> ResourceRequest {
        ResourceRequest::default()
    }

    #[test]
    fn exact_cpu_gpu_memory_disk_and_custom_capacity_can_be_reserved_and_reused() {
        let capacity = ResourceCapacity {
            cpu_slots: 2,
            memory_bytes: Some(64),
            disk_bytes: Some(128),
            gpus: BTreeMap::from([
                ("gpu-a".into(), vec!["cuda".into(), "large-memory".into()]),
                ("gpu-b".into(), vec!["cuda".into()]),
            ]),
            custom: BTreeMap::from([("license".into(), 3)]),
        };
        let engine = LocalTaskExecutionEngine::new(capacity);
        let request = ResourceRequest {
            cpu_slots: 2,
            gpu_count: 1,
            memory_bytes: Some(64),
            disk_bytes: Some(128),
            gpu_labels: vec!["large-memory".into()],
            custom: BTreeMap::from([("license".into(), 3)]),
        };

        let reservation = engine
            .try_prepare_typed(task_id(1), request.clone())
            .expect("request at every capacity boundary should be admitted");
        assert!(matches!(
            engine.try_prepare_typed(task_id(2), request.clone()),
            Err(EngineError::TemporarilyUnavailable)
        ));
        drop(reservation);
        assert!(engine.try_prepare_typed(task_id(3), request).is_ok());
    }

    #[test]
    fn impossible_requests_are_distinguished_from_temporarily_reserved_capacity() {
        let engine = LocalTaskExecutionEngine::new(ResourceCapacity {
            cpu_slots: 1,
            memory_bytes: Some(10),
            disk_bytes: Some(20),
            gpus: BTreeMap::from([("gpu-0".into(), vec!["cuda".into()])]),
            custom: BTreeMap::from([("license".into(), 1)]),
        });

        let mut impossible = request();
        impossible.cpu_slots = 2;
        assert!(matches!(
            engine.try_prepare_typed(task_id(1), impossible),
            Err(EngineError::Unsatisfiable)
        ));

        let mut unavailable = request();
        unavailable.cpu_slots = 1;
        let reservation = engine
            .try_prepare_typed(task_id(2), unavailable.clone())
            .expect("first request should reserve the only CPU slot");
        assert!(matches!(
            engine.try_prepare_typed(task_id(3), unavailable),
            Err(EngineError::TemporarilyUnavailable)
        ));
        drop(reservation);
    }

    #[test]
    fn typed_request_exceeding_each_resource_limit_is_unsatisfiable() {
        let engine = LocalTaskExecutionEngine::new(ResourceCapacity {
            cpu_slots: 2,
            memory_bytes: Some(10),
            disk_bytes: Some(20),
            gpus: BTreeMap::from([("gpu-0".into(), vec!["cuda".into()])]),
            custom: BTreeMap::from([("license".into(), 2)]),
        });
        let mut cases = Vec::new();

        let mut cpu = request();
        cpu.cpu_slots = 3;
        cases.push(cpu);

        let mut gpu_count = request();
        gpu_count.gpu_count = 2;
        cases.push(gpu_count);

        let mut gpu_label = request();
        gpu_label.gpu_count = 1;
        gpu_label.gpu_labels.push("rocm".into());
        cases.push(gpu_label);

        let mut memory = request();
        memory.memory_bytes = Some(11);
        cases.push(memory);

        let mut disk = request();
        disk.disk_bytes = Some(21);
        cases.push(disk);

        let mut unknown_custom = request();
        unknown_custom.custom.insert("missing".into(), 1);
        cases.push(unknown_custom);

        let mut excessive_custom = request();
        excessive_custom.custom.insert("license".into(), 3);
        cases.push(excessive_custom);

        for (index, request) in cases.into_iter().enumerate() {
            assert!(matches!(
                engine.try_prepare_typed(task_id(index as u64 + 10), request),
                Err(EngineError::Unsatisfiable)
            ));
        }
    }

    #[test]
    fn omitted_memory_and_disk_quotas_consume_no_capacity() {
        let engine = LocalTaskExecutionEngine::new(ResourceCapacity {
            cpu_slots: 1,
            memory_bytes: None,
            disk_bytes: None,
            ..ResourceCapacity::default()
        });
        assert!(engine.try_prepare_typed(task_id(20), request()).is_ok());
    }

    #[test]
    fn exhausted_reservation_token_states_return_the_dedicated_error() {
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
