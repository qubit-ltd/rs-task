// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::any::Any;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::engine::EngineError;
use crate::engine::ExecutionHandle;
use crate::engine::ExecutionOutcome;
use crate::engine::PreparedExecution;
use crate::engine::TaskExecutionEngine;
use crate::handler::TaskContext;
use crate::handler::TaskHandler;
use crate::model::ResourceCapacity;
use crate::model::ResourceRequest;
use crate::model::ResourceSnapshot;
use crate::model::TaskId;
use crate::store::TaskFuture;

type Allocation = (u32, Vec<String>, BTreeMap<String, u64>);
type AllocationLedger = HashMap<u64, Allocation>;

/// Mutable aggregate of resources currently reserved by active attempts.
#[derive(Default)]
struct Usage {
    cpu: u32,
    gpus: Vec<String>,
    custom: BTreeMap<String, u64>,
}

/// Single-process executor that atomically accounts for CPU, GPU, and custom
/// resources.
pub struct LocalTaskExecutionEngine {
    capacity: ResourceCapacity,
    usage: Arc<Mutex<Usage>>,
    next_token: std::sync::atomic::AtomicU64,
    allocations: Arc<Mutex<AllocationLedger>>,
}

impl LocalTaskExecutionEngine {
    /// Creates a local engine with explicit resource capacity.
    #[must_use]
    pub fn new(capacity: ResourceCapacity) -> Self {
        Self {
            capacity,
            usage: Arc::new(Mutex::new(Usage::default())),
            next_token: std::sync::atomic::AtomicU64::new(1),
            allocations: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl TaskExecutionEngine for LocalTaskExecutionEngine {
    fn capacity(&self) -> ResourceSnapshot {
        let usage = self.usage.lock();
        ResourceSnapshot {
            capacity: self.capacity.clone(),
            used_cpu_slots: usage.cpu,
            used_gpus: usage.gpus.clone(),
            used_custom: usage.custom.clone(),
        }
    }

    fn prepare<'a>(
        &'a self,
        id: TaskId,
        request: ResourceRequest,
    ) -> TaskFuture<'a, Result<PreparedExecution, EngineError>> {
        Box::pin(async move {
            let matching_gpu_capacity = self
                .capacity
                .gpus
                .values()
                .filter(|labels| request.gpu_labels.iter().all(|label| labels.contains(label)))
                .count();
            if request.cpu_slots > self.capacity.cpu_slots
                || request.gpu_count as usize > matching_gpu_capacity
                || request
                    .custom
                    .iter()
                    .any(|(name, value)| self.capacity.custom.get(name).is_none_or(|limit| value > limit))
            {
                return Err(EngineError::Unsatisfiable);
            }
            let mut usage = self.usage.lock();
            let available_gpus = self
                .capacity
                .gpus
                .iter()
                .filter(|(id, labels)| {
                    !usage.gpus.contains(id) && request.gpu_labels.iter().all(|label| labels.contains(label))
                })
                .map(|(id, _)| id.clone())
                .take(request.gpu_count as usize)
                .collect::<Vec<_>>();
            let available_custom = request.custom.iter().all(|(name, value)| {
                usage.custom.get(name).copied().unwrap_or(0).saturating_add(*value)
                    <= self.capacity.custom.get(name).copied().unwrap_or(0)
            });
            if usage.cpu.saturating_add(request.cpu_slots) > self.capacity.cpu_slots
                || available_gpus.len() != request.gpu_count as usize
                || !available_custom
            {
                return Err(EngineError::TemporarilyUnavailable);
            }
            usage.cpu += request.cpu_slots;
            usage.gpus.extend(available_gpus.clone());
            for (name, value) in &request.custom {
                *usage.custom.entry(name.clone()).or_default() += value;
            }
            let token = self.next_token.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.allocations
                .lock()
                .insert(token, (request.cpu_slots, available_gpus.clone(), request.custom));
            let allocations = self.allocations.clone();
            let usage_ref = self.usage.clone();
            Ok(PreparedExecution {
                id,
                assigned: available_gpus,
                release: Some(Box::new(move || release(token, &allocations, &usage_ref))),
            })
        })
    }

    fn activate<'a>(
        &'a self,
        mut prepared: PreparedExecution,
        handler: Arc<dyn TaskHandler>,
        payload: Vec<u8>,
        context: TaskContext,
    ) -> TaskFuture<'a, Result<ExecutionHandle, EngineError>> {
        Box::pin(async move {
            let (sender, receiver) = tokio::sync::oneshot::channel();
            let cancelled = context.cancellation_signal();
            let release = prepared.release.take();
            let task_context = context;
            tokio::spawn(async move {
                let _guard = ReservationGuard(release);
                let handler_future = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    handler.run(&payload, task_context)
                })) {
                    Ok(future) => future,
                    Err(payload) => {
                        let _ = sender.send(ExecutionOutcome::Panicked(panic_message(payload)));
                        return;
                    }
                };
                let outcome = match std::panic::AssertUnwindSafe(handler_future).catch_unwind().await {
                    Ok(result) => ExecutionOutcome::Returned(result),
                    Err(payload) => ExecutionOutcome::Panicked(panic_message(payload)),
                };
                let _ = sender.send(outcome);
            });
            Ok(ExecutionHandle { receiver, cancelled })
        })
    }
}

/// Converts a panic payload to a diagnostic string.
fn panic_message(payload: Box<dyn Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else if let Some(message) = payload.downcast_ref::<&'static str>() {
        (*message).to_owned()
    } else {
        "task handler panicked with a non-string payload".into()
    }
}

/// Releases a prepared reservation when its execution worker exits or unwinds.
struct ReservationGuard(Option<Box<dyn FnOnce() + Send>>);
impl Drop for ReservationGuard {
    fn drop(&mut self) {
        if let Some(release) = self.0.take() {
            release();
        }
    }
}

/// Removes one reservation and returns its resources to the shared counters.
fn release(token: u64, allocations: &Mutex<AllocationLedger>, usage: &Mutex<Usage>) {
    if let Some((cpu, gpus, custom)) = allocations.lock().remove(&token) {
        let mut current = usage.lock();
        current.cpu = current.cpu.saturating_sub(cpu);
        current.gpus.retain(|id| !gpus.contains(id));
        for (name, amount) in custom {
            if let Some(value) = current.custom.get_mut(&name) {
                *value = value.saturating_sub(amount);
                if *value == 0 {
                    current.custom.remove(&name);
                }
            }
        }
    }
}

use futures::FutureExt;
