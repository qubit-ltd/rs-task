// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
// Owns private resource-accounting and reservation-guard types.
mod internal;

use std::any::Any;
use std::sync::Arc;

use futures::FutureExt;
use internal::ReservationGuard;
use internal::ResourceLedger;
use internal::release_reservation;
use parking_lot::Mutex;
use tokio::spawn;
use tokio::sync::oneshot;

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

/// Single-process executor that atomically accounts for CPU, GPU, and custom
/// resources.
///
/// # Examples
///
/// ```
/// use qubit_task::engine::LocalTaskExecutionEngine;
/// use qubit_task::engine::TaskExecutionEngine;
/// use qubit_task::model::ResourceCapacity;
///
/// let engine = LocalTaskExecutionEngine::new(ResourceCapacity::default());
/// assert_eq!(engine.capacity().used_cpu_slots, 0);
/// ```
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
}

impl TaskExecutionEngine for LocalTaskExecutionEngine {
    /// Returns the immutable engine limits and current reservation totals.
    ///
    /// # Returns
    ///
    /// Configured limits and a snapshot of currently held resources.
    fn capacity(&self) -> ResourceSnapshot {
        let ledger = self.ledger.lock();
        ResourceSnapshot {
            capacity: self.capacity.clone(),
            used_cpu_slots: ledger.usage.cpu,
            used_gpus: ledger.usage.gpus.clone(),
            used_custom: ledger.usage.custom.clone(),
        }
    }

    /// Reserves requested CPU, GPU, and custom resources as one atomic unit.
    ///
    /// The returned reservation rolls back automatically unless activation
    /// transfers its release callback to the execution worker.
    ///
    /// # Parameters
    ///
    /// * `id` - Stable identity of the task attempt.
    /// * `request` - Resource amounts and labels required by the attempt.
    ///
    /// # Returns
    ///
    /// A prepared reservation or an engine error describing unavailable
    /// capacity.
    ///
    /// # Errors
    ///
    /// Returns `Unsatisfiable` when the request exceeds configured capacity
    /// and `TemporarilyUnavailable` when valid resources are currently held.
    fn try_prepare(&self, id: TaskId, request: ResourceRequest) -> Result<PreparedExecution, EngineError> {
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
        let cpu_available = ledger
            .usage
            .cpu
            .checked_add(request.cpu_slots)
            .is_some_and(|total| total <= self.capacity.cpu_slots);
        if !cpu_available || available_gpus.len() != request.gpu_count as usize || !available_custom {
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
        ledger.usage.gpus.extend(available_gpus.clone());
        for (name, value) in &request.custom {
            *ledger.usage.custom.entry(name.clone()).or_default() += value;
        }
        ledger
            .allocations
            .insert(token, (request.cpu_slots, available_gpus.clone(), request.custom));
        drop(ledger);
        let ledger = Arc::clone(&self.ledger);
        Ok(PreparedExecution {
            id,
            assigned: available_gpus,
            release: Some(Box::new(move || release_reservation(token, &ledger))),
        })
    }

    /// Starts the handler future on a Tokio async worker and tracks its
    /// completion.
    ///
    /// The reservation remains held until the handler exits, including panic
    /// unwinding, so the returned handle represents the full resource lease.
    ///
    /// # Parameters
    ///
    /// * `prepared` - Reservation created for this attempt.
    /// * `handler` - Handler invoked with the request payload.
    /// * `payload` - Opaque task input bytes.
    /// * `context` - Attempt metadata and cooperative cancellation signal.
    ///
    /// # Returns
    ///
    /// A handle whose receiver reports the handler result or panic.
    ///
    /// # Errors
    ///
    /// This implementation does not return an engine error after receiving a
    /// prepared reservation.
    ///
    /// # Panics
    ///
    /// Panics if the returned future is polled outside an active Tokio
    /// runtime, because activation starts a Tokio task.
    fn activate<'a>(
        &'a self,
        mut prepared: PreparedExecution,
        handler: Arc<dyn TaskHandler>,
        payload: Vec<u8>,
        context: TaskContext,
    ) -> TaskFuture<'a, Result<ExecutionHandle, EngineError>> {
        Box::pin(async move {
            let (sender, receiver) = oneshot::channel();
            let cancelled = context.cancellation_signal();
            let release = prepared.release.take();
            let task_context = context;
            let guard = ReservationGuard(release);
            spawn(async move {
                let handler_future = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    handler.run(&payload, task_context)
                })) {
                    Ok(future) => future,
                    Err(payload) => {
                        drop(guard);
                        let _ = sender.send(ExecutionOutcome::Panicked(panic_message(payload)));
                        return;
                    }
                };
                let outcome = match std::panic::AssertUnwindSafe(handler_future).catch_unwind().await {
                    Ok(result) => ExecutionOutcome::Returned(result),
                    Err(payload) => ExecutionOutcome::Panicked(panic_message(payload)),
                };
                // Completion means handler cleanup and resource release have
                // both finished, so service shutdown can safely await handles.
                drop(guard);
                let _ = sender.send(outcome);
            });
            Ok(ExecutionHandle { receiver, cancelled })
        })
    }
}

/// Converts a panic payload to a diagnostic string.
///
/// # Parameters
///
/// * `payload` - Panic value captured from handler execution.
///
/// # Returns
///
/// The panic message or a stable fallback for non-string payloads.
fn panic_message(payload: Box<dyn Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else if let Some(message) = payload.downcast_ref::<&'static str>() {
        (*message).to_owned()
    } else {
        "task handler panicked with a non-string payload".into()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    use super::LocalTaskExecutionEngine;
    use crate::engine::EngineError;
    use crate::engine::ExecutionOutcome;
    use crate::engine::TaskExecutionEngine;
    use crate::handler::TaskContext;
    use crate::handler::TaskHandler;
    use crate::handler::TaskHandlerDescriptor;
    use crate::handler::TaskHandlerRegistry;
    use crate::handler::TaskRunOutcome;
    use crate::handler::TaskRunResult;
    use crate::model::ResourceCapacity;
    use crate::model::ResourceRequest;
    use crate::model::TaskId;
    use crate::model::TaskOutput;
    use crate::store::TaskFuture;

    struct NoopHandler;

    impl TaskHandler for NoopHandler {
        fn descriptor(&self) -> TaskHandlerDescriptor {
            TaskHandlerDescriptor {
                task_type: "noop".into(),
                version: "1".into(),
            }
        }

        fn run<'a>(&'a self, _payload: &'a [u8], _context: TaskContext) -> TaskFuture<'a, TaskRunResult> {
            Box::pin(async { Ok(TaskRunOutcome::Succeeded(TaskOutput::default())) })
        }
    }

    /// Completion is observable only after all reserved resource classes are
    /// released and can be allocated to another task. The private fixture and
    /// crate-private completion receiver keep this contract in the unit test.
    #[tokio::test]
    async fn test_successful_execution_releases_all_resources_before_completion() {
        let custom = BTreeMap::from([("license".to_owned(), 2)]);
        let engine = LocalTaskExecutionEngine::new(ResourceCapacity {
            cpu_slots: 1,
            gpus: BTreeMap::from([("gpu-0".to_owned(), vec!["compute".to_owned()])]),
            custom: custom.clone(),
        });
        let request = ResourceRequest {
            cpu_slots: 1,
            gpu_count: 1,
            gpu_labels: vec!["compute".to_owned()],
            custom: custom.clone(),
        };
        let mut handlers = TaskHandlerRegistry::new();
        handlers
            .register(Arc::new(NoopHandler))
            .expect("register noop task handler");
        let handler = handlers.resolve("noop", "1").expect("resolve registered task handler");
        let id = TaskId::generate();
        let prepared = engine
            .try_prepare(id, request.clone())
            .expect("reserve all resource classes");
        assert_eq!(prepared.assigned_resources(), ["gpu-0"]);
        let handle = engine
            .activate(
                prepared,
                handler,
                Vec::new(),
                TaskContext::new(id, 1, vec!["gpu-0".to_owned()], Arc::new(AtomicBool::new(false))),
            )
            .await
            .expect("activate reserved execution");
        // The current-thread runtime has not polled the spawned handler yet.
        let held = engine.capacity();
        assert_eq!(held.used_cpu_slots, 1);
        assert_eq!(held.used_gpus, ["gpu-0"]);
        assert_eq!(held.used_custom, custom);
        assert!(matches!(
            engine.try_prepare(TaskId::generate(), request.clone()),
            Err(EngineError::TemporarilyUnavailable)
        ));

        let outcome = tokio::time::timeout(std::time::Duration::from_secs(2), handle.receiver)
            .await
            .expect("execution completes within test watchdog")
            .expect("worker sends completion outcome");
        assert!(matches!(
            outcome,
            ExecutionOutcome::Returned(Ok(TaskRunOutcome::Succeeded(_)))
        ));
        let released = engine.capacity();
        assert_eq!(released.used_cpu_slots, 0);
        assert!(released.used_gpus.is_empty());
        assert!(released.used_custom.values().all(|used| *used == 0));

        let next = engine
            .try_prepare(TaskId::generate(), request)
            .expect("completed resources can be reserved again");
        assert_eq!(next.assigned_resources(), ["gpu-0"]);
        let reassigned = engine.capacity();
        assert_eq!(reassigned.used_cpu_slots, 1);
        assert_eq!(reassigned.used_gpus, ["gpu-0"]);
        assert_eq!(reassigned.used_custom, custom);
        drop(next);
        let released_again = engine.capacity();
        assert_eq!(released_again.used_cpu_slots, 0);
        assert!(released_again.used_gpus.is_empty());
        assert!(released_again.used_custom.values().all(|used| *used == 0));
    }

    #[test]
    fn runtime_drop_releases_unpolled_execution_reservation() {
        let engine = LocalTaskExecutionEngine::new(ResourceCapacity {
            cpu_slots: 1,
            ..ResourceCapacity::default()
        });
        let id = TaskId::generate();
        let prepared = engine
            .try_prepare(
                id,
                ResourceRequest {
                    cpu_slots: 1,
                    ..ResourceRequest::default()
                },
            )
            .unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let _handle = runtime
            .block_on(engine.activate(
                prepared,
                Arc::new(NoopHandler),
                Vec::new(),
                TaskContext::new(id, 1, Vec::new(), Arc::new(AtomicBool::new(false))),
            ))
            .unwrap();
        assert_eq!(engine.capacity().used_cpu_slots, 1);
        drop(runtime);
        assert_eq!(engine.capacity().used_cpu_slots, 0);
    }
}
