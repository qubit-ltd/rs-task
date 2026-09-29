// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;

use parking_lot::Mutex;
use tokio::runtime;
use tokio::sync;
use tokio::sync::Notify;
use tokio::sync::oneshot;

use super::RunningCancellation;
use crate::engine::TaskExecutionEngine;
use crate::handler::TaskHandler;
use crate::handler::TaskHandlerRegistry;
use crate::model::OwnerEpoch;
use crate::model::TaskId;
use crate::model::TaskState;
use crate::scheduling::SchedulingPolicy;
use crate::service::admission_budget::AdmissionBudget;
use crate::service::admission_gate::AdmissionGate;
use crate::service::local_task_result_error::LocalTaskResultError as LocalResultError;
use crate::service::retry_policy::RetryPolicy;
use crate::service::scheduler_queue::SchedulerQueue;
#[cfg(feature = "event-bus")]
use crate::service::task_event_publisher::TaskEventPublisher;
use crate::service::task_wait_registry::TaskWaitRegistry;
use crate::store::TaskStore;

/// Components and synchronization state shared by service handle clones.
pub(crate) struct ServiceCore {
    /// Authoritative lifecycle and request store.
    pub(crate) store: Arc<dyn TaskStore>,
    /// Backend that atomically reserves resources and starts handlers.
    pub(crate) engine: Arc<dyn TaskExecutionEngine>,
    /// Strategy used to order scheduler candidates.
    pub(crate) policy: Arc<dyn SchedulingPolicy>,
    /// Runtime used for service-owned asynchronous workers.
    pub(crate) runtime_handle: runtime::Handle,
    /// Exact-version handler registry.
    pub(crate) handlers: TaskHandlerRegistry,
    /// Maximum accepted queue entries.
    pub(crate) queue_capacity: usize,
    /// Maximum candidates examined by each scheduler pass.
    pub(crate) scan_budget: usize,
    /// Maximum attempts allowed for each task.
    pub(crate) max_attempts: u32,
    /// Delay schedule used for retryable failures.
    pub(crate) retry_policy: RetryPolicy,
    /// Concurrent execution slots.
    pub(crate) running_slots: Arc<sync::Semaphore>,
    /// Ready and delayed task queues.
    pub(crate) queue: Mutex<SchedulerQueue>,
    /// Number of accepted tasks occupying queue capacity.
    pub(crate) queue_count: AtomicUsize,
    /// Process-local handlers not persisted in the store.
    pub(crate) local_handlers: Mutex<HashMap<TaskId, Arc<dyn TaskHandler>>>,
    /// Result finalization senders for process-local task handles.
    pub(crate) local_finalizations: Mutex<HashMap<TaskId, oneshot::Sender<Result<TaskState, LocalResultError>>>>,
    /// Cooperative cancellation signals indexed by task ID.
    pub(crate) cancellations: Mutex<HashMap<TaskId, RunningCancellation>>,
    /// Wakes the scheduler and shutdown coordinator after state changes.
    pub(crate) changed: Notify,
    /// Per-task notification registry used by waiters.
    pub(in crate::service) wait_registry: Arc<TaskWaitRegistry>,
    /// Coordinates lifecycle transitions with shutdown of the event publisher.
    pub(crate) transition_event_lock: sync::RwLock<()>,
    /// Prevents new admissions after shutdown starts.
    pub(in crate::service) admission: AdmissionGate,
    /// Bounds detached admission workers and payload retention.
    pub(in crate::service) admission_budget: Arc<AdmissionBudget>,
    /// Exclusive recoverable-store ownership epoch, if supported.
    pub(crate) owner: Option<OwnerEpoch>,
    /// First latched store failure suspending service progress.
    pub(crate) store_fault: Mutex<Option<String>>,
    /// First latched scheduler failure suspending service progress.
    pub(crate) scheduler_fault: Mutex<Option<String>>,
    /// Running attempts whose completion has not been finalized.
    pub(crate) attempts_in_flight: AtomicUsize,
    /// Wakes shutdown waiters when attempt count changes.
    pub(crate) attempts_changed: Notify,
    /// Whether the scheduler worker has exited.
    pub(crate) scheduler_finished: AtomicBool,
    /// Wakes shutdown waiters when the scheduler exits.
    pub(crate) scheduler_finished_notify: Notify,
    /// Optional bounded lifecycle event publisher.
    #[cfg(feature = "event-bus")]
    pub(in crate::service) event_bus: Option<TaskEventPublisher>,
}
