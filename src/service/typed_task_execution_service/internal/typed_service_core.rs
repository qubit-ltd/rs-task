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
use std::sync::atomic::Ordering;

use parking_lot::Mutex;
use qubit_codec::ValueBytesCodecRegistry;
use qubit_id::Id;
use qubit_id::IdGenerationError;
use qubit_id::IdGenerator;

use super::super::super::owner_release_guard::OwnerReleaseGuard;
#[cfg(feature = "event-bus")]
use super::super::super::task_event_publisher::TaskEventPublisher;
use super::ExternalCancelEntry;
use super::ShutdownProgress;
use crate::engine::LocalTaskExecutionEngine;
use crate::handler::typed::TypedTaskHandlerRegistry;
use crate::model::typed::TaskId;
use crate::service::RetryPolicy;
use crate::store::TaskStore;

/// Holds dependencies and lifecycle state shared by typed service handles.
pub(in crate::service::typed_task_execution_service) struct TypedServiceCore {
    /// Store that owns accepted task state and lifecycle transitions.
    pub(in crate::service::typed_task_execution_service) store: Arc<dyn TaskStore>,
    /// Payload codecs used when requests are submitted and handlers start.
    pub(in crate::service::typed_task_execution_service) codecs: Arc<ValueBytesCodecRegistry>,
    /// Generator for identities assigned to new tasks.
    pub(in crate::service::typed_task_execution_service) id_generator: Arc<dyn IdGenerator<Id, IdGenerationError>>,
    /// Local engine that reserves resources for task attempts.
    pub(in crate::service::typed_task_execution_service) engine: Arc<LocalTaskExecutionEngine>,
    /// Registered handlers available to the scheduler.
    pub(in crate::service::typed_task_execution_service) handlers: TypedTaskHandlerRegistry,
    /// Cooperative cancellation flags keyed by task and attempt.
    pub(in crate::service::typed_task_execution_service) cancellations: Mutex<HashMap<(TaskId, u32), Arc<AtomicBool>>>,
    /// External cancellation requests currently running or already successful.
    pub(in crate::service::typed_task_execution_service) external_cancellations:
        Mutex<HashMap<(TaskId, u32), ExternalCancelEntry>>,
    /// Guard for exclusive ownership of the store.
    pub(in crate::service::typed_task_execution_service) owner_guard: tokio::sync::Mutex<Option<OwnerReleaseGuard>>,
    /// Gate that prevents new admissions while shutdown drains.
    pub(in crate::service::typed_task_execution_service) admission: tokio::sync::RwLock<()>,
    /// Wakes the scheduler after work or lifecycle changes.
    pub(in crate::service::typed_task_execution_service) scheduler_changed: tokio::sync::Notify,
    /// Indicates that the scheduler task has stopped.
    pub(in crate::service::typed_task_execution_service) scheduler_stopped: AtomicBool,
    /// Wakes shutdown when the scheduler stops.
    pub(in crate::service::typed_task_execution_service) scheduler_stopped_changed: tokio::sync::Notify,
    /// Maximum number of task attempts that may run concurrently.
    pub(in crate::service::typed_task_execution_service) max_running_tasks: usize,
    /// Maximum queued summaries inspected per scheduler page.
    pub(in crate::service::typed_task_execution_service) scan_page_size: usize,
    /// Successful newer starts permitted while an older task waits for
    /// resources.
    pub(in crate::service::typed_task_execution_service) max_resource_bypasses: usize,
    /// Maximum attempts allowed for retryable handler failures.
    pub(in crate::service::typed_task_execution_service) max_attempts: u32,
    /// Delay policy applied before retrying failed attempts.
    pub(in crate::service::typed_task_execution_service) retry_policy: RetryPolicy,
    /// First operational fault observed by the service.
    pub(in crate::service::typed_task_execution_service) fault: Mutex<Option<String>>,
    /// Number of handler and external cancellation operations still active.
    pub(in crate::service::typed_task_execution_service) in_flight: AtomicUsize,
    /// Wakes shutdown when the active operation count changes.
    pub(in crate::service::typed_task_execution_service) in_flight_changed: tokio::sync::Notify,
    /// Indicates that shutdown has begun.
    pub(in crate::service::typed_task_execution_service) shutting_down: AtomicBool,
    /// Wakes the shutdown supervisor and retrying waiters.
    pub(in crate::service::typed_task_execution_service) shutdown_changed: tokio::sync::Notify,
    /// Wakes callers waiting for a shutdown attempt result.
    pub(in crate::service::typed_task_execution_service) shutdown_completed: tokio::sync::Notify,
    /// Results and retry state shared by concurrent shutdown callers.
    pub(in crate::service::typed_task_execution_service) shutdown_progress: Mutex<ShutdownProgress>,
    /// Indicates that the last public service handle was dropped.
    pub(in crate::service::typed_task_execution_service) public_gone: AtomicBool,
    /// Indicates that exclusive store ownership was released.
    pub(in crate::service::typed_task_execution_service) owner_released: AtomicBool,
    /// Optional durable lifecycle event publisher.
    #[cfg(feature = "event-bus")]
    pub(in crate::service::typed_task_execution_service) publisher: Option<TaskEventPublisher>,
    /// Most recent publisher close error, if shutdown could not drain it.
    #[cfg(feature = "event-bus")]
    pub(in crate::service::typed_task_execution_service) notification_close_error: Mutex<Option<String>>,
}

impl TypedServiceCore {
    /// Signals the supervisor without blocking the dropping handle or caller.
    pub(in crate::service::typed_task_execution_service) fn request_shutdown(&self) {
        self.shutting_down.store(true, Ordering::Release);
        self.scheduler_changed.notify_waiters();
        self.shutdown_changed.notify_waiters();
    }
}
