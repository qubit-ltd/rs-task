// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
// Admits detached lifecycle writes and manages bounded queue reservations.
mod admission;
// Persists attempt outcomes and installs retries.
mod attempt_finalizer;
// Supervises persistence panics and owns pre-poll attempt guards.
mod attempt_finalizer_supervisor;
// Tracks attempts that still own execution resources.
mod attempt_in_flight_guard;
// Latches storage and scheduler failures and wakes blocked work.
mod fault;
// Reads service statistics from a consistent store snapshot.
mod query;
// Returns unused candidate reservations to the scheduler queue.
mod queue_window_guard;
// Returns retry queue capacity on every uncommitted exit.
mod retry_queue_reservation;
// Cancels one running attempt through its completion signal.
mod running_cancellation;
// Selects and starts runnable tasks.
mod scheduler;
// Owns the shared service state and its synchronization primitives.
mod service_core;
// Holds one public service handle lease.
mod service_handle_lease;
// Drains work and releases the store owner during shutdown.
mod shutdown;
// Applies conditional state changes and publishes lifecycle notifications.
mod transition;
// Validates requests and computes shared scheduling values.
mod validation;

pub(super) use attempt_finalizer::finish_attempt;
use attempt_finalizer_supervisor::spawn_attempt_finalizer;
use retry_queue_reservation::RetryQueueReservation;
pub(super) use attempt_in_flight_guard::AttemptInFlightGuard;
pub(super) use fault::finalize_local;
pub(super) use fault::panic_message;
pub(super) use fault::pause_on_store_fault;
pub(super) use fault::record_scheduler_fault;
pub(super) use fault::record_store_fault;
pub(super) use query::task_stats;
pub(super) use queue_window_guard::QueueWindowGuard;
pub(crate) use running_cancellation::RunningCancellation;
pub(super) use scheduler::scheduler_loop;
pub(crate) use service_core::ServiceCore;
pub(in crate::service::task_execution_service) use service_handle_lease::ServiceHandleLease;
pub(super) use shutdown::begin_shutdown_core;
#[cfg(test)]
pub(in crate::service::task_execution_service) use shutdown::combine_shutdown_results;
pub(super) use transition::publish_record;
pub(super) use transition::transition;
pub(super) use transition::transition_with_deadline;
pub(super) use validation::now_ms;
pub(super) use validation::release_core_queue_slot;
pub(super) use validation::retry_deadline_ms;
pub(super) use validation::truncate_utf8;
pub(super) use validation::try_reserve_core_queue_slot;
