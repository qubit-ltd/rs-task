// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
mod attempt_finalizer;
mod attempt_in_flight_guard;
mod queue_window_guard;
mod scheduler;
mod service_core;
mod shutdown;

pub(super) use attempt_finalizer::finish_attempt;
pub(super) use attempt_in_flight_guard::AttemptInFlightGuard;
pub(super) use queue_window_guard::QueueWindowGuard;
pub(super) use scheduler::scheduler_loop;
pub(crate) use service_core::RunningCancellation;
pub(crate) use service_core::ServiceCore;
pub(super) use shutdown::begin_shutdown_core;
