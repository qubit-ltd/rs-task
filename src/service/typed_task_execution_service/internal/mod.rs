// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
mod external_cancel_entry;
mod external_cancel_flight;
mod external_cancel_result;
mod in_flight_guard;
mod public_lifetime;
mod resource_fairness;
mod service_runner;
mod shutdown_outcome;
mod shutdown_progress;
mod typed_service_core;

pub(in crate::service::typed_task_execution_service) use external_cancel_entry::ExternalCancelEntry;
pub(in crate::service::typed_task_execution_service) use external_cancel_flight::ExternalCancelFlight;
pub(in crate::service::typed_task_execution_service) use external_cancel_result::ExternalCancelResult;
pub(in crate::service::typed_task_execution_service) use in_flight_guard::InFlightGuard;
pub(in crate::service::typed_task_execution_service) use public_lifetime::PublicLifetime;
pub(in crate::service::typed_task_execution_service) use service_runner::ServiceRunner;
pub(in crate::service::typed_task_execution_service) use shutdown_outcome::ShutdownOutcome;
pub(in crate::service::typed_task_execution_service) use shutdown_progress::ShutdownProgress;
pub(in crate::service::typed_task_execution_service) use typed_service_core::TypedServiceCore;
