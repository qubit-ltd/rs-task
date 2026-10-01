// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Pluggable resource reservation and task execution engines.

mod engine_error;
mod local_task_execution_engine;
mod typed_resource_reservation;
pub(crate) use engine_error::EngineError;
pub(crate) use local_task_execution_engine::LocalTaskExecutionEngine;
pub use typed_resource_reservation::TypedResourceReservation;
