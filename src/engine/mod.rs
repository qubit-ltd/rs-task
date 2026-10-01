// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Pluggable resource reservation and task execution engines.

mod engine_error;
#[cfg(test)]
mod execution_handle;
#[cfg(test)]
mod execution_outcome;
mod local_task_execution_engine;
#[cfg(test)]
mod prepared_execution;
#[cfg(test)]
mod task_execution_engine;
#[cfg(not(test))]
mod typed_resource_reservation;
pub(crate) use engine_error::EngineError;
#[cfg(test)]
pub use execution_handle::ExecutionHandle;
#[cfg(test)]
pub use execution_outcome::ExecutionOutcome;
pub(crate) use local_task_execution_engine::LocalTaskExecutionEngine;
#[cfg(test)]
pub use prepared_execution::PreparedExecution;
#[cfg(test)]
pub use task_execution_engine::TaskExecutionEngine;
#[cfg(not(test))]
pub use typed_resource_reservation::TypedResourceReservation;
