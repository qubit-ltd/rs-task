// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Pluggable resource reservation and task execution engines.

mod engine_error;
mod execution_handle;
mod execution_outcome;
mod local_task_execution_engine;
mod prepared_execution;
mod task_execution_engine;
mod task_execution_engine_provider;

pub use engine_error::EngineError;
pub use execution_handle::ExecutionHandle;
pub use execution_outcome::ExecutionOutcome;
pub use local_task_execution_engine::LocalTaskExecutionEngine;
pub use prepared_execution::PreparedExecution;
pub use task_execution_engine::TaskExecutionEngine;
pub use task_execution_engine_provider::TaskExecutionEngineProvider;
