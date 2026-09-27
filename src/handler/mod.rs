// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Versioned task handler contracts and registry.

mod local_task_handler;
mod registry_error;
mod task_context;
mod task_handler;
mod task_handler_descriptor;
mod task_handler_provider;
mod task_handler_registry;
mod task_run_outcome;

pub use local_task_handler::LocalTaskHandler;
pub use registry_error::RegistryError;
pub use task_context::TaskContext;
pub use task_handler::TaskHandler;
pub use task_handler_descriptor::TaskHandlerDescriptor;
pub use task_handler_provider::TaskHandlerProvider;
pub use task_handler_registry::TaskHandlerRegistry;
pub use task_run_outcome::TaskRunOutcome;
pub use task_run_outcome::TaskRunResult;
