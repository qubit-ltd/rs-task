// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Versioned task handler contracts and registry.

#[cfg(test)]
mod local_task_handler;
#[cfg(test)]
mod registry_error;
#[cfg(test)]
mod task_context;
#[cfg(test)]
mod task_handler;
#[cfg(test)]
mod task_handler_descriptor;
#[cfg(test)]
mod task_handler_registry;
mod task_run_outcome;
#[cfg(not(test))]
pub(crate) mod typed;

#[cfg(test)]
pub(crate) use local_task_handler::LocalTaskHandler;
#[cfg(test)]
pub(crate) use registry_error::RegistryError;
#[cfg(test)]
pub(crate) use task_context::TaskContext;
#[cfg(test)]
pub(crate) use task_handler::TaskHandler;
#[cfg(test)]
pub(crate) use task_handler_descriptor::TaskHandlerDescriptor;
#[cfg(test)]
pub(crate) use task_handler_registry::TaskHandlerRegistry;
pub use task_run_outcome::TaskRunOutcome;
pub use task_run_outcome::TaskRunResult;
#[cfg(not(test))]
pub use typed::CancellationMode;
#[cfg(not(test))]
pub use typed::ExternalCancellationHook;
#[cfg(not(test))]
pub use typed::HandlerDispatchError;
#[cfg(not(test))]
pub use typed::HandlerRegistrationError;
#[cfg(not(test))]
pub use typed::TaskHandlerDescriptor;
#[cfg(not(test))]
pub use typed::TypedTaskContext as TaskContext;
#[cfg(not(test))]
pub use typed::TypedTaskHandler as TaskHandler;
#[cfg(not(test))]
pub use typed::TypedTaskHandlerRegistry as TaskHandlerRegistry;
