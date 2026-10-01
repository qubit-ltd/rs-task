// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Versioned task handler contracts and registry.

mod task_run_outcome;
pub(crate) mod typed;

pub use task_run_outcome::TaskRunOutcome;
pub use task_run_outcome::TaskRunResult;
pub use typed::CancellationMode;
pub use typed::ExternalCancellationHook;
pub use typed::HandlerDispatchError;
pub use typed::HandlerRegistrationError;
pub use typed::TaskHandlerDescriptor;
pub use typed::TypedTaskContext as TaskContext;
pub use typed::TypedTaskHandler as TaskHandler;
pub use typed::TypedTaskHandlerRegistry as TaskHandlerRegistry;
