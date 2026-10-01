// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Typed handler contracts and their runtime payload adapter.

mod cancellation_mode;
mod external_cancellation_hook;
mod handler_dispatch_error;
mod handler_registration_error;
mod prepared_task;
mod task_handler_descriptor;
mod typed_task_context;
mod typed_task_handler;
mod typed_task_handler_registry;

pub use cancellation_mode::CancellationMode;
pub use external_cancellation_hook::ExternalCancellationHook;
pub use handler_dispatch_error::HandlerDispatchError;
pub use handler_registration_error::HandlerRegistrationError;
pub(crate) use prepared_task::PreparedTask;
pub use task_handler_descriptor::TaskHandlerDescriptor;
pub use typed_task_context::TypedTaskContext;
pub use typed_task_handler::TypedTaskHandler;
pub use typed_task_handler_registry::TypedTaskHandlerRegistry;
