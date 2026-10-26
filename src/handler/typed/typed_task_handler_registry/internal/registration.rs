// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;

use super::ErasedHandler;
use crate::handler::typed::ExternalCancellationHook;
use crate::handler::typed::TaskHandlerDescriptor;

/// Stored metadata and handler state for one registered task kind.
pub(in crate::handler::typed::typed_task_handler_registry) struct Registration {
    /// Source label reported if a duplicate kind is registered.
    pub(in crate::handler::typed::typed_task_handler_registry) source: String,
    /// Descriptor exposed to callers and used during dispatch validation.
    pub(in crate::handler::typed::typed_task_handler_registry) descriptor: TaskHandlerDescriptor,
    /// Type-erased handler implementation used to prepare payloads.
    pub(in crate::handler::typed::typed_task_handler_registry) handler: Arc<dyn ErasedHandler>,
    /// Optional hook for kinds configured for external cancellation.
    pub(in crate::handler::typed::typed_task_handler_registry) external_cancellation_hook:
        Option<ExternalCancellationHook>,
}
