// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Weak;
use std::sync::atomic::Ordering;

use super::TypedServiceCore;

/// Requests graceful shutdown when the final public handle is dropped.
pub(in crate::service::typed_task_execution_service) struct PublicLifetime {
    /// Weak reference used to request shutdown when the public handle
    /// disappears.
    pub(in crate::service::typed_task_execution_service) core: Weak<TypedServiceCore>,
}

impl Drop for PublicLifetime {
    /// Notifies the service core when the final public handle is dropped.
    fn drop(&mut self) {
        if let Some(core) = self.core.upgrade() {
            core.public_gone.store(true, Ordering::Release);
            core.request_shutdown();
        }
    }
}
