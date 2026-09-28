// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use super::ServiceCore;
use super::begin_shutdown_core;

/// Tracks the lifetime of all public service handles and their admission
/// workers.
pub(in crate::service::task_execution_service) struct ServiceHandleLease {
    /// Weak reference used to start shutdown after the final handle is dropped.
    pub(in crate::service::task_execution_service) core: std::sync::Weak<ServiceCore>,
}

impl Drop for ServiceHandleLease {
    /// Starts asynchronous service shutdown when the last handle lease is gone.
    fn drop(&mut self) {
        if let Some(core) = self.core.upgrade() {
            begin_shutdown_core(core);
        }
    }
}
