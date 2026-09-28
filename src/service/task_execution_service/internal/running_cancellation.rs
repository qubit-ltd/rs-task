// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

/// Signal belonging to one specific execution attempt of a task.
pub(crate) struct RunningCancellation {
    /// Execution generation owning this cancellation signal.
    pub(crate) attempt: u32,
    /// Shared signal observed by the handler and engine.
    pub(crate) signal: Arc<AtomicBool>,
}
