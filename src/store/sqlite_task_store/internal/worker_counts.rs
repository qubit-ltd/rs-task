// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
#[cfg(test)]
use std::sync::atomic::AtomicUsize;

/// Counts active SQLite test workers and records their observed peak.
#[cfg(test)]
#[derive(Default)]
pub(in crate::store::sqlite_task_store) struct WorkerCounts {
    /// Number of blocking workers currently inside a connection operation.
    pub(in crate::store::sqlite_task_store) active: AtomicUsize,
    /// Maximum number of simultaneous workers observed by a test.
    pub(in crate::store::sqlite_task_store) peak: AtomicUsize,
}
