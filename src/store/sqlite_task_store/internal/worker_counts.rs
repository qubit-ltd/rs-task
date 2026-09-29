// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
#[cfg(test)]
use std::sync::atomic::AtomicUsize;

#[cfg(test)]
#[derive(Default)]
pub(in crate::store::sqlite_task_store) struct WorkerCounts {
    pub(in crate::store::sqlite_task_store) active: AtomicUsize,
    pub(in crate::store::sqlite_task_store) peak: AtomicUsize,
}
