// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Persists `rs-progress` events as bounded task progress snapshots.

#[path = "task_progress_reporter/task_progress_reporter.rs"]
mod reporter_impl;

pub(in crate::service) use reporter_impl::TaskProgressReporter;
