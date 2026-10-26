// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Persists `rs-progress` events as bounded task progress snapshots.

mod task_progress_reporter;

pub(super) use task_progress_reporter::TaskProgressReporter;
