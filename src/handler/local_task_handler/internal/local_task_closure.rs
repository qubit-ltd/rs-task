// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! One-shot closure type consumed by a local task handler.

use crate::handler::TaskContext;
use crate::handler::TaskRunResult;

/// One-shot local task closure with its per-attempt context.
pub(in crate::handler::local_task_handler) type LocalTaskClosure = Box<dyn FnOnce(TaskContext) -> TaskRunResult + Send>;
