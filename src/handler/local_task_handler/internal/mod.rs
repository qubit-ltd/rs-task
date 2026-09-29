// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Private implementation details for the local task handler adapter.

mod local_task_closure;

pub(in crate::handler::local_task_handler) use local_task_closure::LocalTaskClosure;
