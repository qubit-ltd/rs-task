// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Prepared execution payload for a validated typed task handler.

#[path = "prepared_task/prepared_task.rs"]
mod implementation;

pub(crate) use implementation::PreparedTask;
