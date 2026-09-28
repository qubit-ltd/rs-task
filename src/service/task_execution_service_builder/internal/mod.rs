// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
// Validates and restores bounded pages before scheduler startup.
mod recovery;
// Owns the store lease during incomplete service construction.
mod owner_guard;

pub(in crate::service::task_execution_service_builder) use owner_guard::OwnerGuard;
pub(in crate::service::task_execution_service_builder) use recovery::restore_tasks_paged;
