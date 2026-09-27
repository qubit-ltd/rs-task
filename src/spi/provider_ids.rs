// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
/// Stable ID for the default FIFO fair scheduling provider.
pub const FAIR_FIFO_PROVIDER_ID: &str = "qubit.task.scheduler.fair-fifo";

/// Stable ID for the local execution engine provider.
pub const LOCAL_ENGINE_PROVIDER_ID: &str = "qubit.task.engine.local";

/// Stable ID for the in-memory storage provider.
pub const MEMORY_STORE_PROVIDER_ID: &str = "qubit.task.store.memory";

/// Stable ID reserved for the recoverable SQLite storage provider.
#[cfg(feature = "sqlite")]
pub const SQLITE_STORE_PROVIDER_ID: &str = "qubit.task.store.sqlite";
