// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Pluggable task history and recovery storage.

mod memory_task_store;
#[cfg(feature = "sqlite")]
mod sqlite_task_store;
mod store_error;
mod task_future;
mod task_store;

pub use memory_task_store::DEFAULT_MAX_UNFINISHED_RECORDS;
pub use memory_task_store::MemoryTaskStore;
#[cfg(feature = "sqlite")]
pub use sqlite_task_store::SqliteTaskStore;
pub use store_error::StoreError;
pub use task_future::TaskFuture;
pub use task_store::TaskStore;
