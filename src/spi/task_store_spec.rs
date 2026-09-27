// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;

use qubit_spi::ServiceSpec;
use qubit_spi::SyncServiceSpec;

use super::TaskStoreConfig;
use crate::store::StoreError;
use crate::store::TaskStore;

/// Service family for a task history backend.
///
/// # Examples
///
/// ```
/// use qubit_spi::ServiceSpec;
/// use qubit_task::spi::TaskStoreConfig;
/// use qubit_task::spi::TaskStoreSpec;
///
/// let config: <TaskStoreSpec as ServiceSpec>::Config = TaskStoreConfig::default();
/// assert!(matches!(config, TaskStoreConfig::Memory { .. }));
/// ```
pub struct TaskStoreSpec;

impl ServiceSpec for TaskStoreSpec {
    /// Store-specific options selected by the application.
    type Config = TaskStoreConfig;
    /// Store configuration, initialization, or persistence failure.
    type Error = StoreError;
}

impl SyncServiceSpec for TaskStoreSpec {
    /// Shared task store created from the selected configuration.
    type Output = Arc<dyn TaskStore>;
}
