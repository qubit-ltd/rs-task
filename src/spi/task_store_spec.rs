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
    type Config = TaskStoreConfig;
    type Error = StoreError;
}

impl SyncServiceSpec for TaskStoreSpec {
    type Output = Arc<dyn TaskStore>;
}
