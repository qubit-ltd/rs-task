// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::any::Any;
use std::sync::Arc;

use qubit_spi::ServiceSpec;
use qubit_spi::SyncServiceSpec;

use crate::handler::TaskHandler;

/// Service family for task handlers. Applications may register many providers.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
///
/// use qubit_spi::ServiceSpec;
/// use qubit_task::spi::TaskHandlerSpec;
///
/// let config: <TaskHandlerSpec as ServiceSpec>::Config = Arc::new("settings".to_owned());
/// assert!(config.downcast_ref::<String>().is_some());
/// ```
pub struct TaskHandlerSpec;

impl ServiceSpec for TaskHandlerSpec {
    /// Type-erased, thread-safe configuration supplied to a handler provider.
    type Config = Arc<dyn Any + Send + Sync>;
    /// I/O error returned when handler provider configuration is invalid.
    type Error = std::io::Error;
}

impl SyncServiceSpec for TaskHandlerSpec {
    /// Shared task handler created for its declared type and version.
    type Output = Arc<dyn TaskHandler>;
}
