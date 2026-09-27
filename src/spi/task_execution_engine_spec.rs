// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::convert::Infallible;
use std::sync::Arc;

use qubit_spi::ServiceSpec;
use qubit_spi::SyncServiceSpec;

use crate::engine::TaskExecutionEngine;
use crate::model::ResourceCapacity;

/// Service family for a local or future distributed execution engine.
///
/// # Examples
///
/// ```
/// use qubit_spi::ServiceSpec;
/// use qubit_task::model::ResourceCapacity;
/// use qubit_task::spi::TaskExecutionEngineSpec;
///
/// let config: <TaskExecutionEngineSpec as ServiceSpec>::Config = ResourceCapacity::default();
/// assert_eq!(config.cpu_slots, 0);
/// ```
pub struct TaskExecutionEngineSpec;

impl ServiceSpec for TaskExecutionEngineSpec {
    type Config = ResourceCapacity;
    type Error = Infallible;
}

impl SyncServiceSpec for TaskExecutionEngineSpec {
    type Output = Arc<dyn TaskExecutionEngine>;
}
