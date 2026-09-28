// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use crate::model::StoreCapabilities;

/// Effective store capabilities and local-closure support.
///
/// # Examples
///
/// ```
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use qubit_task::TaskExecutionService;
///
/// let service = TaskExecutionService::in_memory().await?;
/// assert!(service.capabilities().submit_local);
/// service.shutdown().await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaskServiceCapabilities {
    /// Capabilities declared by the selected store.
    pub store: StoreCapabilities,
    /// Whether local in-process handlers can be submitted.
    pub submit_local: bool,
}
