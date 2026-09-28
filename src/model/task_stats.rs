// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use super::ResourceSnapshot;

/// Aggregate task counts and resource capacity suitable for service monitoring.
///
/// Store counts and resource usage are collected consecutively and are not an
/// atomic snapshot across both components.
///
/// # Examples
///
/// ```
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     use qubit_task::TaskExecutionService;
///
///     let service = TaskExecutionService::in_memory().await?;
///     let stats = service.stats().await?;
///     assert_eq!(stats.queued, 0);
///     service.shutdown().await?;
///     Ok(())
/// }
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TaskStats {
    /// Number of tasks waiting for resources.
    pub queued: usize,
    /// Number of currently executing tasks.
    pub running: usize,
    /// Number of tasks requiring intervention.
    pub blocked: usize,
    /// Number of retained terminal records.
    pub terminal: usize,
    /// Resource snapshot at the time of collection.
    pub resources: ResourceSnapshot,
}
