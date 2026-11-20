// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Errors returned while applying and reporting task progress updates.

use qubit_progress::EmissionError;
use qubit_progress::MetricError;

/// Failure while applying and reporting a task progress update.
///
/// # Examples
///
/// ```
/// use qubit_task::service::TaskProgressError;
///
/// fn inspect_progress_error(error: &TaskProgressError) {
///     match error {
///         TaskProgressError::UnknownMetric(metric_id) => {
///             assert!(!metric_id.is_empty());
///         }
///         TaskProgressError::Metric(_) | TaskProgressError::Emission(_) => {}
///     }
/// }
/// ```
#[derive(Debug, thiserror::Error)]
#[must_use]
pub enum TaskProgressError {
    /// The requested metric is not part of this progress operation.
    #[error("unknown task progress metric `{0}`")]
    UnknownMetric(
        /// Identifier of the metric that is not configured.
        String,
    ),
    /// The metric rejected the requested delta.
    #[error("the metric rejected the progress delta: {0}")]
    Metric(
        /// Validation failure returned by the metric.
        #[source]
        MetricError,
    ),
    /// The updated snapshot could not be reported.
    #[error("the progress snapshot could not be reported: {0}")]
    Emission(
        /// Failure returned while reporting the updated snapshot.
        #[source]
        EmissionError,
    ),
}
