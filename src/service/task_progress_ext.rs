// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Convenience methods for applying and persisting task progress updates.

use std::future::Future;

use qubit_progress::AsyncProgress;
use qubit_progress::EmissionError;
use qubit_progress::MetricDelta;
use qubit_progress::MetricError;

/// Applies a metric update and persists the resulting progress snapshot.
pub trait TaskProgressExt {
    /// Applies `delta` to a configured metric, then persists the current
    /// snapshot.
    ///
    /// If persistence fails, the delta remains applied locally. A retry should
    /// report the current snapshot again rather than applying the delta twice.
    ///
    /// # Parameters
    ///
    /// - `metric_id`: ID of a metric configured for this progress operation.
    /// - `delta`: Metric update to apply before reporting.
    ///
    /// # Returns
    ///
    /// A future that completes after the updated snapshot is reported.
    ///
    /// # Errors
    ///
    /// Returns [`TaskProgressError::UnknownMetric`] if the metric is absent,
    /// [`TaskProgressError::Metric`] if the update is invalid, or
    /// [`TaskProgressError::Emission`] if reporting fails.
    fn report_metric_delta<'a>(
        &'a mut self,
        metric_id: &'a str,
        delta: MetricDelta,
    ) -> impl Future<Output = Result<(), TaskProgressError>> + Send + 'a;
}

impl TaskProgressExt for AsyncProgress<'_> {
    fn report_metric_delta<'a>(
        &'a mut self,
        metric_id: &'a str,
        delta: MetricDelta,
    ) -> impl Future<Output = Result<(), TaskProgressError>> + Send + 'a {
        async move {
            let metric = self
                .metric(metric_id)
                .ok_or_else(|| TaskProgressError::UnknownMetric(metric_id.to_owned()))?;
            metric.apply_delta(delta).map_err(TaskProgressError::Metric)?;
            self.report_async().await.map_err(TaskProgressError::Emission)
        }
    }
}

/// Failure while applying and reporting a task progress update.
#[derive(Debug, thiserror::Error)]
#[must_use]
pub enum TaskProgressError {
    /// The requested metric is not part of this progress operation.
    #[error("unknown task progress metric `{0}`")]
    UnknownMetric(String),
    /// The metric rejected the requested delta.
    #[error("the metric rejected the progress delta: {0}")]
    Metric(#[source] MetricError),
    /// The updated snapshot could not be reported.
    #[error("the progress snapshot could not be reported: {0}")]
    Emission(#[source] EmissionError),
}
