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
use qubit_progress::MetricDelta;

use super::TaskProgressError;

/// Applies a metric update and persists the resulting progress snapshot.
///
/// Implementations apply the delta before reporting. If reporting fails, the
/// local metric has already changed, so a retry should report the current
/// snapshot without applying the same delta again.
///
/// # Examples
///
/// ```
/// use qubit_progress::MetricDelta;
/// use qubit_task::service::TaskProgressError;
/// use qubit_task::service::TaskProgressExt;
///
/// async fn report<P: TaskProgressExt + ?Sized>(
///     progress: &mut P,
///     metric_id: &str,
///     delta: MetricDelta,
/// ) -> Result<(), TaskProgressError> {
///     progress.report_metric_delta(metric_id, delta).await
/// }
/// ```
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
    /// Applies a configured metric delta and reports the updated progress.
    ///
    /// The metric is changed before asynchronous reporting begins. If
    /// reporting fails, the caller should retry reporting the current snapshot
    /// without reapplying this delta.
    ///
    /// # Parameters
    ///
    /// * `metric_id` - Identifier of a metric configured on this progress.
    /// * `delta` - Update applied before the snapshot is reported.
    ///
    /// # Returns
    ///
    /// A future that resolves after the snapshot has been reported.
    ///
    /// # Errors
    ///
    /// Returns [`TaskProgressError::UnknownMetric`] when the metric is absent,
    /// [`TaskProgressError::Metric`] when it rejects the delta, or
    /// [`TaskProgressError::Emission`] when reporting fails.
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
