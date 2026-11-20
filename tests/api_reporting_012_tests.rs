// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;

#[cfg(feature = "event-bus")]
use qubit_event_bus::model::Topic;
use qubit_progress::AsyncProgress;
use qubit_progress::AsyncReporter;
use qubit_progress::Event;
use qubit_progress::Metric;
use qubit_progress::MetricDelta;
use qubit_progress::ReportFuture;
#[cfg(feature = "event-bus")]
use qubit_task::event::TaskEvent;
use qubit_task::service::TaskProgressError;
use qubit_task::service::TaskProgressExt;

struct NoopReporter;

impl AsyncReporter for NoopReporter {
    fn report<'a>(&'a self, _event: &'a Event) -> ReportFuture<'a> {
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test]
async fn test_public_progress_extension_reports_metric_delta() {
    let mut progress = AsyncProgress::builder_arc(Arc::new(NoopReporter))
        .metric(Metric::new("items", "Items"))
        .start_async()
        .await
        .expect("progress configuration is valid");

    progress
        .report_metric_delta("items", MetricDelta::new().started(1).succeeded(1))
        .await
        .expect("the configured metric is reported");
}

#[tokio::test]
async fn test_public_progress_extension_classifies_unknown_metrics() {
    let mut progress = AsyncProgress::builder_arc(Arc::new(NoopReporter))
        .metric(Metric::new("items", "Items"))
        .start_async()
        .await
        .expect("progress configuration is valid");

    let error = progress
        .report_metric_delta("missing", MetricDelta::new().started(1))
        .await
        .expect_err("unknown metrics are rejected");

    assert!(matches!(error, TaskProgressError::UnknownMetric(id) if id == "missing"));
}

#[cfg(feature = "event-bus")]
#[test]
fn test_task_event_exposes_the_lifecycle_topic() {
    let topic: Topic<TaskEvent> = TaskEvent::topic();

    assert_eq!(topic.name(), "task.lifecycle");
}
