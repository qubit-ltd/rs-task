// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use qubit_id::Id;
use qubit_progress::AsyncReporter;
use qubit_progress::Event;
use qubit_progress::ReportFuture;

use crate::handler::TaskRunOutcome;
use crate::handler::typed::PreparedTask;
use crate::handler::typed::TypedTaskContext;
use crate::model::typed::TaskId;

struct NoopReporter;

impl AsyncReporter for NoopReporter {
    fn report<'a>(&'a self, _event: &'a Event) -> ReportFuture<'a> {
        Box::pin(async { Ok(()) })
    }
}

#[test]
fn test_debug_shows_the_prepared_task_type_without_exposing_handler_state() {
    let task = PreparedTask::new(Box::new(|_| Box::pin(async { Ok(TaskRunOutcome::Cancelled) })));

    let formatted = format!("{task:?}");

    assert!(formatted.starts_with("PreparedTask"));
    assert!(formatted.contains(".."));
}

#[tokio::test]
async fn test_run_invokes_the_prepared_handler_with_its_task_context() {
    let task_id = TaskId::from_id(Id::new(17));
    let context = TypedTaskContext::new(task_id, 3, Arc::new(AtomicBool::new(false)), Arc::new(NoopReporter));
    let task = PreparedTask::new(Box::new(move |context| {
        Box::pin(async move {
            assert_eq!(context.task_id(), task_id);
            assert_eq!(context.attempt(), 3);
            Ok(TaskRunOutcome::Cancelled)
        })
    }));

    let result = task.run(context).await.expect("prepared handler completes");

    assert!(matches!(result, TaskRunOutcome::Cancelled));
}
