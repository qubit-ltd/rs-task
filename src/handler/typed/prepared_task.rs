// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use crate::handler::TaskRunResult;
use crate::handler::typed::TypedTaskContext;
use crate::store::TaskFuture;

type RunPrepared = Box<dyn FnOnce(TypedTaskContext) -> TaskFuture<'static, TaskRunResult> + Send>;

/// A payload that passed routing, schema, codec, and Rust value-type checks.
pub struct PreparedTask {
    run: RunPrepared,
}

impl PreparedTask {
    /// Creates a prepared task from the registry's erased handler adapter.
    pub(crate) fn new(run: RunPrepared) -> Self {
        Self { run }
    }

    /// Starts the already validated attempt using its task context.
    #[must_use]
    pub fn run(self, context: TypedTaskContext) -> TaskFuture<'static, TaskRunResult> {
        (self.run)(context)
    }
}

impl std::fmt::Debug for PreparedTask {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("PreparedTask").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    use qubit_progress::AsyncReporter;
    use qubit_progress::Event;
    use qubit_progress::ReportFuture;

    use super::PreparedTask;
    use crate::handler::typed::TypedTaskContext;

    struct NoopReporter;

    impl AsyncReporter for NoopReporter {
        fn report<'a>(&'a self, _event: &'a Event) -> ReportFuture<'a> {
            Box::pin(async { Ok(()) })
        }
    }

    #[test]
    fn debug_shows_the_prepared_task_type_without_exposing_handler_state() {
        let task = PreparedTask::new(Box::new(|_| {
            Box::pin(async { Ok(crate::handler::TaskRunOutcome::Cancelled) })
        }));

        let formatted = format!("{task:?}");

        assert!(formatted.starts_with("PreparedTask"));
        assert!(formatted.contains(".."));
    }

    #[tokio::test]
    async fn run_invokes_the_prepared_handler_with_its_task_context() {
        let task_id = crate::model::typed::TaskId::from_id(qubit_id::Id::new(17));
        let context = TypedTaskContext::new(task_id, 3, Arc::new(AtomicBool::new(false)), Arc::new(NoopReporter));
        let task = PreparedTask::new(Box::new(move |context| {
            Box::pin(async move {
                assert_eq!(context.task_id(), task_id);
                assert_eq!(context.attempt(), 3);
                Ok(crate::handler::TaskRunOutcome::Cancelled)
            })
        }));

        let result = task.run(context).await.expect("prepared handler completes");

        assert!(matches!(result, crate::handler::TaskRunOutcome::Cancelled));
    }
}
