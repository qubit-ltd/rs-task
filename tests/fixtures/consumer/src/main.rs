// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Exercises a separately compiled task handler through the public service API.

mod fixture_ids;

use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use qubit_task::TaskExecutionService;
use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::model::TaskId;
use qubit_task::model::TaskState;
use qubit_task::model::TaskSummary;
use qubit_task::store::MemoryTaskStore;
use qubit_task_fixture_provider::FixtureHandler;
use qubit_task_fixture_provider::FixturePayload;
use qubit_task_fixture_provider::codec_registry;
use qubit_task_fixture_provider::descriptor;
use qubit_task_fixture_provider::request;

use crate::fixture_ids::FixtureIds;

/// Runs a task through the public API from the separately compiled consumer.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut builder = TaskExecutionServiceBuilder::new(
        Arc::new(MemoryTaskStore::new(16)),
        codec_registry()?,
        Arc::new(FixtureIds::new(1)),
    );
    builder.handlers_mut().register::<FixturePayload, _>(
        descriptor("fixture")?,
        Arc::new(FixtureHandler),
    )?;
    let service = builder.build().await?;
    let accepted = service
        .submit(request(
            "fixture",
            serde_json::json!({"value": "input"}),
            "fixture-run-1".into(),
        ))
        .await?;
    let completed = wait_for_terminal(&service, accepted.id).await?;
    assert!(matches!(completed.state, TaskState::Succeeded));
    service.shutdown().await?;
    Ok(())
}

/// Polls the service until the task reaches a terminal state or the deadline expires.
async fn wait_for_terminal(
    service: &TaskExecutionService,
    id: TaskId,
) -> Result<TaskSummary, Box<dyn Error>> {
    for _ in 0..1_000 {
        if let Some(summary) = service.get(id).await? {
            if summary.state.is_terminal() {
                return Ok(summary);
            }
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    Err("fixture task did not finish".into())
}
