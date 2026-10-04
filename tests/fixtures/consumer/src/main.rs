// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Exercises a separately compiled task handler through the public service API.

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use qubit_id::Id;
use qubit_id::IdGenerationError;
use qubit_id::IdGenerator;
use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::model::TaskId;
use qubit_task::model::TaskState;
use qubit_task::store::MemoryTaskStore;
use qubit_task_fixture_provider::FixtureHandler;
use qubit_task_fixture_provider::FixturePayload;
use qubit_task_fixture_provider::codec_registry;
use qubit_task_fixture_provider::descriptor;
use qubit_task_fixture_provider::request;

struct FixtureIds(AtomicU64);

impl IdGenerator<Id, IdGenerationError> for FixtureIds {
    fn generate(&self) -> Result<Id, IdGenerationError> {
        Ok(Id::new(self.0.fetch_add(1, Ordering::Relaxed)))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut builder = TaskExecutionServiceBuilder::new(
        Arc::new(MemoryTaskStore::new(16)),
        codec_registry()?,
        Arc::new(FixtureIds(AtomicU64::new(1))),
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

async fn wait_for_terminal(
    service: &qubit_task::TaskExecutionService,
    id: TaskId,
) -> Result<qubit_task::model::TaskSummary, Box<dyn std::error::Error>> {
    for _ in 0..1_000 {
        if let Some(summary) = service.get(id).await? {
            if summary.state.is_terminal() {
                return Ok(summary);
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    Err("fixture task did not finish".into())
}
