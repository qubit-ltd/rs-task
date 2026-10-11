// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Queries task summaries by category with the typed cursor contract.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use qubit_codec::ValueBytesCodecRegistry;
use qubit_id::Id;
use qubit_id::IdGenerationError;
use qubit_id::IdGenerator;
use qubit_task::service::TaskServiceError;
use qubit_task::store::MemoryTaskStore;
use qubit_task::store::StoreError;
use qubit_task::TaskExecutionService;
use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::model::TaskId;
use qubit_task::model::TaskQuery;
use qubit_task::model::TaskState;

/// Call after repairing the handler or codec configuration and restarting.
async fn resume_after_repair(service: &TaskExecutionService, id: TaskId) -> Result<(), Box<dyn std::error::Error>> {
    let Some(summary) = service.get(id).await? else {
        return Ok(());
    };
    if matches!(summary.state, TaskState::Blocked { .. }) {
        if summary.cancel_requested {
            eprintln!("{} still has a pending cancellation", id.to_padded_decimal());
            return Ok(());
        }
        match service.resume_blocked(id, summary.state_version).await {
            Ok(_) => println!("{} queued for retry", id.to_padded_decimal()),
            Err(TaskServiceError::Store(StoreError::Conflict)) => {
                eprintln!("{} changed; read its summary again", id.to_padded_decimal());
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

struct SequentialIds(AtomicU64);

impl IdGenerator for SequentialIds {
    fn generate(&self) -> Result<Id, IdGenerationError> {
        Ok(Id::new(self.0.fetch_add(1, Ordering::Relaxed)))
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    runtime.block_on(async {
        let service = TaskExecutionServiceBuilder::new(
            Arc::new(MemoryTaskStore::new(256)),
            Arc::new(ValueBytesCodecRegistry::empty()),
            Arc::new(SequentialIds(AtomicU64::new(1))),
        )
        .build()
        .await?;

        let mut query = TaskQuery {
            category: Some("image-processing".into()),
            limit: 50,
            ..TaskQuery::default()
        };
        loop {
            let page = service.query(query.clone()).await?;
            for task in page.records {
                println!("{} {:?}", task.id.to_padded_decimal(), task.state);
            }
            let Some(next) = page.next else { break };
            query.after = Some(next);
        }
        let _operator_recovery_example = resume_after_repair;
        service.shutdown().await?;
        Ok::<(), Box<dyn std::error::Error>>(())
    })
}
