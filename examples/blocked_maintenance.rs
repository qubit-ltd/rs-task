// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Reviews old blocked tasks and explicitly abandons only unchanged revisions.

use std::num::NonZeroUsize;
use std::time::Duration;

use qubit_task::TaskExecutionService;
use qubit_task::model::TaskQuery;
use qubit_task::model::TaskStateKind;
use qubit_task::service::TaskServiceError;
use qubit_task::store::StoreError;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
    runtime.block_on(run())
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let service = TaskExecutionService::in_memory().await?;
    let cutoff_ms = now_ms().saturating_sub(Duration::from_secs(24 * 60 * 60).as_millis() as u64);
    let mut cursor = None;
    let mut abandoned = 0_usize;
    let mut conflicts = 0_usize;

    loop {
        let page = service
            .list(TaskQuery {
                states: vec![TaskStateKind::Blocked],
                limit: 100,
                after: cursor,
                ..TaskQuery::default()
            })
            .await?;
        cursor = page.next;
        for task in page.records {
            if task.accepted_at_ms > cutoff_ms {
                continue;
            }
            match service.abandon_blocked(task.id, task.state_version).await {
                Ok(_) => abandoned += 1,
                Err(TaskServiceError::Store(StoreError::Conflict)) => conflicts += 1,
                Err(TaskServiceError::NotBlocked { .. }) => conflicts += 1,
                Err(error) => return Err(error.into()),
            }
        }
        if cursor.is_none() {
            break;
        }
    }

    let pruned = service
        .prune_terminal_before(cutoff_ms, NonZeroUsize::new(100).expect("100 is nonzero"))
        .await?;
    eprintln!("abandoned={abandoned}, changed_during_review={conflicts}, pruned_terminal={pruned}");
    service.shutdown().await?;
    Ok(())
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
