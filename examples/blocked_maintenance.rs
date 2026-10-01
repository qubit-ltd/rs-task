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
use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::model::TaskQuery;
use qubit_task::store::MemoryTaskStore;

struct SequentialIds(AtomicU64);

impl qubit_id::IdGenerator for SequentialIds {
    fn generate(&self) -> Result<qubit_id::Id, qubit_id::IdGenerationError> {
        Ok(qubit_id::Id::new(self.0.fetch_add(1, Ordering::Relaxed)))
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
        service.shutdown().await?;
        Ok::<(), Box<dyn std::error::Error>>(())
    })
}
