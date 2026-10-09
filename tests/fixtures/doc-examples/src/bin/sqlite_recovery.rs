// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Persists a typed task in SQLite and queries it after reopening the store.

use std::sync::Arc;

use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::model::ResourceCapacity;
use qubit_task::model::TaskState;
use qubit_task::store::SqliteTaskStore;
use qubit_task::store::TaskStore;

#[path = "../typed_support.rs"]
mod typed_support;

use typed_support::register_handler;
use typed_support::request;
use typed_support::SequentialIds;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::temp_dir().join(format!("qubit-task-typed-recovery-{}.sqlite", std::process::id()));
    let store = Arc::new(SqliteTaskStore::open(&path)?);
    let codecs = Arc::new(typed_support::codecs()?);
    let ids = Arc::new(SequentialIds::new(500));
    let mut builder = TaskExecutionServiceBuilder::new(store, codecs, ids)
        .capacity(ResourceCapacity { cpu_slots: 1, ..ResourceCapacity::default() });
    register_handler(&mut builder)?;
    let service = builder.build().await?;
    let accepted = service.submit(request(serde_json::json!({"rows": 4}), "import-4")).await?;
    service.shutdown().await?;
    drop(service);

    let store: Arc<dyn TaskStore> = Arc::new(SqliteTaskStore::open(&path)?);
    let mut reopened = TaskExecutionServiceBuilder::new(
        store,
        Arc::new(typed_support::codecs()?),
        Arc::new(SequentialIds::new(700)),
    );
    register_handler(&mut reopened)?;
    let recovered_service = reopened.build().await?;
    let recovered = recovered_service.get(accepted.id).await?
        .expect("the persisted task remains queryable");
    assert_eq!(recovered.state, TaskState::Succeeded);
    recovered_service.shutdown().await?;
    drop(recovered_service);

    remove_sqlite_files(&path);
    Ok(())
}

fn remove_sqlite_files(path: &std::path::Path) {
    let _ = std::fs::remove_file(path);
    let mut lock_path = path.as_os_str().to_owned();
    lock_path.push(".owner.lock");
    let _ = std::fs::remove_file(std::path::PathBuf::from(lock_path));
    let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
    let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
}
