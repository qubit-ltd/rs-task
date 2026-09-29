// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Seeds durable queued work and verifies that a new SQLite service recovers it.

use std::path::PathBuf;
use std::sync::Arc;

use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::handler::TaskContext;
use qubit_task::handler::TaskHandler;
use qubit_task::handler::TaskHandlerDescriptor;
use qubit_task::handler::TaskRunOutcome;
use qubit_task::handler::TaskRunResult;
use qubit_task::model::OwnerEpoch;
use qubit_task::model::TaskId;
use qubit_task::model::TaskOutput;
use qubit_task::model::TaskRequest;
use qubit_task::model::TaskState;
use qubit_task::store::SqliteTaskStore;
use qubit_task::store::TaskFuture;
use qubit_task::store::TaskStore;

/// Handler registered by the new service to resume persisted imports.
struct ImportV1;

impl TaskHandler for ImportV1 {
    fn descriptor(&self) -> TaskHandlerDescriptor {
        TaskHandlerDescriptor {
            task_type: "documented-import".into(),
            version: "1".into(),
        }
    }

    fn run<'a>(&'a self, payload: &'a [u8], _context: TaskContext) -> TaskFuture<'a, TaskRunResult> {
        Box::pin(async move {
            Ok(TaskRunOutcome::Succeeded(TaskOutput {
                summary: format!("recovered {} bytes", payload.len()).into_bytes(),
            }))
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::temp_dir().join(format!("qubit-task-doc-recovery-{}.sqlite", TaskId::generate()));
    let task_id = TaskId::generate();

    // Model a task accepted by the previous process but not yet executed.
    let store = SqliteTaskStore::open(&path)?;
    let epoch: OwnerEpoch = store.acquire_owner().await?;
    let request = TaskRequest::new("documented-import", "1", b"rows".to_vec())
        .with_idempotency_key("documented-import-1");
    let _ = store.accept(task_id, request).await?;
    store.release_owner(epoch).await?;
    drop(store);

    let service = TaskExecutionServiceBuilder::recoverable_sqlite(&path)?
        .register_handler(Arc::new(ImportV1))?
        .require_recovery(true)
        .build()
        .await?;
    let recovered = service.wait(task_id).await?;
    assert!(matches!(recovered.state, TaskState::Succeeded));
    assert_eq!(recovered.output.expect("summary is persisted").summary, b"recovered 4 bytes");
    service.shutdown().await?;

    remove_sqlite_files(&path);
    Ok(())
}

/// Removes the temporary database and its ownership/SQLite sidecar files.
fn remove_sqlite_files(path: &std::path::Path) {
    let _ = std::fs::remove_file(path);
    let mut lock_path = path.as_os_str().to_owned();
    lock_path.push(".owner.lock");
    let _ = std::fs::remove_file(PathBuf::from(lock_path));
    let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
    let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
}
