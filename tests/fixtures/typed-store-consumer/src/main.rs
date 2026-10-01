// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Exercises the public typed-store API from a separately compiled crate.

#[cfg(feature = "sqlite")]
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use qubit_id::Id;
use qubit_metadata::Metadata;
use qubit_model_metadata::metadata::ModelIdBuf;
use qubit_task::model::TaskId;
use qubit_task::model::TaskState;
use qubit_task::model::TaskStateKind;
use qubit_task::model::ResourceRequest;
use qubit_task::model::StartCommand;
use qubit_task::model::StoredPayload;
use qubit_task::model::StoredTaskRequest;
use qubit_task::model::TaskQuery;
use qubit_task::model::TransitionCommand;
use qubit_task::store::MemoryTaskStore;
use qubit_task::store::TaskStore;

#[cfg(feature = "sqlite")]
use qubit_task::store::SqliteTaskStore;

/// Owns one temporary database namespace and removes it when the fixture exits.
#[cfg(feature = "sqlite")]
struct SqliteFixture {
    directory: PathBuf,
    database: PathBuf,
}

#[cfg(feature = "sqlite")]
impl SqliteFixture {
    fn new() -> std::io::Result<Self> {
        let directory = std::env::temp_dir().join(format!(
            "rs-task-typed-store-consumer-{}-{}",
            std::process::id(),
            now_ms()
        ));
        std::fs::create_dir(&directory)?;
        let database = directory.join("tasks.sqlite");
        Ok(Self { directory, database })
    }
}

#[cfg(feature = "sqlite")]
impl Drop for SqliteFixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.directory).expect("remove this fixture's disposable database directory");
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    exercise_store(Arc::new(MemoryTaskStore::new(16)), task_id(700)).await?;

    #[cfg(feature = "sqlite")]
    {
        let fixture = SqliteFixture::new()?;
        let store = Arc::new(SqliteTaskStore::open_next(&fixture.database)?);
        exercise_store(store, task_id(800)).await?;
        drop(fixture);
    }

    Ok(())
}

async fn exercise_store(store: Arc<dyn TaskStore>, id: TaskId) -> Result<(), Box<dyn std::error::Error>> {
    let owner = store.acquire_owner().await?;
    let request = stored_request();
    let accepted = store.accept_encoded(id, request.clone()).await?;
    assert!(accepted.created, "the first acceptance creates a record");
    assert_eq!(accepted.summary.state, TaskState::Queued);

    let replay = store.accept_encoded(task_id(id.into_id().value() + 1), request).await?;
    assert!(!replay.created, "an identical idempotency key reuses its record");
    assert_eq!(replay.summary.id, id);

    let stored = store
        .get_encoded_task(id)
        .await?
        .expect("the accepted task is readable");
    assert_eq!(stored.request.payload.bytes, b"typed-store-smoke");
    assert_eq!(stored.summary.state, TaskState::Queued);

    let started = store
        .start_encoded(StartCommand {
            id,
            expected_state_version: accepted.summary.state_version,
            started_at_ms: now_ms(),
        })
        .await?;
    assert_eq!(started.state, TaskState::Running);
    assert_eq!(started.attempt, 1);

    let finished = store
        .transition_encoded(TransitionCommand {
            id,
            expected_state_version: started.state_version,
            expected_attempt: started.attempt,
            state: TaskState::Succeeded,
            cancel_requested: false,
            cancel_error: None,
            output: None,
            finished_at_ms: Some(now_ms()),
        })
        .await?;
    assert_eq!(finished.state, TaskState::Succeeded);

    let page = store
        .list_encoded(TaskQuery {
            states: vec![TaskStateKind::Succeeded],
            category: Some("fixture-smoke".into()),
            limit: 10,
            ..TaskQuery::default()
        })
        .await?;
    assert!(page.records.iter().any(|summary| summary.id == id));

    store.release_owner(owner).await?;
    Ok(())
}

fn stored_request() -> StoredTaskRequest {
    StoredTaskRequest {
        kind_id: "fixture.smoke".into(),
        category: Some("fixture-smoke".into()),
        payload: StoredPayload {
            type_id: ModelIdBuf::try_from("fixture.TypedStoreSmoke").expect("valid model ID"),
            schema_version: 1,
            codec_id: "fixture.raw-bytes".into(),
            bytes: b"typed-store-smoke".to_vec(),
        },
        metadata: Metadata::new(),
        resource_limit: ResourceRequest::default(),
        correlation_key: Some("external-consumer".into()),
        idempotency_key: Some("typed-store-smoke".into()),
    }
}

fn task_id(value: u64) -> TaskId {
    TaskId::from_id(Id::new(value))
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after the Unix epoch")
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}
