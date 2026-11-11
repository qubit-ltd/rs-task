// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::num::NonZeroUsize;

use qubit_id::Id;
use qubit_metadata::Metadata;
use qubit_model_id::ModelIdBuf;
use qubit_task::model::OwnerEpoch;
use qubit_task::model::StoreCapabilities;
use qubit_task::model::TaskState;
use qubit_task::model::TaskStateCheckpoint;
use qubit_task::model::typed::AcceptOutcome;
use qubit_task::model::typed::ProgressCommand;
use qubit_task::model::typed::ResourceRequest;
use qubit_task::model::typed::StartCommand;
use qubit_task::model::typed::StoredPayload;
use qubit_task::model::typed::StoredTask;
use qubit_task::model::typed::StoredTaskRequest;
use qubit_task::model::typed::TaskCursor;
use qubit_task::model::typed::TaskId;
use qubit_task::model::typed::TaskPage as TypedTaskPage;
use qubit_task::model::typed::TaskQuery;
use qubit_task::model::typed::TaskSummary;
use qubit_task::model::typed::TransitionCommand;
use qubit_task::store::StoreError;
use qubit_task::store::TaskFuture;
use qubit_task::store::TaskStore;

struct NoOutboxStore;

impl TaskStore for NoOutboxStore {
    fn capabilities(&self) -> StoreCapabilities {
        panic!("required method should not be called")
    }

    fn accept_encoded<'a>(
        &'a self,
        _id: TaskId,
        _request: StoredTaskRequest,
    ) -> TaskFuture<'a, Result<AcceptOutcome, StoreError>> {
        panic!("required method should not be called")
    }

    fn get_encoded_task<'a>(&'a self, _id: TaskId) -> TaskFuture<'a, Result<Option<StoredTask>, StoreError>> {
        Box::pin(async move {
            if _id.into_id().value() == 44 {
                return Err(StoreError::Failure("legacy get failure".to_owned()));
            }
            let request = StoredTaskRequest {
                kind_id: "legacy.kind".to_owned(),
                category: None,
                payload: StoredPayload {
                    type_id: ModelIdBuf::parse("example.Payload").expect("valid model ID"),
                    schema_version: 1,
                    codec_id: "legacy.codec".to_owned(),
                    bytes: vec![1, 2],
                },
                metadata: Metadata::new(),
                resource_limit: ResourceRequest::default(),
                correlation_key: None,
                idempotency_key: None,
            };
            Ok(Some(StoredTask {
                request,
                summary: TaskSummary {
                    id: _id,
                    kind_id: "legacy.kind".to_owned(),
                    category: None,
                    payload_type_id: "example.Payload".to_owned(),
                    payload_schema_version: 1,
                    payload_codec_id: "legacy.codec".to_owned(),
                    metadata: Metadata::new(),
                    resource_limit: ResourceRequest::default(),
                    correlation_key: None,
                    idempotency_key: None,
                    state: TaskState::Queued,
                    cancel_requested: false,
                    cancel_error: None,
                    state_version: 0,
                    attempt: 0,
                    retry_not_before_ms: None,
                    accepted_at_ms: 1,
                    started_at_ms: None,
                    finished_at_ms: None,
                    progress: None,
                    output: None,
                },
            }))
        })
    }

    fn get_state_checkpoint<'a>(
        &'a self,
        _id: TaskId,
    ) -> TaskFuture<'a, Result<Option<TaskStateCheckpoint>, StoreError>> {
        Box::pin(async { Ok(None) })
    }

    fn start_encoded<'a>(&'a self, _command: StartCommand) -> TaskFuture<'a, Result<TaskSummary, StoreError>> {
        panic!("required method should not be called")
    }

    fn transition_encoded<'a>(
        &'a self,
        _command: TransitionCommand,
    ) -> TaskFuture<'a, Result<TaskSummary, StoreError>> {
        panic!("required method should not be called")
    }

    fn update_progress<'a>(&'a self, _command: ProgressCommand) -> TaskFuture<'a, Result<TaskSummary, StoreError>> {
        panic!("required method should not be called")
    }

    fn list_encoded<'a>(&'a self, _query: TaskQuery) -> TaskFuture<'a, Result<TypedTaskPage, StoreError>> {
        panic!("required method should not be called")
    }

    fn list_ready_queued<'a>(
        &'a self,
        _after: Option<TaskCursor>,
        _limit: NonZeroUsize,
        _now_ms: u64,
    ) -> TaskFuture<'a, Result<TypedTaskPage, StoreError>> {
        panic!("required method should not be called")
    }

    fn next_retry_deadline<'a>(&'a self, _now_ms: u64) -> TaskFuture<'a, Result<Option<u64>, StoreError>> {
        panic!("required method should not be called")
    }

    fn prune_terminal_before<'a>(
        &'a self,
        _finished_before_ms: u64,
        _max_rows: NonZeroUsize,
    ) -> TaskFuture<'a, Result<usize, StoreError>> {
        panic!("required method should not be called")
    }

    fn acquire_owner<'a>(&'a self) -> TaskFuture<'a, Result<OwnerEpoch, StoreError>> {
        panic!("required method should not be called")
    }

    fn release_owner<'a>(&'a self, _epoch: OwnerEpoch) -> TaskFuture<'a, Result<(), StoreError>> {
        panic!("required method should not be called")
    }
}

fn typed_id(value: u64) -> TaskId {
    TaskId::from_id(Id::new(value))
}

#[tokio::test]
async fn test_outbox_defaults_report_unsupported_for_supplied_inputs() {
    let store = NoOutboxStore;

    assert!(matches!(
        store.enable_event_outbox().await,
        Err(StoreError::UnsupportedCapability)
    ));
    assert!(matches!(
        store.list_event_outbox(0).await,
        Err(StoreError::UnsupportedCapability)
    ));
    assert!(matches!(
        store.mark_event_published(typed_id(43), 7).await,
        Err(StoreError::UnsupportedCapability)
    ));
}

#[tokio::test]
async fn test_legacy_store_summary_default_preserves_encoded_read_error() {
    let store = NoOutboxStore;
    assert!(matches!(
        store.get_summary(typed_id(44)).await,
        Err(StoreError::Failure(message)) if message == "legacy get failure"
    ));
}

#[tokio::test]
async fn test_legacy_store_summary_default_derives_summary_from_encoded_task() {
    let store = NoOutboxStore;
    let id = typed_id(43);
    let task = store.get_encoded_task(id).await.unwrap().unwrap();
    assert_eq!(store.get_summary(id).await.unwrap(), Some(task.summary));
}
