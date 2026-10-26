// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::num::NonZeroUsize;

use qubit_id::Id;
use qubit_task::model::OwnerEpoch;
use qubit_task::model::StoreCapabilities;
use qubit_task::model::TaskStateCheckpoint;
use qubit_task::model::typed::AcceptOutcome;
use qubit_task::model::typed::ProgressCommand;
use qubit_task::model::typed::StartCommand;
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
        panic!("required method should not be called")
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
