// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
mod default_method_tests {
    use std::num::NonZeroUsize;

    use crate::model::OwnerEpoch;
    use crate::model::StoreCapabilities;
    use crate::model::typed::ProgressCommand;
    use crate::model::typed::StartCommand;
    use crate::model::typed::StoredTask;
    use crate::model::typed::StoredTaskRequest;
    use crate::model::typed::TaskCursor;
    use crate::model::typed::TaskId;
    use crate::model::typed::TaskPage as TypedTaskPage;
    use crate::model::typed::TaskQuery;
    use crate::model::typed::TaskSummary;
    use crate::model::typed::TransitionCommand;
    use crate::store::StoreError;
    use crate::store::TaskFuture;
    use crate::store::TaskStore;
    struct NoOutboxStore;

    impl TaskStore for NoOutboxStore {
        fn capabilities(&self) -> StoreCapabilities {
            panic!("required method should not be called")
        }

        fn accept_encoded<'a>(
            &'a self,
            _id: TaskId,
            _request: StoredTaskRequest,
        ) -> TaskFuture<'a, Result<crate::model::typed::AcceptOutcome, StoreError>> {
            panic!("required method should not be called")
        }

        fn get_encoded_task<'a>(&'a self, _id: TaskId) -> TaskFuture<'a, Result<Option<StoredTask>, StoreError>> {
            panic!("required method should not be called")
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
        TaskId::from_id(qubit_id::Id::new(value))
    }

    #[tokio::test]
    async fn outbox_defaults_report_unsupported_for_supplied_inputs() {
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
}
