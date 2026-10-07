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
    use crate::model::legacy::AcceptOutcome;
    use crate::model::legacy::RecoveryPage;
    use crate::model::legacy::TaskCursor as LegacyTaskCursor;
    use crate::model::legacy::TaskId as LegacyTaskId;
    use crate::model::legacy::TaskPage;
    use crate::model::legacy::TaskQuery as LegacyTaskQuery;
    use crate::model::legacy::TaskRecord;
    use crate::model::legacy::TaskRequest;
    use crate::model::legacy::TaskSummary as LegacyTaskSummary;
    use crate::model::legacy::TransitionCommand as LegacyTransitionCommand;
    use crate::model::next::ProgressCommand;
    use crate::model::next::StartCommand;
    use crate::model::next::StoredPayload;
    use crate::model::next::StoredTask;
    use crate::model::next::StoredTaskRequest;
    use crate::model::next::TaskCursor;
    use crate::model::next::TaskId;
    use crate::model::next::TaskPage as TypedTaskPage;
    use crate::model::next::TaskQuery;
    use crate::model::next::TaskSummary;
    use crate::model::next::TransitionCommand;
    use crate::store::LegacyTaskStore;
    use crate::store::StoreError;
    use crate::store::TaskFuture;
    use crate::store::TaskStore;

    struct LegacyOnlyStore;

    impl LegacyTaskStore for LegacyOnlyStore {
        fn capabilities(&self) -> StoreCapabilities {
            panic!("required method should not be called")
        }

        fn accept<'a>(
            &'a self,
            _id: LegacyTaskId,
            _request: TaskRequest,
        ) -> TaskFuture<'a, Result<AcceptOutcome, StoreError>> {
            panic!("required method should not be called")
        }

        fn get_by_idempotency_key<'a>(
            &'a self,
            _key: &'a str,
        ) -> TaskFuture<'a, Result<Option<TaskRecord>, StoreError>> {
            panic!("required method should not be called")
        }

        fn get_summary_by_idempotency_key<'a>(
            &'a self,
            _key: &'a str,
        ) -> TaskFuture<'a, Result<Option<LegacyTaskSummary>, StoreError>> {
            panic!("required method should not be called")
        }

        fn transition<'a>(
            &'a self,
            _command: LegacyTransitionCommand,
        ) -> TaskFuture<'a, Result<LegacyTaskSummary, StoreError>> {
            panic!("required method should not be called")
        }

        fn get_summary<'a>(
            &'a self,
            _id: LegacyTaskId,
        ) -> TaskFuture<'a, Result<Option<LegacyTaskSummary>, StoreError>> {
            panic!("required method should not be called")
        }

        fn get<'a>(&'a self, _id: LegacyTaskId) -> TaskFuture<'a, Result<Option<TaskRecord>, StoreError>> {
            panic!("required method should not be called")
        }

        fn list<'a>(&'a self, _query: LegacyTaskQuery) -> TaskFuture<'a, Result<TaskPage, StoreError>> {
            panic!("required method should not be called")
        }

        fn acquire_owner<'a>(&'a self) -> TaskFuture<'a, Result<OwnerEpoch, StoreError>> {
            panic!("required method should not be called")
        }

        fn has_unfinished_over_limit<'a>(&'a self, _limit: usize) -> TaskFuture<'a, Result<bool, StoreError>> {
            panic!("required method should not be called")
        }

        fn scan_unfinished<'a>(
            &'a self,
            _cursor: Option<LegacyTaskCursor>,
        ) -> TaskFuture<'a, Result<RecoveryPage, StoreError>> {
            panic!("required method should not be called")
        }

        fn release_owner<'a>(&'a self, _epoch: OwnerEpoch) -> TaskFuture<'a, Result<(), StoreError>> {
            panic!("required method should not be called")
        }
    }

    struct NoOutboxStore;

    impl TaskStore for NoOutboxStore {
        fn capabilities(&self) -> StoreCapabilities {
            panic!("required method should not be called")
        }

        fn accept_encoded<'a>(
            &'a self,
            _id: TaskId,
            _request: StoredTaskRequest,
        ) -> TaskFuture<'a, Result<crate::model::next::AcceptOutcome, StoreError>> {
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

    fn stored_request() -> StoredTaskRequest {
        StoredTaskRequest {
            kind_id: "test.kind".to_owned(),
            category: None,
            payload: StoredPayload {
                type_id: qubit_model_id::ModelIdBuf::parse("test.Payload").expect("valid model ID"),
                schema_version: 1,
                codec_id: "test.codec".to_owned(),
                bytes: vec![1, 2, 3],
            },
            metadata: qubit_metadata::Metadata::default(),
            resource_limit: crate::model::next::ResourceRequest::default(),
            correlation_key: None,
            idempotency_key: None,
        }
    }

    #[tokio::test]
    async fn legacy_typed_defaults_report_unsupported_for_supplied_inputs() {
        let store = LegacyOnlyStore;
        let id = typed_id(42);
        let request = stored_request();
        let transition = TransitionCommand {
            id,
            expected_state_version: 3,
            expected_attempt: 2,
            retry_not_before_ms: Some(10),
            state: crate::model::TaskState::Queued,
            cancel_requested: false,
            cancel_error: None,
            finished_at_ms: None,
            output: None,
        };

        assert!(matches!(
            store.accept_encoded(id, request).await,
            Err(StoreError::UnsupportedCapability)
        ));
        assert!(matches!(
            store.get_encoded_task(id).await,
            Err(StoreError::UnsupportedCapability)
        ));
        assert!(matches!(
            store
                .start_encoded(StartCommand {
                    id,
                    expected_state_version: 4,
                    started_at_ms: 12,
                })
                .await,
            Err(StoreError::UnsupportedCapability)
        ));
        assert!(matches!(
            store.transition_encoded(transition).await,
            Err(StoreError::UnsupportedCapability)
        ));
        assert!(matches!(
            store
                .update_progress(ProgressCommand::new(id, 2, 5, None, Vec::new(), 15))
                .await,
            Err(StoreError::UnsupportedCapability)
        ));
        assert!(matches!(
            store.list_encoded(TaskQuery::default()).await,
            Err(StoreError::UnsupportedCapability)
        ));
        assert!(matches!(
            store
                .list_ready_queued(Some(TaskCursor::new(9, id)), NonZeroUsize::new(3).unwrap(), 20)
                .await,
            Err(StoreError::UnsupportedCapability)
        ));
        assert!(matches!(
            store.next_retry_deadline(21).await,
            Err(StoreError::UnsupportedCapability)
        ));
        assert!(matches!(
            store
                .prune_typed_terminal_before(22, NonZeroUsize::new(4).unwrap())
                .await,
            Err(StoreError::UnsupportedCapability)
        ));
        assert!(matches!(
            store.prune_terminal_before(23, NonZeroUsize::new(5).unwrap()).await,
            Err(StoreError::UnsupportedCapability)
        ));
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
