// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
// =============================================================================
/// Forwards [`TaskStore`](crate::store::TaskStore) methods to an inner store.
///
/// Use `delegate_task_store!(inner; except []);` inside a `TaskStore` impl.
/// Methods named in `except` must be implemented by the wrapper.
///
/// ```compile_fail
/// qubit_task::store::delegate_task_store!(inner; except [not_a_store_method]);
/// ```
#[macro_export]
macro_rules! delegate_task_store {
    ($field:ident; except []) => {
        $crate::delegate_task_store!(@emit $field; all);
    };
    ($field:ident; except [accept_encoded]) => {
        $crate::delegate_task_store!(@emit $field; no_accept);
    };
    (@emit $field:ident; all) => {
        $crate::delegate_task_store!(@emit $field; no_accept);
        fn accept_encoded<'a>(&'a self, id: $crate::model::typed::TaskId, request: $crate::model::typed::StoredTaskRequest) -> $crate::store::TaskFuture<'a, ::core::result::Result<$crate::model::typed::AcceptOutcome, $crate::store::StoreError>> {
            $crate::store::TaskStore::accept_encoded(&*self.$field, id, request)
        }
    };
    (@emit $field:ident; no_accept) => {
        fn enable_event_outbox<'a>(&'a self) -> $crate::store::TaskFuture<'a, ::core::result::Result<(), $crate::store::StoreError>> { $crate::store::TaskStore::enable_event_outbox(&*self.$field) }
        fn list_event_outbox<'a>(&'a self, limit: usize) -> $crate::store::TaskFuture<'a, ::core::result::Result<::std::vec::Vec<$crate::store::EventOutboxEntry>, $crate::store::StoreError>> { $crate::store::TaskStore::list_event_outbox(&*self.$field, limit) }
        fn mark_event_published<'a>(&'a self, id: $crate::model::typed::TaskId, version: u64) -> $crate::store::TaskFuture<'a, ::core::result::Result<(), $crate::store::StoreError>> { $crate::store::TaskStore::mark_event_published(&*self.$field, id, version) }
        fn quarantine_event_outbox_head<'a>(&'a self, id: $crate::model::typed::TaskId, version: u64, event: &'a str, reason: &'a str, last_error: &'a str) -> $crate::store::TaskFuture<'a, ::core::result::Result<(), $crate::store::StoreError>> { $crate::store::TaskStore::quarantine_event_outbox_head(&*self.$field, id, version, event, reason, last_error) }
        fn capabilities(&self) -> $crate::model::StoreCapabilities { $crate::store::TaskStore::capabilities(&*self.$field) }
        fn get_encoded_task<'a>(&'a self, id: $crate::model::typed::TaskId) -> $crate::store::TaskFuture<'a, ::core::result::Result<::core::option::Option<$crate::model::typed::StoredTask>, $crate::store::StoreError>> { $crate::store::TaskStore::get_encoded_task(&*self.$field, id) }
        fn get_summary<'a>(&'a self, id: $crate::model::typed::TaskId) -> $crate::store::TaskFuture<'a, ::core::result::Result<::core::option::Option<$crate::model::typed::TaskSummary>, $crate::store::StoreError>> { $crate::store::TaskStore::get_summary(&*self.$field, id) }
        fn get_state_checkpoint<'a>(&'a self, id: $crate::model::typed::TaskId) -> $crate::store::TaskFuture<'a, ::core::result::Result<::core::option::Option<$crate::model::typed::TaskStateCheckpoint>, $crate::store::StoreError>> { $crate::store::TaskStore::get_state_checkpoint(&*self.$field, id) }
        fn start_encoded<'a>(&'a self, command: $crate::model::typed::StartCommand) -> $crate::store::TaskFuture<'a, ::core::result::Result<$crate::model::typed::TaskSummary, $crate::store::StoreError>> { $crate::store::TaskStore::start_encoded(&*self.$field, command) }
        fn transition_encoded<'a>(&'a self, command: $crate::model::typed::TransitionCommand) -> $crate::store::TaskFuture<'a, ::core::result::Result<$crate::model::typed::TaskSummary, $crate::store::StoreError>> { $crate::store::TaskStore::transition_encoded(&*self.$field, command) }
        fn update_progress<'a>(&'a self, command: $crate::model::typed::ProgressCommand) -> $crate::store::TaskFuture<'a, ::core::result::Result<$crate::model::typed::TaskSummary, $crate::store::StoreError>> { $crate::store::TaskStore::update_progress(&*self.$field, command) }
        fn list_encoded<'a>(&'a self, query: $crate::model::typed::TaskQuery) -> $crate::store::TaskFuture<'a, ::core::result::Result<$crate::model::typed::TaskPage, $crate::store::StoreError>> { $crate::store::TaskStore::list_encoded(&*self.$field, query) }
        fn list_ready_queued<'a>(&'a self, after: ::core::option::Option<$crate::model::typed::TaskCursor>, limit: ::core::num::NonZeroUsize, now_ms: u64) -> $crate::store::TaskFuture<'a, ::core::result::Result<$crate::model::typed::TaskPage, $crate::store::StoreError>> { $crate::store::TaskStore::list_ready_queued(&*self.$field, after, limit, now_ms) }
        fn next_retry_deadline<'a>(&'a self, now_ms: u64) -> $crate::store::TaskFuture<'a, ::core::result::Result<::core::option::Option<u64>, $crate::store::StoreError>> { $crate::store::TaskStore::next_retry_deadline(&*self.$field, now_ms) }
        fn prune_terminal_before<'a>(&'a self, cutoff: u64, limit: ::core::num::NonZeroUsize) -> $crate::store::TaskFuture<'a, ::core::result::Result<usize, $crate::store::StoreError>> { $crate::store::TaskStore::prune_terminal_before(&*self.$field, cutoff, limit) }
        fn acquire_owner<'a>(&'a self) -> $crate::store::TaskFuture<'a, ::core::result::Result<$crate::model::OwnerEpoch, $crate::store::StoreError>> { $crate::store::TaskStore::acquire_owner(&*self.$field) }
        fn release_owner<'a>(&'a self, epoch: $crate::model::OwnerEpoch) -> $crate::store::TaskFuture<'a, ::core::result::Result<(), $crate::store::StoreError>> { $crate::store::TaskStore::release_owner(&*self.$field, epoch) }
    };
    ($($invalid:tt)*) => { compile_error!("unknown delegate_task_store! exception or invalid syntax"); };
}
