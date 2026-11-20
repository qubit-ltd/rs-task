// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
// =============================================================================
/// Forwards [`TaskStore`](crate::store::TaskStore) methods to an inner store.
///
/// Use `delegate_task_store!(inner; except []);` inside a `TaskStore` impl.
/// Exceptions are comma-separated method names; each valid method may appear
/// at most once. The wrapper must implement every excepted method.
///
/// ```compile_fail
/// struct Wrapper { inner: qubit_task::store::MemoryTaskStore }
/// impl qubit_task::store::TaskStore for Wrapper {
///     qubit_task::store::delegate_task_store!(inner; except [not_a_store_method]);
/// }
/// ```
///
/// ```compile_fail
/// struct Wrapper { inner: qubit_task::store::MemoryTaskStore }
/// impl qubit_task::store::TaskStore for Wrapper {
///     qubit_task::store::delegate_task_store!(inner; except [capabilities, capabilities]);
/// }
/// ```
#[macro_export]
macro_rules! delegate_task_store {
    ($field:ident; except [$($exception:ident),* $(,)?]) => {
        $crate::delegate_task_store!(@parse $field; [$($exception),*]; [$($exception),*]);
    };
    (@parse $field:ident; [$($all:ident),*]; [enable_event_outbox $(, $tail:ident)*]) => {
        $crate::delegate_task_store!(@unique enable_event_outbox; [$($tail),*]);
        $crate::delegate_task_store!(@parse $field; [$($all),*]; [$($tail),*]);
    };
    (@parse $field:ident; [$($all:ident),*]; [list_event_outbox $(, $tail:ident)*]) => {
        $crate::delegate_task_store!(@unique list_event_outbox; [$($tail),*]);
        $crate::delegate_task_store!(@parse $field; [$($all),*]; [$($tail),*]);
    };
    (@parse $field:ident; [$($all:ident),*]; [mark_event_published $(, $tail:ident)*]) => {
        $crate::delegate_task_store!(@unique mark_event_published; [$($tail),*]);
        $crate::delegate_task_store!(@parse $field; [$($all),*]; [$($tail),*]);
    };
    (@parse $field:ident; [$($all:ident),*]; [quarantine_event_outbox_head $(, $tail:ident)*]) => {
        $crate::delegate_task_store!(@unique quarantine_event_outbox_head; [$($tail),*]);
        $crate::delegate_task_store!(@parse $field; [$($all),*]; [$($tail),*]);
    };
    (@parse $field:ident; [$($all:ident),*]; [capabilities $(, $tail:ident)*]) => {
        $crate::delegate_task_store!(@unique capabilities; [$($tail),*]);
        $crate::delegate_task_store!(@parse $field; [$($all),*]; [$($tail),*]);
    };
    (@parse $field:ident; [$($all:ident),*]; [accept_encoded $(, $tail:ident)*]) => {
        $crate::delegate_task_store!(@unique accept_encoded; [$($tail),*]);
        $crate::delegate_task_store!(@parse $field; [$($all),*]; [$($tail),*]);
    };
    (@parse $field:ident; [$($all:ident),*]; [get_encoded_task $(, $tail:ident)*]) => {
        $crate::delegate_task_store!(@unique get_encoded_task; [$($tail),*]);
        $crate::delegate_task_store!(@parse $field; [$($all),*]; [$($tail),*]);
    };
    (@parse $field:ident; [$($all:ident),*]; [get_summary $(, $tail:ident)*]) => {
        $crate::delegate_task_store!(@unique get_summary; [$($tail),*]);
        $crate::delegate_task_store!(@parse $field; [$($all),*]; [$($tail),*]);
    };
    (@parse $field:ident; [$($all:ident),*]; [get_state_checkpoint $(, $tail:ident)*]) => {
        $crate::delegate_task_store!(@unique get_state_checkpoint; [$($tail),*]);
        $crate::delegate_task_store!(@parse $field; [$($all),*]; [$($tail),*]);
    };
    (@parse $field:ident; [$($all:ident),*]; [start_encoded $(, $tail:ident)*]) => {
        $crate::delegate_task_store!(@unique start_encoded; [$($tail),*]);
        $crate::delegate_task_store!(@parse $field; [$($all),*]; [$($tail),*]);
    };
    (@parse $field:ident; [$($all:ident),*]; [transition_encoded $(, $tail:ident)*]) => {
        $crate::delegate_task_store!(@unique transition_encoded; [$($tail),*]);
        $crate::delegate_task_store!(@parse $field; [$($all),*]; [$($tail),*]);
    };
    (@parse $field:ident; [$($all:ident),*]; [update_progress $(, $tail:ident)*]) => {
        $crate::delegate_task_store!(@unique update_progress; [$($tail),*]);
        $crate::delegate_task_store!(@parse $field; [$($all),*]; [$($tail),*]);
    };
    (@parse $field:ident; [$($all:ident),*]; [list_encoded $(, $tail:ident)*]) => {
        $crate::delegate_task_store!(@unique list_encoded; [$($tail),*]);
        $crate::delegate_task_store!(@parse $field; [$($all),*]; [$($tail),*]);
    };
    (@parse $field:ident; [$($all:ident),*]; [list_ready_queued $(, $tail:ident)*]) => {
        $crate::delegate_task_store!(@unique list_ready_queued; [$($tail),*]);
        $crate::delegate_task_store!(@parse $field; [$($all),*]; [$($tail),*]);
    };
    (@parse $field:ident; [$($all:ident),*]; [next_retry_deadline $(, $tail:ident)*]) => {
        $crate::delegate_task_store!(@unique next_retry_deadline; [$($tail),*]);
        $crate::delegate_task_store!(@parse $field; [$($all),*]; [$($tail),*]);
    };
    (@parse $field:ident; [$($all:ident),*]; [prune_terminal_before $(, $tail:ident)*]) => {
        $crate::delegate_task_store!(@unique prune_terminal_before; [$($tail),*]);
        $crate::delegate_task_store!(@parse $field; [$($all),*]; [$($tail),*]);
    };
    (@parse $field:ident; [$($all:ident),*]; [acquire_owner $(, $tail:ident)*]) => {
        $crate::delegate_task_store!(@unique acquire_owner; [$($tail),*]);
        $crate::delegate_task_store!(@parse $field; [$($all),*]; [$($tail),*]);
    };
    (@parse $field:ident; [$($all:ident),*]; [release_owner $(, $tail:ident)*]) => {
        $crate::delegate_task_store!(@unique release_owner; [$($tail),*]);
        $crate::delegate_task_store!(@parse $field; [$($all),*]; [$($tail),*]);
    };
    (@parse $field:ident; [$($all:ident),*]; []) => {
        $crate::delegate_task_store!(@filter $field; [$($all),*]; enable_event_outbox);
        $crate::delegate_task_store!(@filter $field; [$($all),*]; list_event_outbox);
        $crate::delegate_task_store!(@filter $field; [$($all),*]; mark_event_published);
        $crate::delegate_task_store!(@filter $field; [$($all),*]; quarantine_event_outbox_head);
        $crate::delegate_task_store!(@filter $field; [$($all),*]; capabilities);
        $crate::delegate_task_store!(@filter $field; [$($all),*]; accept_encoded);
        $crate::delegate_task_store!(@filter $field; [$($all),*]; get_encoded_task);
        $crate::delegate_task_store!(@filter $field; [$($all),*]; get_summary);
        $crate::delegate_task_store!(@filter $field; [$($all),*]; get_state_checkpoint);
        $crate::delegate_task_store!(@filter $field; [$($all),*]; start_encoded);
        $crate::delegate_task_store!(@filter $field; [$($all),*]; transition_encoded);
        $crate::delegate_task_store!(@filter $field; [$($all),*]; update_progress);
        $crate::delegate_task_store!(@filter $field; [$($all),*]; list_encoded);
        $crate::delegate_task_store!(@filter $field; [$($all),*]; list_ready_queued);
        $crate::delegate_task_store!(@filter $field; [$($all),*]; next_retry_deadline);
        $crate::delegate_task_store!(@filter $field; [$($all),*]; prune_terminal_before);
        $crate::delegate_task_store!(@filter $field; [$($all),*]; acquire_owner);
        $crate::delegate_task_store!(@filter $field; [$($all),*]; release_owner);
    };
    (@parse $field:ident; [$($all:ident),*]; [$unknown:ident $(, $tail:ident)*]) => {
        compile_error!("unknown delegate_task_store! exception");
    };
    (@unique enable_event_outbox; [enable_event_outbox $(, $tail:ident)*]) => { compile_error!("duplicate delegate_task_store! exception: enable_event_outbox"); };
    (@unique enable_event_outbox; [$head:ident $(, $tail:ident)*]) => { $crate::delegate_task_store!(@unique enable_event_outbox; [$($tail),*]); };
    (@unique enable_event_outbox; []) => {};
    (@unique list_event_outbox; [list_event_outbox $(, $tail:ident)*]) => { compile_error!("duplicate delegate_task_store! exception: list_event_outbox"); };
    (@unique list_event_outbox; [$head:ident $(, $tail:ident)*]) => { $crate::delegate_task_store!(@unique list_event_outbox; [$($tail),*]); };
    (@unique list_event_outbox; []) => {};
    (@unique mark_event_published; [mark_event_published $(, $tail:ident)*]) => { compile_error!("duplicate delegate_task_store! exception: mark_event_published"); };
    (@unique mark_event_published; [$head:ident $(, $tail:ident)*]) => { $crate::delegate_task_store!(@unique mark_event_published; [$($tail),*]); };
    (@unique mark_event_published; []) => {};
    (@unique quarantine_event_outbox_head; [quarantine_event_outbox_head $(, $tail:ident)*]) => { compile_error!("duplicate delegate_task_store! exception: quarantine_event_outbox_head"); };
    (@unique quarantine_event_outbox_head; [$head:ident $(, $tail:ident)*]) => { $crate::delegate_task_store!(@unique quarantine_event_outbox_head; [$($tail),*]); };
    (@unique quarantine_event_outbox_head; []) => {};
    (@unique capabilities; [capabilities $(, $tail:ident)*]) => { compile_error!("duplicate delegate_task_store! exception: capabilities"); };
    (@unique capabilities; [$head:ident $(, $tail:ident)*]) => { $crate::delegate_task_store!(@unique capabilities; [$($tail),*]); };
    (@unique capabilities; []) => {};
    (@unique accept_encoded; [accept_encoded $(, $tail:ident)*]) => { compile_error!("duplicate delegate_task_store! exception: accept_encoded"); };
    (@unique accept_encoded; [$head:ident $(, $tail:ident)*]) => { $crate::delegate_task_store!(@unique accept_encoded; [$($tail),*]); };
    (@unique accept_encoded; []) => {};
    (@unique get_encoded_task; [get_encoded_task $(, $tail:ident)*]) => { compile_error!("duplicate delegate_task_store! exception: get_encoded_task"); };
    (@unique get_encoded_task; [$head:ident $(, $tail:ident)*]) => { $crate::delegate_task_store!(@unique get_encoded_task; [$($tail),*]); };
    (@unique get_encoded_task; []) => {};
    (@unique get_summary; [get_summary $(, $tail:ident)*]) => { compile_error!("duplicate delegate_task_store! exception: get_summary"); };
    (@unique get_summary; [$head:ident $(, $tail:ident)*]) => { $crate::delegate_task_store!(@unique get_summary; [$($tail),*]); };
    (@unique get_summary; []) => {};
    (@unique get_state_checkpoint; [get_state_checkpoint $(, $tail:ident)*]) => { compile_error!("duplicate delegate_task_store! exception: get_state_checkpoint"); };
    (@unique get_state_checkpoint; [$head:ident $(, $tail:ident)*]) => { $crate::delegate_task_store!(@unique get_state_checkpoint; [$($tail),*]); };
    (@unique get_state_checkpoint; []) => {};
    (@unique start_encoded; [start_encoded $(, $tail:ident)*]) => { compile_error!("duplicate delegate_task_store! exception: start_encoded"); };
    (@unique start_encoded; [$head:ident $(, $tail:ident)*]) => { $crate::delegate_task_store!(@unique start_encoded; [$($tail),*]); };
    (@unique start_encoded; []) => {};
    (@unique transition_encoded; [transition_encoded $(, $tail:ident)*]) => { compile_error!("duplicate delegate_task_store! exception: transition_encoded"); };
    (@unique transition_encoded; [$head:ident $(, $tail:ident)*]) => { $crate::delegate_task_store!(@unique transition_encoded; [$($tail),*]); };
    (@unique transition_encoded; []) => {};
    (@unique update_progress; [update_progress $(, $tail:ident)*]) => { compile_error!("duplicate delegate_task_store! exception: update_progress"); };
    (@unique update_progress; [$head:ident $(, $tail:ident)*]) => { $crate::delegate_task_store!(@unique update_progress; [$($tail),*]); };
    (@unique update_progress; []) => {};
    (@unique list_encoded; [list_encoded $(, $tail:ident)*]) => { compile_error!("duplicate delegate_task_store! exception: list_encoded"); };
    (@unique list_encoded; [$head:ident $(, $tail:ident)*]) => { $crate::delegate_task_store!(@unique list_encoded; [$($tail),*]); };
    (@unique list_encoded; []) => {};
    (@unique list_ready_queued; [list_ready_queued $(, $tail:ident)*]) => { compile_error!("duplicate delegate_task_store! exception: list_ready_queued"); };
    (@unique list_ready_queued; [$head:ident $(, $tail:ident)*]) => { $crate::delegate_task_store!(@unique list_ready_queued; [$($tail),*]); };
    (@unique list_ready_queued; []) => {};
    (@unique next_retry_deadline; [next_retry_deadline $(, $tail:ident)*]) => { compile_error!("duplicate delegate_task_store! exception: next_retry_deadline"); };
    (@unique next_retry_deadline; [$head:ident $(, $tail:ident)*]) => { $crate::delegate_task_store!(@unique next_retry_deadline; [$($tail),*]); };
    (@unique next_retry_deadline; []) => {};
    (@unique prune_terminal_before; [prune_terminal_before $(, $tail:ident)*]) => { compile_error!("duplicate delegate_task_store! exception: prune_terminal_before"); };
    (@unique prune_terminal_before; [$head:ident $(, $tail:ident)*]) => { $crate::delegate_task_store!(@unique prune_terminal_before; [$($tail),*]); };
    (@unique prune_terminal_before; []) => {};
    (@unique acquire_owner; [acquire_owner $(, $tail:ident)*]) => { compile_error!("duplicate delegate_task_store! exception: acquire_owner"); };
    (@unique acquire_owner; [$head:ident $(, $tail:ident)*]) => { $crate::delegate_task_store!(@unique acquire_owner; [$($tail),*]); };
    (@unique acquire_owner; []) => {};
    (@unique release_owner; [release_owner $(, $tail:ident)*]) => { compile_error!("duplicate delegate_task_store! exception: release_owner"); };
    (@unique release_owner; [$head:ident $(, $tail:ident)*]) => { $crate::delegate_task_store!(@unique release_owner; [$($tail),*]); };
    (@unique release_owner; []) => {};
    (@filter $field:ident; [enable_event_outbox $(, $tail:ident)*]; enable_event_outbox) => {};
    (@filter $field:ident; [$head:ident $(, $tail:ident)*]; enable_event_outbox) => { $crate::delegate_task_store!(@filter $field; [$($tail),*]; enable_event_outbox); };
    (@filter $field:ident; []; enable_event_outbox) => {
        fn enable_event_outbox<'a>(&'a self) -> $crate::store::TaskFuture<'a, ::core::result::Result<(), $crate::store::StoreError>> {
            $crate::store::TaskStore::enable_event_outbox(&*self.$field)
        }
    };
    (@filter $field:ident; [list_event_outbox $(, $tail:ident)*]; list_event_outbox) => {};
    (@filter $field:ident; [$head:ident $(, $tail:ident)*]; list_event_outbox) => { $crate::delegate_task_store!(@filter $field; [$($tail),*]; list_event_outbox); };
    (@filter $field:ident; []; list_event_outbox) => {
        fn list_event_outbox<'a>(&'a self, limit: usize) -> $crate::store::TaskFuture<'a, ::core::result::Result<::std::vec::Vec<$crate::store::EventOutboxEntry>, $crate::store::StoreError>> {
            $crate::store::TaskStore::list_event_outbox(&*self.$field, limit)
        }
    };
    (@filter $field:ident; [mark_event_published $(, $tail:ident)*]; mark_event_published) => {};
    (@filter $field:ident; [$head:ident $(, $tail:ident)*]; mark_event_published) => { $crate::delegate_task_store!(@filter $field; [$($tail),*]; mark_event_published); };
    (@filter $field:ident; []; mark_event_published) => {
        fn mark_event_published<'a>(&'a self, id: $crate::model::typed::TaskId, version: u64) -> $crate::store::TaskFuture<'a, ::core::result::Result<(), $crate::store::StoreError>> {
            $crate::store::TaskStore::mark_event_published(&*self.$field, id, version)
        }
    };
    (@filter $field:ident; [quarantine_event_outbox_head $(, $tail:ident)*]; quarantine_event_outbox_head) => {};
    (@filter $field:ident; [$head:ident $(, $tail:ident)*]; quarantine_event_outbox_head) => { $crate::delegate_task_store!(@filter $field; [$($tail),*]; quarantine_event_outbox_head); };
    (@filter $field:ident; []; quarantine_event_outbox_head) => {
        fn quarantine_event_outbox_head<'a>(&'a self, id: $crate::model::typed::TaskId, version: u64, event: &'a str, reason: &'a str, last_error: &'a str) -> $crate::store::TaskFuture<'a, ::core::result::Result<(), $crate::store::StoreError>> {
            $crate::store::TaskStore::quarantine_event_outbox_head(&*self.$field, id, version, event, reason, last_error)
        }
    };
    (@filter $field:ident; [capabilities $(, $tail:ident)*]; capabilities) => {};
    (@filter $field:ident; [$head:ident $(, $tail:ident)*]; capabilities) => { $crate::delegate_task_store!(@filter $field; [$($tail),*]; capabilities); };
    (@filter $field:ident; []; capabilities) => {
        fn capabilities(&self) -> $crate::model::StoreCapabilities { $crate::store::TaskStore::capabilities(&*self.$field) }
    };
    (@filter $field:ident; [accept_encoded $(, $tail:ident)*]; accept_encoded) => {};
    (@filter $field:ident; [$head:ident $(, $tail:ident)*]; accept_encoded) => { $crate::delegate_task_store!(@filter $field; [$($tail),*]; accept_encoded); };
    (@filter $field:ident; []; accept_encoded) => {
        fn accept_encoded<'a>(&'a self, id: $crate::model::typed::TaskId, request: $crate::model::typed::StoredTaskRequest) -> $crate::store::TaskFuture<'a, ::core::result::Result<$crate::model::typed::AcceptOutcome, $crate::store::StoreError>> {
            $crate::store::TaskStore::accept_encoded(&*self.$field, id, request)
        }
    };
    (@filter $field:ident; [get_encoded_task $(, $tail:ident)*]; get_encoded_task) => {};
    (@filter $field:ident; [$head:ident $(, $tail:ident)*]; get_encoded_task) => { $crate::delegate_task_store!(@filter $field; [$($tail),*]; get_encoded_task); };
    (@filter $field:ident; []; get_encoded_task) => {
        fn get_encoded_task<'a>(&'a self, id: $crate::model::typed::TaskId) -> $crate::store::TaskFuture<'a, ::core::result::Result<::core::option::Option<$crate::model::typed::StoredTask>, $crate::store::StoreError>> {
            $crate::store::TaskStore::get_encoded_task(&*self.$field, id)
        }
    };
    (@filter $field:ident; [get_summary $(, $tail:ident)*]; get_summary) => {};
    (@filter $field:ident; [$head:ident $(, $tail:ident)*]; get_summary) => { $crate::delegate_task_store!(@filter $field; [$($tail),*]; get_summary); };
    (@filter $field:ident; []; get_summary) => {
        fn get_summary<'a>(&'a self, id: $crate::model::typed::TaskId) -> $crate::store::TaskFuture<'a, ::core::result::Result<::core::option::Option<$crate::model::typed::TaskSummary>, $crate::store::StoreError>> {
            $crate::store::TaskStore::get_summary(&*self.$field, id)
        }
    };
    (@filter $field:ident; [get_state_checkpoint $(, $tail:ident)*]; get_state_checkpoint) => {};
    (@filter $field:ident; [$head:ident $(, $tail:ident)*]; get_state_checkpoint) => { $crate::delegate_task_store!(@filter $field; [$($tail),*]; get_state_checkpoint); };
    (@filter $field:ident; []; get_state_checkpoint) => {
        fn get_state_checkpoint<'a>(&'a self, id: $crate::model::typed::TaskId) -> $crate::store::TaskFuture<'a, ::core::result::Result<::core::option::Option<$crate::model::typed::TaskStateCheckpoint>, $crate::store::StoreError>> {
            $crate::store::TaskStore::get_state_checkpoint(&*self.$field, id)
        }
    };
    (@filter $field:ident; [start_encoded $(, $tail:ident)*]; start_encoded) => {};
    (@filter $field:ident; [$head:ident $(, $tail:ident)*]; start_encoded) => { $crate::delegate_task_store!(@filter $field; [$($tail),*]; start_encoded); };
    (@filter $field:ident; []; start_encoded) => {
        fn start_encoded<'a>(&'a self, command: $crate::model::typed::StartCommand) -> $crate::store::TaskFuture<'a, ::core::result::Result<$crate::model::typed::TaskSummary, $crate::store::StoreError>> {
            $crate::store::TaskStore::start_encoded(&*self.$field, command)
        }
    };
    (@filter $field:ident; [transition_encoded $(, $tail:ident)*]; transition_encoded) => {};
    (@filter $field:ident; [$head:ident $(, $tail:ident)*]; transition_encoded) => { $crate::delegate_task_store!(@filter $field; [$($tail),*]; transition_encoded); };
    (@filter $field:ident; []; transition_encoded) => {
        fn transition_encoded<'a>(&'a self, command: $crate::model::typed::TransitionCommand) -> $crate::store::TaskFuture<'a, ::core::result::Result<$crate::model::typed::TaskSummary, $crate::store::StoreError>> {
            $crate::store::TaskStore::transition_encoded(&*self.$field, command)
        }
    };
    (@filter $field:ident; [update_progress $(, $tail:ident)*]; update_progress) => {};
    (@filter $field:ident; [$head:ident $(, $tail:ident)*]; update_progress) => { $crate::delegate_task_store!(@filter $field; [$($tail),*]; update_progress); };
    (@filter $field:ident; []; update_progress) => {
        fn update_progress<'a>(&'a self, command: $crate::model::typed::ProgressCommand) -> $crate::store::TaskFuture<'a, ::core::result::Result<$crate::model::typed::TaskSummary, $crate::store::StoreError>> {
            $crate::store::TaskStore::update_progress(&*self.$field, command)
        }
    };
    (@filter $field:ident; [list_encoded $(, $tail:ident)*]; list_encoded) => {};
    (@filter $field:ident; [$head:ident $(, $tail:ident)*]; list_encoded) => { $crate::delegate_task_store!(@filter $field; [$($tail),*]; list_encoded); };
    (@filter $field:ident; []; list_encoded) => {
        fn list_encoded<'a>(&'a self, query: $crate::model::typed::TaskQuery) -> $crate::store::TaskFuture<'a, ::core::result::Result<$crate::model::typed::TaskPage, $crate::store::StoreError>> {
            $crate::store::TaskStore::list_encoded(&*self.$field, query)
        }
    };
    (@filter $field:ident; [list_ready_queued $(, $tail:ident)*]; list_ready_queued) => {};
    (@filter $field:ident; [$head:ident $(, $tail:ident)*]; list_ready_queued) => { $crate::delegate_task_store!(@filter $field; [$($tail),*]; list_ready_queued); };
    (@filter $field:ident; []; list_ready_queued) => {
        fn list_ready_queued<'a>(&'a self, after: ::core::option::Option<$crate::model::typed::TaskCursor>, limit: ::core::num::NonZeroUsize, now_ms: u64) -> $crate::store::TaskFuture<'a, ::core::result::Result<$crate::model::typed::TaskPage, $crate::store::StoreError>> {
            $crate::store::TaskStore::list_ready_queued(&*self.$field, after, limit, now_ms)
        }
    };
    (@filter $field:ident; [next_retry_deadline $(, $tail:ident)*]; next_retry_deadline) => {};
    (@filter $field:ident; [$head:ident $(, $tail:ident)*]; next_retry_deadline) => { $crate::delegate_task_store!(@filter $field; [$($tail),*]; next_retry_deadline); };
    (@filter $field:ident; []; next_retry_deadline) => {
        fn next_retry_deadline<'a>(&'a self, now_ms: u64) -> $crate::store::TaskFuture<'a, ::core::result::Result<::core::option::Option<u64>, $crate::store::StoreError>> {
            $crate::store::TaskStore::next_retry_deadline(&*self.$field, now_ms)
        }
    };
    (@filter $field:ident; [prune_terminal_before $(, $tail:ident)*]; prune_terminal_before) => {};
    (@filter $field:ident; [$head:ident $(, $tail:ident)*]; prune_terminal_before) => { $crate::delegate_task_store!(@filter $field; [$($tail),*]; prune_terminal_before); };
    (@filter $field:ident; []; prune_terminal_before) => {
        fn prune_terminal_before<'a>(&'a self, cutoff: u64, limit: ::core::num::NonZeroUsize) -> $crate::store::TaskFuture<'a, ::core::result::Result<usize, $crate::store::StoreError>> {
            $crate::store::TaskStore::prune_terminal_before(&*self.$field, cutoff, limit)
        }
    };
    (@filter $field:ident; [acquire_owner $(, $tail:ident)*]; acquire_owner) => {};
    (@filter $field:ident; [$head:ident $(, $tail:ident)*]; acquire_owner) => { $crate::delegate_task_store!(@filter $field; [$($tail),*]; acquire_owner); };
    (@filter $field:ident; []; acquire_owner) => {
        fn acquire_owner<'a>(&'a self) -> $crate::store::TaskFuture<'a, ::core::result::Result<$crate::model::OwnerEpoch, $crate::store::StoreError>> {
            $crate::store::TaskStore::acquire_owner(&*self.$field)
        }
    };
    (@filter $field:ident; [release_owner $(, $tail:ident)*]; release_owner) => {};
    (@filter $field:ident; [$head:ident $(, $tail:ident)*]; release_owner) => { $crate::delegate_task_store!(@filter $field; [$($tail),*]; release_owner); };
    (@filter $field:ident; []; release_owner) => {
        fn release_owner<'a>(&'a self, epoch: $crate::model::OwnerEpoch) -> $crate::store::TaskFuture<'a, ::core::result::Result<(), $crate::store::StoreError>> {
            $crate::store::TaskStore::release_owner(&*self.$field, epoch)
        }
    };
    ($($invalid:tt)*) => { compile_error!("invalid delegate_task_store! syntax"); };
}
