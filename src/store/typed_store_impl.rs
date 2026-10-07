// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Typed store implementations backed by the existing atomic store engines.

use std::num::NonZeroUsize;

use super::LegacyTaskStore;
use super::MemoryTaskStore;
use super::StoreError;
use super::TaskFuture;
use super::TaskStore;
use crate::model::OwnerEpoch;
use crate::model::StoreCapabilities;
use crate::model::next::AcceptOutcome;
use crate::model::next::ProgressCommand;
use crate::model::next::StartCommand;
use crate::model::next::StoredTask;
use crate::model::next::StoredTaskRequest;
use crate::model::next::TaskId;
use crate::model::next::TaskPage;
use crate::model::next::TaskQuery;
use crate::model::next::TaskSummary;
use crate::model::next::TransitionCommand;

impl TaskStore for MemoryTaskStore {
    fn capabilities(&self) -> StoreCapabilities {
        LegacyTaskStore::capabilities(self)
    }

    fn accept_encoded<'a>(
        &'a self,
        id: TaskId,
        request: StoredTaskRequest,
    ) -> TaskFuture<'a, Result<AcceptOutcome, StoreError>> {
        LegacyTaskStore::accept_encoded(self, id, request)
    }

    fn get_encoded_task<'a>(
        &'a self,
        id: TaskId,
    ) -> TaskFuture<'a, Result<Option<StoredTask>, StoreError>> {
        LegacyTaskStore::get_encoded_task(self, id)
    }

    fn start_encoded<'a>(
        &'a self,
        command: StartCommand,
    ) -> TaskFuture<'a, Result<TaskSummary, StoreError>> {
        LegacyTaskStore::start_encoded(self, command)
    }

    fn transition_encoded<'a>(
        &'a self,
        command: TransitionCommand,
    ) -> TaskFuture<'a, Result<TaskSummary, StoreError>> {
        LegacyTaskStore::transition_encoded(self, command)
    }

    fn update_progress<'a>(
        &'a self,
        command: ProgressCommand,
    ) -> TaskFuture<'a, Result<TaskSummary, StoreError>> {
        LegacyTaskStore::update_progress(self, command)
    }

    fn list_encoded<'a>(
        &'a self,
        query: TaskQuery,
    ) -> TaskFuture<'a, Result<TaskPage, StoreError>> {
        LegacyTaskStore::list_encoded(self, query)
    }

    fn list_ready_queued<'a>(
        &'a self,
        after: Option<crate::model::next::TaskCursor>,
        limit: NonZeroUsize,
        now_ms: u64,
    ) -> TaskFuture<'a, Result<TaskPage, StoreError>> {
        LegacyTaskStore::list_ready_queued(self, after, limit, now_ms)
    }

    fn next_retry_deadline<'a>(
        &'a self,
        now_ms: u64,
    ) -> TaskFuture<'a, Result<Option<u64>, StoreError>> {
        LegacyTaskStore::next_retry_deadline(self, now_ms)
    }

    fn prune_terminal_before<'a>(
        &'a self,
        finished_before_ms: u64,
        max_rows: NonZeroUsize,
    ) -> TaskFuture<'a, Result<usize, StoreError>> {
        LegacyTaskStore::prune_typed_terminal_before(self, finished_before_ms, max_rows)
    }

    fn acquire_owner<'a>(&'a self) -> TaskFuture<'a, Result<OwnerEpoch, StoreError>> {
        LegacyTaskStore::acquire_owner(self)
    }

    fn release_owner<'a>(&'a self, epoch: OwnerEpoch) -> TaskFuture<'a, Result<(), StoreError>> {
        LegacyTaskStore::release_owner(self, epoch)
    }
}

#[cfg(feature = "sqlite")]
impl TaskStore for super::SqliteTaskStore {
    fn enable_event_outbox<'a>(&'a self) -> TaskFuture<'a, Result<(), StoreError>> {
        self.enable_outbox()
    }
    fn list_event_outbox<'a>(
        &'a self,
        limit: usize,
    ) -> TaskFuture<'a, Result<Vec<super::EventOutboxEntry>, StoreError>> {
        self.list_outbox(limit)
    }
    fn mark_event_published<'a>(
        &'a self,
        task_id: TaskId,
        state_version: u64,
    ) -> TaskFuture<'a, Result<(), StoreError>> {
        self.mark_outbox_published(task_id, state_version)
    }

    fn capabilities(&self) -> StoreCapabilities {
        LegacyTaskStore::capabilities(self)
    }

    fn accept_encoded<'a>(
        &'a self,
        id: TaskId,
        request: StoredTaskRequest,
    ) -> TaskFuture<'a, Result<AcceptOutcome, StoreError>> {
        LegacyTaskStore::accept_encoded(self, id, request)
    }

    fn get_encoded_task<'a>(
        &'a self,
        id: TaskId,
    ) -> TaskFuture<'a, Result<Option<StoredTask>, StoreError>> {
        LegacyTaskStore::get_encoded_task(self, id)
    }

    fn start_encoded<'a>(
        &'a self,
        command: StartCommand,
    ) -> TaskFuture<'a, Result<TaskSummary, StoreError>> {
        LegacyTaskStore::start_encoded(self, command)
    }

    fn transition_encoded<'a>(
        &'a self,
        command: TransitionCommand,
    ) -> TaskFuture<'a, Result<TaskSummary, StoreError>> {
        LegacyTaskStore::transition_encoded(self, command)
    }

    fn update_progress<'a>(
        &'a self,
        command: ProgressCommand,
    ) -> TaskFuture<'a, Result<TaskSummary, StoreError>> {
        LegacyTaskStore::update_progress(self, command)
    }

    fn list_encoded<'a>(
        &'a self,
        query: TaskQuery,
    ) -> TaskFuture<'a, Result<TaskPage, StoreError>> {
        LegacyTaskStore::list_encoded(self, query)
    }

    fn list_ready_queued<'a>(
        &'a self,
        after: Option<crate::model::next::TaskCursor>,
        limit: NonZeroUsize,
        now_ms: u64,
    ) -> TaskFuture<'a, Result<TaskPage, StoreError>> {
        LegacyTaskStore::list_ready_queued(self, after, limit, now_ms)
    }

    fn next_retry_deadline<'a>(
        &'a self,
        now_ms: u64,
    ) -> TaskFuture<'a, Result<Option<u64>, StoreError>> {
        LegacyTaskStore::next_retry_deadline(self, now_ms)
    }

    fn prune_terminal_before<'a>(
        &'a self,
        finished_before_ms: u64,
        max_rows: NonZeroUsize,
    ) -> TaskFuture<'a, Result<usize, StoreError>> {
        LegacyTaskStore::prune_typed_terminal_before(self, finished_before_ms, max_rows)
    }

    fn acquire_owner<'a>(&'a self) -> TaskFuture<'a, Result<OwnerEpoch, StoreError>> {
        LegacyTaskStore::acquire_owner(self)
    }

    fn release_owner<'a>(&'a self, epoch: OwnerEpoch) -> TaskFuture<'a, Result<(), StoreError>> {
        LegacyTaskStore::release_owner(self, epoch)
    }
}
