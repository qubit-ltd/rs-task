// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Typed store implementations backed by the existing atomic store engines.

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

    fn get_encoded_task<'a>(&'a self, id: TaskId) -> TaskFuture<'a, Result<Option<StoredTask>, StoreError>> {
        LegacyTaskStore::get_encoded_task(self, id)
    }

    fn start_encoded<'a>(&'a self, command: StartCommand) -> TaskFuture<'a, Result<TaskSummary, StoreError>> {
        LegacyTaskStore::start_encoded(self, command)
    }

    fn transition_encoded<'a>(&'a self, command: TransitionCommand) -> TaskFuture<'a, Result<TaskSummary, StoreError>> {
        LegacyTaskStore::transition_encoded(self, command)
    }

    fn update_progress<'a>(&'a self, command: ProgressCommand) -> TaskFuture<'a, Result<TaskSummary, StoreError>> {
        LegacyTaskStore::update_progress(self, command)
    }

    fn list_encoded<'a>(&'a self, query: TaskQuery) -> TaskFuture<'a, Result<TaskPage, StoreError>> {
        LegacyTaskStore::list_encoded(self, query)
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

    fn get_encoded_task<'a>(&'a self, id: TaskId) -> TaskFuture<'a, Result<Option<StoredTask>, StoreError>> {
        LegacyTaskStore::get_encoded_task(self, id)
    }

    fn start_encoded<'a>(&'a self, command: StartCommand) -> TaskFuture<'a, Result<TaskSummary, StoreError>> {
        LegacyTaskStore::start_encoded(self, command)
    }

    fn transition_encoded<'a>(&'a self, command: TransitionCommand) -> TaskFuture<'a, Result<TaskSummary, StoreError>> {
        LegacyTaskStore::transition_encoded(self, command)
    }

    fn update_progress<'a>(&'a self, command: ProgressCommand) -> TaskFuture<'a, Result<TaskSummary, StoreError>> {
        LegacyTaskStore::update_progress(self, command)
    }

    fn list_encoded<'a>(&'a self, query: TaskQuery) -> TaskFuture<'a, Result<TaskPage, StoreError>> {
        LegacyTaskStore::list_encoded(self, query)
    }

    fn acquire_owner<'a>(&'a self) -> TaskFuture<'a, Result<OwnerEpoch, StoreError>> {
        LegacyTaskStore::acquire_owner(self)
    }

    fn release_owner<'a>(&'a self, epoch: OwnerEpoch) -> TaskFuture<'a, Result<(), StoreError>> {
        LegacyTaskStore::release_owner(self, epoch)
    }
}
