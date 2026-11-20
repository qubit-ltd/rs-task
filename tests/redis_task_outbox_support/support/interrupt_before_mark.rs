// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Store adapter that identifies the accepted-publication / uncommitted-delete
//! boundary.
use std::sync::Arc;

use qubit_task::model::TaskId;
use qubit_task::store::SqliteTaskStore;
use qubit_task::store::StoreError;
use qubit_task::store::TaskFuture;
use qubit_task::store::TaskStore;

/// Passes every real SQLite operation through, except publication
/// acknowledgement deletion.
pub struct InterruptBeforeMark {
    pub inner: Arc<SqliteTaskStore>,
    pub reached: tokio::sync::Notify,
}

impl TaskStore for InterruptBeforeMark {
    fn mark_event_published<'a>(&'a self, _: TaskId, _: u64) -> TaskFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            self.reached.notify_one();
            std::future::pending().await
        })
    }

    qubit_task::store::delegate_task_store!(inner; except [mark_event_published]);
}
