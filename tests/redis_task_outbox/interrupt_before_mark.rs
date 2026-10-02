//! Store adapter that identifies the accepted-publication / uncommitted-delete boundary.
use std::sync::Arc;
use qubit_task::model::{AcceptOutcome, OwnerEpoch, ProgressCommand, StartCommand, StoreCapabilities, StoredTask, StoredTaskRequest, TaskId, TaskPage, TaskQuery, TaskSummary, TransitionCommand};
use qubit_task::store::{EventOutboxEntry, SqliteTaskStore, StoreError, TaskFuture, TaskStore};

/// Passes every real SQLite operation through, except publication acknowledgement deletion.
pub struct InterruptBeforeMark {
    pub inner: Arc<SqliteTaskStore>,
    pub reached: tokio::sync::Notify,
}

impl TaskStore for InterruptBeforeMark {
    fn capabilities(&self) -> StoreCapabilities { self.inner.capabilities() }
    fn enable_event_outbox<'a>(&'a self) -> TaskFuture<'a, Result<(), StoreError>> { self.inner.enable_event_outbox() }
    fn list_event_outbox<'a>(&'a self, limit: usize) -> TaskFuture<'a, Result<Vec<EventOutboxEntry>, StoreError>> { self.inner.list_event_outbox(limit) }
    fn mark_event_published<'a>(&'a self, _: TaskId, _: u64) -> TaskFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            self.reached.notify_one();
            std::future::pending().await
        })
    }
    fn accept_encoded<'a>(&'a self, id: TaskId, request: StoredTaskRequest) -> TaskFuture<'a, Result<AcceptOutcome, StoreError>> { self.inner.accept_encoded(id, request) }
    fn get_encoded_task<'a>(&'a self, id: TaskId) -> TaskFuture<'a, Result<Option<StoredTask>, StoreError>> { self.inner.get_encoded_task(id) }
    fn start_encoded<'a>(&'a self, command: StartCommand) -> TaskFuture<'a, Result<TaskSummary, StoreError>> { self.inner.start_encoded(command) }
    fn transition_encoded<'a>(&'a self, command: TransitionCommand) -> TaskFuture<'a, Result<TaskSummary, StoreError>> { self.inner.transition_encoded(command) }
    fn update_progress<'a>(&'a self, command: ProgressCommand) -> TaskFuture<'a, Result<TaskSummary, StoreError>> { self.inner.update_progress(command) }
    fn list_encoded<'a>(&'a self, query: TaskQuery) -> TaskFuture<'a, Result<TaskPage, StoreError>> { self.inner.list_encoded(query) }
    fn acquire_owner<'a>(&'a self) -> TaskFuture<'a, Result<OwnerEpoch, StoreError>> { self.inner.acquire_owner() }
    fn release_owner<'a>(&'a self, owner: OwnerEpoch) -> TaskFuture<'a, Result<(), StoreError>> { self.inner.release_owner(owner) }
}
