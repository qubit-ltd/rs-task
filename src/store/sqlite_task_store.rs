// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::num::NonZeroUsize;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use internal::DatabaseIdentity;
use internal::SqliteOwnerState;
#[cfg(test)]
use internal::WorkerCounts;
#[cfg(test)]
use internal::WorkerGuard;
use internal::accept_encoded;
use internal::acquire_owner_lock;
use internal::get_encoded_task;
use internal::initialize_next_schema;
use internal::list_encoded;
use internal::list_ready_queued;
use internal::next_retry_deadline;
use internal::start_encoded;
use internal::transition_encoded;
use internal::update_progress;
use parking_lot::Mutex;
use rusqlite::Connection;
use rusqlite::params;
use tokio::sync;
use tokio::task;

use super::StoreError;
use super::TaskFuture;
use crate::model::OwnerEpoch;
use crate::model::StoreCapabilities;
use crate::model::typed::TaskPage as EncodedTaskPage;
use crate::model::typed::TaskQuery as EncodedTaskQuery;

mod internal;

/// SQLite-backed history with an exclusive OS lock for one active service
/// process.
///
/// # Examples
///
/// ```
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use std::fs;
/// use std::io::ErrorKind;
/// use std::time::SystemTime;
///
/// use qubit_task::store::TaskStore;
/// use qubit_task::store::SqliteTaskStore;
///
/// let timestamp = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?.as_nanos();
/// let directory = std::env::temp_dir().join(format!("qubit-task-doc-{}-{timestamp}", std::process::id()));
/// match fs::create_dir(&directory) {
///     Ok(()) => {},
///     Err(error) if error.kind() == ErrorKind::AlreadyExists => {
///         return Err(error.into());
///     }
///     Err(error) => return Err(error.into()),
/// }
/// let path = directory.join("tasks.sqlite");
/// let store = SqliteTaskStore::open(&path)?;
/// assert!(store.capabilities().restart_recovery);
/// drop(store);
/// fs::remove_file(&path)?;
/// let mut lock_path = path.as_os_str().to_owned();
/// lock_path.push(".owner.lock");
/// fs::remove_file(std::path::PathBuf::from(lock_path))?;
/// fs::remove_dir(directory)?;
/// # Ok(())
/// # }
/// ```
pub struct SqliteTaskStore {
    /// SQLite connection serialized behind a synchronous mutex.
    connection: Arc<Mutex<Connection>>,
    /// Canonical path used to reacquire the process lock after owner release.
    database_path: PathBuf,
    /// Process-lock file and currently held recovery epoch.
    owner_state: Arc<Mutex<SqliteOwnerState>>,
    /// Bounds connection operations to one blocking worker at a time.
    operation_slot: Arc<sync::Semaphore>,
    outbox_enabled: Arc<std::sync::atomic::AtomicBool>,
    /// Tracks blocking worker overlap in unit tests.
    #[cfg(test)]
    worker_counts: Arc<WorkerCounts>,
}

impl SqliteTaskStore {
    /// Serializes outbox operations with epoch validation and owner release.
    /// Returns `OwnerConflict` if ownership has not been acquired or was
    /// released.
    fn run_outbox<'a, T, F>(&'a self, operation: F) -> TaskFuture<'a, Result<T, StoreError>>
    where
        T: Send + 'static,
        F: FnOnce(&Connection) -> Result<T, StoreError> + Send + 'static,
    {
        let owner_state = Arc::clone(&self.owner_state);
        self.run(move |connection| {
            let owner = owner_state.lock();
            if owner.epoch.is_none() || owner.lock_file.is_none() {
                return Err(StoreError::OwnerConflict);
            }
            operation(connection)
        })
    }

    /// Enables subsequent transactional snapshots while holding the owner
    /// barrier.
    pub(super) fn enable_outbox(&self) -> TaskFuture<'_, Result<(), StoreError>> {
        let enabled = Arc::clone(&self.outbox_enabled);
        self.run_outbox(move |_| {
            enabled.store(true, std::sync::atomic::Ordering::Release);
            Ok(())
        })
    }

    /// Reads a bounded oldest-first page; rejects absent ownership and invalid
    /// limits.
    pub(super) fn list_outbox(&self, limit: usize) -> TaskFuture<'_, Result<Vec<super::EventOutboxEntry>, StoreError>> {
        self.run_outbox(move |connection| {
            if !(1..=256).contains(&limit) { return Err(StoreError::InvalidRequest("outbox page limit must be 1..=256")); }
            let mut statement = connection.prepare("SELECT task_id,state_version,event_id,event_json FROM task_event_outbox ORDER BY created_at_ms,task_id,state_version LIMIT ?1").map_err(failure)?;
            let rows = statement.query_map([limit as i64], |row| Ok((row.get::<_, String>(0)?,row.get::<_, u64>(1)?,row.get::<_, String>(2)?,row.get::<_, String>(3)?))).map_err(failure)?;
            rows.map(|row| {
                let (id, state_version, event_id, event_json) = row.map_err(failure)?;
                let task_id = crate::model::typed::TaskId::from_id(qubit_id::Id::new(id.parse::<u64>().map_err(failure)?));
                Ok(super::EventOutboxEntry { task_id, state_version, event_id, event_json })
            }).collect()
        })
    }

    /// Deletes a confirmed event idempotently while ownership remains fenced.
    pub(super) fn mark_outbox_published(
        &self,
        id: crate::model::typed::TaskId,
        version: u64,
    ) -> TaskFuture<'_, Result<(), StoreError>> {
        self.run_outbox(move |connection| {
            connection
                .execute(
                    "DELETE FROM task_event_outbox WHERE task_id=?1 AND state_version=?2",
                    rusqlite::params![id.to_padded_decimal(), version],
                )
                .map_err(failure)?;
            Ok(())
        })
    }



    /// Opens a database using the typed numeric-ID task schema.
    ///
    /// This cutover entry point creates a fresh schema or reopens the same
    /// typed schema. Legacy UUID databases are rejected with a migration
    /// diagnostic and left unchanged; converting their IDs requires an
    /// explicit mapping migration.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let (identity, _database_file) = DatabaseIdentity::open(path.as_ref())?;
        let lock = acquire_owner_lock(identity.path())?;
        identity.verify()?;
        let mut connection = Connection::open(identity.path()).map_err(failure)?;
        identity.verify()?;
        connection
            .busy_timeout(std::time::Duration::from_secs(5))
            .map_err(failure)?;
        initialize_next_schema(&mut connection)?;
        connection
            .execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;")
            .map_err(failure)?;
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
            database_path: identity.path().to_path_buf(),
            owner_state: Arc::new(Mutex::new(SqliteOwnerState {
                lock_file: Some(lock),
                epoch: None,
            })),
            operation_slot: Arc::new(sync::Semaphore::new(1)),
            outbox_enabled: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            #[cfg(test)]
            worker_counts: Arc::new(WorkerCounts::default()),
        })
    }

    /// Runs one connection operation on the blocking pool through its serial
    /// slot.
    ///
    /// # Type Parameters
    ///
    /// * `T` - Value produced by the operation.
    /// * `F` - Blocking connection operation.
    ///
    /// # Parameters
    ///
    /// * `operation` - Closure executed while holding the operation slot.
    ///
    /// # Returns
    ///
    /// A future resolving to the operation value.
    ///
    /// # Errors
    ///
    /// Returns an error when the slot, blocking worker, or operation fails.
    fn run<'a, T, F>(&'a self, operation: F) -> TaskFuture<'a, Result<T, StoreError>>
    where
        T: Send + 'static,
        F: FnOnce(&Connection) -> Result<T, StoreError> + Send + 'static,
    {
        let connection = self.connection.clone();
        let operation_slot = Arc::clone(&self.operation_slot);
        #[cfg(test)]
        let worker_counts = Arc::clone(&self.worker_counts);
        Box::pin(async move {
            let permit = operation_slot
                .acquire_owned()
                .await
                .map_err(|_| StoreError::Failure("SQLite operation queue is closed".into()))?;
            task::spawn_blocking(move || {
                let _permit = permit;
                #[cfg(test)]
                let _worker_guard = WorkerGuard::enter(worker_counts);
                operation(&connection.lock())
            })
            .await
            .map_err(|error| StoreError::Failure(format!("SQLite blocking operation stopped: {error}")))?
        })
    }

    /// Runs one serialized write only while this store still owns its process
    /// lock.
    ///
    /// # Type Parameters
    ///
    /// * `T` - Value produced by the operation.
    /// * `F` - Blocking connection operation.
    ///
    /// # Parameters
    ///
    /// * `operation` - Write closure executed while ownership is checked.
    ///
    /// # Returns
    ///
    /// A future resolving to the operation value.
    ///
    /// # Errors
    ///
    /// Returns an error when ownership is absent or the write fails.
    fn run_write<'a, T, F>(&'a self, operation: F) -> TaskFuture<'a, Result<T, StoreError>>
    where
        T: Send + 'static,
        F: FnOnce(&Connection) -> Result<T, StoreError> + Send + 'static,
    {
        let owner_state = Arc::clone(&self.owner_state);
        self.run(move |connection| {
            let owner_state = owner_state.lock();
            if owner_state.lock_file.is_none() {
                return Err(StoreError::Failure("SQLite task store has no active owner".into()));
            }
            operation(connection)
        })
    }
}

/// Builds the initial queued record before the SQLite acceptance transaction.
///
/// # Parameters
///
/// * `id` - Service-assigned identity for the new task.
/// * `request` - Validated reconstructable request to accept.
///
fn failure(error: impl std::fmt::Display) -> StoreError {
    StoreError::Failure(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use futures::poll;
    use rusqlite::Connection;
    use tokio as tokio_crate;
    use tokio::pin;
    use tokio::spawn;
    use tokio::sync::mpsc;
    use tokio::sync::oneshot;
    use tokio::task;
    use tokio::time;

    use super::SqliteTaskStore;
    use crate::store::StoreError;
    use crate::store::TaskStore;

    /// Creates a unique database path for one worker scheduling test.
    fn test_database_path() -> std::path::PathBuf {
        std::env::temp_dir().join(format!("qubit-task-sqlite-worker-{}-{}.sqlite", std::process::id(), std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).expect("clock is after epoch").as_nanos()))
    }

    /// Removes only disposable files created by this worker test.
    fn remove_database(path: &std::path::Path) {
        let _ = std::fs::remove_file(path);
        let mut lock_path = path.as_os_str().to_owned();
        lock_path.push(".owner.lock");
        let _ = std::fs::remove_file(std::path::PathBuf::from(lock_path));
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }

    #[tokio_crate::test]
    async fn test_pruning_rejects_values_outside_sqlite_integer_range() {
        let path = test_database_path();
        let store = SqliteTaskStore::open(&path).expect("SQLite store opens");
        let cutoff = store
            .prune_terminal_before(u64::MAX, std::num::NonZeroUsize::new(1).expect("positive limit"))
            .await;
        assert!(matches!(cutoff, Err(crate::store::StoreError::InvalidRequest(_))));
        let limit = store
            .prune_terminal_before(
                0,
                std::num::NonZeroUsize::new(i64::MAX as usize + 1).expect("positive limit"),
            )
            .await;
        assert!(matches!(limit, Err(crate::store::StoreError::InvalidRequest(_))));
        drop(store);
        remove_database(&path);
    }

    #[tokio_crate::test]
    async fn test_blocking_worker_peak_is_bounded_to_one() {
        let path = test_database_path();
        let store = Arc::new(SqliteTaskStore::open(&path).expect("SQLite store opens"));
        let (started_sender, started_receiver) = oneshot::channel();
        let (release_sender, release_receiver) = std::sync::mpsc::channel();
        let running_store = Arc::clone(&store);
        let first = spawn(async move {
            running_store
                .run(move |_| {
                    let _ = started_sender.send(());
                    release_receiver.recv().expect("first operation is released");
                    Ok(())
                })
                .await
        });
        time::timeout(std::time::Duration::from_secs(2), started_receiver)
            .await
            .expect("first operation enters the blocking worker")
            .expect("first operation signals its start");

        let (ready_sender, mut ready_receiver) = mpsc::unbounded_channel();
        let mut waiters = Vec::new();
        for _ in 0..8 {
            let waiting_store = Arc::clone(&store);
            let ready_sender = ready_sender.clone();
            waiters.push(spawn(async move {
                let operation = waiting_store.run(|_| Ok(()));
                let _ = ready_sender.send(());
                operation.await
            }));
        }
        for _ in 0..8 {
            ready_receiver
                .recv()
                .await
                .expect("all waiting operations reach their permit wait");
        }
        let peak = store.worker_counts.peak.load(std::sync::atomic::Ordering::Acquire);
        release_sender.send(()).expect("first operation is still waiting");
        first
            .await
            .expect("first operation joins")
            .expect("first operation succeeds");
        for waiter in waiters {
            waiter
                .await
                .expect("waiting operation joins")
                .expect("waiting operation succeeds");
        }
        assert_eq!(peak, 1, "only one worker enters before the connection lock");

        drop(store);
        remove_database(&path);
    }

    /// Cancelling an async caller must leave its real blocking transaction
    /// fenced until commit. Channels mark transaction entry and continuation;
    /// worker counters and a second connection observe the actual store path.
    #[tokio_crate::test]
    async fn test_cancelled_write_caller_keeps_owner_until_transaction_commits() {
        let path = test_database_path();
        let store = Arc::new(SqliteTaskStore::open(&path).expect("SQLite store opens"));
        let epoch = store.acquire_owner().await.expect("store acquires owner");
        store
            .run_write(|connection| {
                connection
                    .execute_batch("CREATE TABLE barrier_probe (value INTEGER NOT NULL)")
                    .map_err(super::failure)
            })
            .await
            .expect("create probe in this test's database");

        let (entered_sender, entered_receiver) = oneshot::channel();
        let (continue_sender, continue_receiver) = std::sync::mpsc::channel();
        let (committed_sender, committed_receiver) = oneshot::channel();
        let writing_store = Arc::clone(&store);
        let caller = spawn(async move {
            writing_store
                .run_write(move |connection| {
                    let transaction = connection.unchecked_transaction().map_err(super::failure)?;
                    transaction
                        .execute("INSERT INTO barrier_probe(value) VALUES(42)", [])
                        .map_err(super::failure)?;
                    entered_sender.send(()).expect("test observes transaction entry");
                    // A dropped test continuation also lets the blocking worker
                    // exit on assertion failure instead of hanging runtime teardown.
                    continue_receiver.recv().map_err(super::failure)?;
                    transaction.commit().map_err(super::failure)?;
                    committed_sender.send(()).expect("test observes commit");
                    Ok(())
                })
                .await
        });
        time::timeout(Duration::from_secs(5), entered_receiver)
            .await
            .expect("blocking write enters its transaction")
            .expect("transaction entry signal");
        assert_eq!(store.worker_counts.active.load(Ordering::Acquire), 1);
        assert_eq!(store.operation_slot.available_permits(), 0);
        let observer = Connection::open(&path).expect("independent SQLite reader opens");
        let uncommitted: usize = observer
            .query_row("SELECT COUNT(*) FROM barrier_probe", [], |row| row.get(0))
            .expect("reader observes committed rows only");
        assert_eq!(uncommitted, 0, "the entered write has not committed");

        caller.abort();
        assert!(caller.await.expect_err("caller is cancelled").is_cancelled());
        assert_eq!(store.worker_counts.active.load(Ordering::Acquire), 1);
        {
            let release = store.release_owner(epoch);
            pin!(release);
            assert!(
                poll!(release.as_mut()).is_pending(),
                "owner release must wait for the cancelled caller's worker"
            );
            assert_eq!(store.worker_counts.active.load(Ordering::Acquire), 1);
            assert!(
                matches!(SqliteTaskStore::open(&path), Err(StoreError::OwnerConflict)),
                "OS ownership remains fenced"
            );

            continue_sender.send(()).expect("allow transaction to commit");
            time::timeout(Duration::from_secs(5), committed_receiver)
                .await
                .expect("transaction commits after continuation")
                .expect("transaction commit signal");
            time::timeout(Duration::from_secs(5), release)
                .await
                .expect("owner release finishes after the worker")
                .expect("owner release succeeds");
        }
        assert_eq!(store.worker_counts.active.load(Ordering::Acquire), 0);
        assert_eq!(store.operation_slot.available_permits(), 1);
        let committed: usize = observer
            .query_row("SELECT COUNT(*) FROM barrier_probe", [], |row| row.get(0))
            .expect("reader observes the committed write");
        assert_eq!(committed, 1);
        let replacement = SqliteTaskStore::open(&path).expect("ownership can transfer after the completion barrier");
        assert!(
            matches!(store.run_write(|_| Ok(())).await, Err(StoreError::Failure(_))),
            "old owner stays fenced from later writes"
        );
        drop(replacement);
        drop(observer);
        drop(store);
        remove_database(&path);
    }

    #[tokio_crate::test]
    async fn test_run_waits_for_its_single_operation_slot() {
        let path = test_database_path();
        let store = Arc::new(SqliteTaskStore::open(&path).expect("SQLite store opens"));
        let permit = store
            .operation_slot
            .clone()
            .acquire_owned()
            .await
            .expect("operation slot is available");
        let (started_sender, mut started_receiver) = oneshot::channel();
        let running_store = Arc::clone(&store);
        let operation = spawn(async move {
            running_store
                .run(move |_| {
                    let _ = started_sender.send(());
                    Ok(())
                })
                .await
        });
        task::yield_now().await;
        assert!(matches!(
            started_receiver.try_recv(),
            Err(oneshot::error::TryRecvError::Empty | oneshot::error::TryRecvError::Closed)
        ));
        drop(permit);
        time::timeout(std::time::Duration::from_secs(2), started_receiver)
            .await
            .expect("operation starts after the slot is released")
            .expect("operation signals its start");
        operation
            .await
            .expect("operation task joins")
            .expect("SQLite run succeeds");

        drop(store);
        remove_database(&path);
    }

    #[tokio_crate::test]
    async fn test_cancelled_slot_waiter_does_not_block_later_operations() {
        let path = test_database_path();
        let store = Arc::new(SqliteTaskStore::open(&path).expect("SQLite store opens"));
        let permit = store
            .operation_slot
            .clone()
            .acquire_owned()
            .await
            .expect("operation slot is available");
        let (started_sender, mut started_receiver) = oneshot::channel();
        let waiting_store = Arc::clone(&store);
        let waiting = spawn(async move {
            waiting_store
                .run(move |_| {
                    let _ = started_sender.send(());
                    Ok(())
                })
                .await
        });
        task::yield_now().await;
        waiting.abort();
        drop(permit);
        let result = store.run(|_| Ok(())).await;
        assert!(result.is_ok());
        assert!(matches!(
            started_receiver.try_recv(),
            Err(oneshot::error::TryRecvError::Empty | oneshot::error::TryRecvError::Closed)
        ));

        drop(store);
        remove_database(&path);
    }
}

impl super::TaskStore for SqliteTaskStore {
    /// Reports that history is persistent and unfinished records can recover.
    ///
    /// # Returns
    ///
    /// Both persistent-history and restart-recovery capabilities.
    fn capabilities(&self) -> StoreCapabilities {
        StoreCapabilities {
            persistent_history: true,
            restart_recovery: true,
        }
    }


    /// Atomically accepts an encoded typed request on a store opened with
    /// [`SqliteTaskStore::open`].
    fn accept_encoded<'a>(
        &'a self,
        id: crate::model::typed::TaskId,
        request: crate::model::typed::StoredTaskRequest,
    ) -> TaskFuture<'a, Result<crate::model::typed::AcceptOutcome, StoreError>> {
                let outbox_enabled = Arc::clone(&self.outbox_enabled);
        self.run_write(move |connection| {
            accept_encoded(
                connection,
                id,
                request,
                outbox_enabled.load(std::sync::atomic::Ordering::Acquire),
            )
        })
    }


    /// Loads an encoded typed task without decoding its application payload.
    fn get_encoded_task<'a>(
        &'a self,
        id: crate::model::typed::TaskId,
    ) -> TaskFuture<'a, Result<Option<crate::model::typed::StoredTask>, StoreError>> {
                self.run(move |connection| get_encoded_task(connection, id))
    }


    /// Starts one queued typed task attempt with a revision compare-and-set.
    fn start_encoded<'a>(
        &'a self,
        command: crate::model::typed::StartCommand,
    ) -> TaskFuture<'a, Result<crate::model::typed::TaskSummary, StoreError>> {
                let outbox_enabled = Arc::clone(&self.outbox_enabled);
        self.run_write(move |connection| {
            start_encoded(
                connection,
                command,
                outbox_enabled.load(std::sync::atomic::Ordering::Acquire),
            )
        })
    }


    /// Applies a typed lifecycle transition atomically.
    fn transition_encoded<'a>(
        &'a self,
        command: crate::model::typed::TransitionCommand,
    ) -> TaskFuture<'a, Result<crate::model::typed::TaskSummary, StoreError>> {
                let outbox_enabled = Arc::clone(&self.outbox_enabled);
        self.run_write(move |connection| {
            transition_encoded(
                connection,
                command,
                outbox_enabled.load(std::sync::atomic::Ordering::Acquire),
            )
        })
    }


    /// Persists a progress snapshot for the matching running attempt.
    fn update_progress<'a>(
        &'a self,
        command: crate::model::typed::ProgressCommand,
    ) -> TaskFuture<'a, Result<crate::model::typed::TaskSummary, StoreError>> {
                self.run_write(move |connection| update_progress(connection, command))
    }


    fn list_encoded<'a>(&'a self, query: EncodedTaskQuery) -> TaskFuture<'a, Result<EncodedTaskPage, StoreError>> {
                self.run(move |connection| list_encoded(connection, query))
    }


    fn list_ready_queued<'a>(
        &'a self,
        after: Option<crate::model::typed::TaskCursor>,
        limit: NonZeroUsize,
        now_ms: u64,
    ) -> TaskFuture<'a, Result<EncodedTaskPage, StoreError>> {
                self.run(move |connection| list_ready_queued(connection, after, limit, now_ms))
    }


    fn next_retry_deadline<'a>(&'a self, now_ms: u64) -> TaskFuture<'a, Result<Option<u64>, StoreError>> {
                self.run(move |connection| next_retry_deadline(connection, now_ms))
    }


    fn prune_terminal_before<'a>(
        &'a self,
        finished_before_ms: u64,
        max_rows: NonZeroUsize,
    ) -> TaskFuture<'a, Result<usize, StoreError>> {
                let finished_before_ms = match i64::try_from(finished_before_ms) {
            Ok(value) => value,
            Err(_) => {
                return Box::pin(async {
                    Err(StoreError::InvalidRequest(
                        "task finish cutoff exceeds the SQLite integer range",
                    ))
                });
            }
        };
        let max_rows = match i64::try_from(max_rows.get()) {
            Ok(value) => value,
            Err(_) => {
                return Box::pin(async {
                    Err(StoreError::InvalidRequest(
                        "task history prune limit exceeds the SQLite integer range",
                    ))
                });
            }
        };
        self.run_write(move |connection| {
            let transaction = connection.unchecked_transaction().map_err(failure)?;
            let mut statement = transaction
                .prepare("SELECT id FROM tasks WHERE state_kind IN ('Succeeded','Failed','Panicked','Cancelled') AND CAST(json_extract(lifecycle_json, '$.finished_at_ms') AS INTEGER) < ?1 ORDER BY CAST(json_extract(lifecycle_json, '$.finished_at_ms') AS INTEGER), id LIMIT ?2")
                .map_err(failure)?;
            let rows = statement
                .query_map(params![finished_before_ms, max_rows], |row| row.get::<_, String>(0))
                .map_err(failure)?;
            let ids = rows.collect::<Result<Vec<_>, _>>().map_err(failure)?;
            drop(statement);
            for id in &ids {
                transaction
                    .execute("DELETE FROM tasks WHERE id=?1", [id])
                    .map_err(failure)?;
            }
            transaction.commit().map_err(failure)?;
            Ok(ids.len())
        })
    }


    /// Increments and records the exclusive service ownership epoch.
    ///
    /// # Returns
    ///
    /// A future resolving to the new owner epoch.
    ///
    /// # Errors
    ///
    /// Returns an error when the owner lock is absent or metadata access fails.
    fn acquire_owner<'a>(&'a self) -> TaskFuture<'a, Result<OwnerEpoch, StoreError>> {
        let owner_state = Arc::clone(&self.owner_state);
        let database_path = self.database_path.clone();
        self.run(move |connection| {
            let mut owner_state = owner_state.lock();
            if owner_state.epoch.is_some() {
                return Err(StoreError::OwnerConflict);
            }
            if owner_state.lock_file.is_none() {
                owner_state.lock_file = Some(acquire_owner_lock(&database_path)?);
            }
            connection.execute("INSERT INTO metadata(key,value) VALUES('owner_epoch',1) ON CONFLICT(key) DO UPDATE SET value=value+1", []).map_err(failure)?;
            let epoch = connection.query_row("SELECT value FROM metadata WHERE key='owner_epoch'", [], |row| row.get::<_, u64>(0)).map(OwnerEpoch).map_err(failure)?;
            owner_state.epoch = Some(epoch);
            Ok(epoch)
        })
    }


    /// Releases the process lock only when the supplied epoch still matches.
    ///
    /// # Parameters
    ///
    /// * `epoch` - Ownership generation previously issued by this store.
    ///
    /// # Returns
    ///
    /// A future resolving after the lock file is released.
    ///
    /// # Errors
    ///
    /// Returns an error for a stale epoch, absent owner, or lock failure.
    fn release_owner<'a>(&'a self, epoch: OwnerEpoch) -> TaskFuture<'a, Result<(), StoreError>> {
        let owner_state = Arc::clone(&self.owner_state);
        let outbox_enabled = Arc::clone(&self.outbox_enabled);
        self.run(move |_| {
            let mut owner_state = owner_state.lock();
            if owner_state.epoch != Some(epoch) {
                return Err(StoreError::OwnerConflict);
            }
            let file = owner_state.lock_file.take().ok_or(StoreError::OwnerConflict)?;
            owner_state.epoch = None;
            outbox_enabled.store(false, std::sync::atomic::Ordering::Release);
            file.unlock().map_err(failure)
        })
    }

    fn enable_event_outbox<'a>(&'a self) -> TaskFuture<'a, Result<(), StoreError>> { self.enable_outbox() }
    fn list_event_outbox<'a>(&'a self, limit: usize) -> TaskFuture<'a, Result<Vec<super::EventOutboxEntry>, StoreError>> { self.list_outbox(limit) }
    fn mark_event_published<'a>(&'a self, task_id: crate::model::typed::TaskId, state_version: u64) -> TaskFuture<'a, Result<(), StoreError>> { self.mark_outbox_published(task_id, state_version) }
}
