// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//    SPDX-License-Identifier: Apache-2.0
// =============================================================================
#![cfg(feature = "conformance")]

#[cfg(feature = "sqlite")]
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use crate::conformance::StoreFixture;
use crate::conformance::verify_core_contract;
use crate::conformance::verify_recovery_contract;
use crate::model::AcceptOutcome;
use crate::model::OwnerEpoch;
use crate::model::RecoveryPage;
use crate::model::StoreCapabilities;
use crate::model::TaskCursor;
use crate::model::TaskId;
use crate::model::TaskPage;
use crate::model::TaskQuery;
use crate::model::TaskRecord;
use crate::model::TaskRequest;
use crate::model::TaskStateCounts;
use crate::model::TaskSummary;
use crate::model::TransitionCommand;
use crate::store::LegacyTaskStore as TaskStore;
use crate::store::MemoryTaskStore;
#[cfg(feature = "sqlite")]
use crate::store::SqliteTaskStore;
use crate::store::StoreError;
use crate::store::TaskFuture;

struct MemoryFixture;
impl StoreFixture for MemoryFixture {
    fn open<'a>(&'a self) -> TaskFuture<'a, Result<Arc<dyn TaskStore>, StoreError>> {
        Box::pin(async { Ok(Arc::new(MemoryTaskStore::new(32)) as Arc<dyn TaskStore>) })
    }
}

#[cfg(feature = "sqlite")]
struct SqliteFixture {
    directory: PathBuf,
}
#[cfg(feature = "sqlite")]
impl SqliteFixture {
    /// Creates an isolated disposable database namespace.
    fn new() -> Self {
        let directory = std::env::temp_dir().join(format!("rs-task-conformance-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory).expect("create disposable fixture directory");
        Self { directory }
    }
}
#[cfg(feature = "sqlite")]
impl StoreFixture for SqliteFixture {
    fn open<'a>(&'a self) -> TaskFuture<'a, Result<Arc<dyn TaskStore>, StoreError>> {
        Box::pin(async {
            Ok(Arc::new(SqliteTaskStore::open(self.directory.join("tasks.sqlite"))?) as Arc<dyn TaskStore>)
        })
    }
}
#[cfg(feature = "sqlite")]
impl Drop for SqliteFixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.directory).expect("cleanup disposable fixture namespace");
    }
}

#[derive(Clone)]
enum Mutation {
    Idempotency,
    StaleVersion,
    WrongNext,
    DuplicatePage,
    #[cfg(feature = "sqlite")]
    WrongNextAndCleanup(Arc<AtomicUsize>, usize),
    UnexpectedVolatileOwner(Arc<AtomicUsize>),
    UnexpectedSharedOwner(Arc<AtomicUsize>),
    #[cfg(feature = "sqlite")]
    ShortRecoveryPage,
    #[cfg(feature = "sqlite")]
    OversizedRecoveryPage,
    #[cfg(feature = "sqlite")]
    EmptyRecoveryPage,
    #[cfg(feature = "sqlite")]
    PrematureRecoveryEnd,
}
struct BrokenFixture<F> {
    fixture: F,
    mutation: Mutation,
}
impl<F: StoreFixture> StoreFixture for BrokenFixture<F> {
    fn open<'a>(&'a self) -> TaskFuture<'a, Result<Arc<dyn TaskStore>, StoreError>> {
        Box::pin(async {
            Ok(Arc::new(BrokenStore {
                inner: self.fixture.open().await?,
                mutation: self.mutation.clone(),
            }) as Arc<dyn TaskStore>)
        })
    }
}
struct BrokenStore {
    inner: Arc<dyn TaskStore>,
    mutation: Mutation,
}
impl TaskStore for BrokenStore {
    fn capabilities(&self) -> StoreCapabilities {
        if matches!(self.mutation, Mutation::UnexpectedSharedOwner(_)) {
            StoreCapabilities {
                persistent_history: true,
                restart_recovery: true,
            }
        } else {
            self.inner.capabilities()
        }
    }
    fn accept<'a>(&'a self, id: TaskId, mut request: TaskRequest) -> TaskFuture<'a, Result<AcceptOutcome, StoreError>> {
        if matches!(self.mutation, Mutation::Idempotency) {
            request.idempotency_key = None;
        }
        self.inner.accept(id, request)
    }
    fn transition<'a>(&'a self, mut command: TransitionCommand) -> TaskFuture<'a, Result<TaskSummary, StoreError>> {
        Box::pin(async move {
            if matches!(self.mutation, Mutation::StaleVersion)
                && let Some(current) = self.inner.get_summary(command.id).await?
            {
                command.expected_version = current.state_version;
                command.expected_attempt = current.attempt;
            }
            self.inner.transition(command).await
        })
    }
    fn list<'a>(&'a self, query: TaskQuery) -> TaskFuture<'a, Result<TaskPage, StoreError>> {
        Box::pin(async move {
            let mut page = self.inner.list(query).await?;
            mutate_page(&self.mutation, &mut page.records, &mut page.next);
            Ok(page)
        })
    }
    fn scan_unfinished<'a>(&'a self, after: Option<TaskCursor>) -> TaskFuture<'a, Result<RecoveryPage, StoreError>> {
        Box::pin(async move {
            let mut page = self.inner.scan_unfinished(after).await?;
            mutate_page(&self.mutation, &mut page.tasks, &mut page.next);
            #[cfg(feature = "sqlite")]
            if matches!(self.mutation, Mutation::ShortRecoveryPage) && page.tasks.len() > 128 {
                page.tasks.truncate(128);
                page.next = page.tasks.last().map(TaskCursor::from);
            }
            #[cfg(feature = "sqlite")]
            match self.mutation {
                Mutation::OversizedRecoveryPage if !page.tasks.is_empty() => page.tasks.push(page.tasks[0].clone()),
                Mutation::EmptyRecoveryPage => {
                    page.tasks.clear();
                    page.next = None;
                }
                Mutation::PrematureRecoveryEnd => page.next = None,
                _ => {}
            }
            Ok(page)
        })
    }
    fn get_by_idempotency_key<'a>(&'a self, key: &'a str) -> TaskFuture<'a, Result<Option<TaskRecord>, StoreError>> {
        self.inner.get_by_idempotency_key(key)
    }
    fn get_summary_by_idempotency_key<'a>(
        &'a self,
        key: &'a str,
    ) -> TaskFuture<'a, Result<Option<TaskSummary>, StoreError>> {
        self.inner.get_summary_by_idempotency_key(key)
    }
    fn get_summary<'a>(&'a self, id: TaskId) -> TaskFuture<'a, Result<Option<TaskSummary>, StoreError>> {
        self.inner.get_summary(id)
    }
    fn get<'a>(&'a self, id: TaskId) -> TaskFuture<'a, Result<Option<TaskRecord>, StoreError>> {
        self.inner.get(id)
    }
    fn count_states<'a>(&'a self) -> TaskFuture<'a, Result<TaskStateCounts, StoreError>> {
        self.inner.count_states()
    }
    fn acquire_owner<'a>(&'a self) -> TaskFuture<'a, Result<OwnerEpoch, StoreError>> {
        if matches!(
            self.mutation,
            Mutation::UnexpectedVolatileOwner(_) | Mutation::UnexpectedSharedOwner(_)
        ) {
            Box::pin(async { Ok(OwnerEpoch(1)) })
        } else {
            self.inner.acquire_owner()
        }
    }
    fn has_unfinished_over_limit<'a>(&'a self, limit: usize) -> TaskFuture<'a, Result<bool, StoreError>> {
        self.inner.has_unfinished_over_limit(limit)
    }
    fn release_owner<'a>(&'a self, epoch: OwnerEpoch) -> TaskFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            match &self.mutation {
                #[cfg(feature = "sqlite")]
                Mutation::WrongNextAndCleanup(calls, fail_on_call) => {
                    let call = calls.fetch_add(1, Ordering::SeqCst) + 1;
                    self.inner.release_owner(epoch).await?;
                    if call == *fail_on_call {
                        return Err(StoreError::Failure("injected cleanup fault".into()));
                    }
                    Ok(())
                }
                Mutation::UnexpectedVolatileOwner(calls) | Mutation::UnexpectedSharedOwner(calls) => {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Err(StoreError::Failure("unexpected owner cleanup fault".into()))
                }
                _ => self.inner.release_owner(epoch).await,
            }
        })
    }
}
/// Injects malformed cursors or duplicate rows through the public page API.
fn mutate_page(mutation: &Mutation, rows: &mut [TaskSummary], next: &mut Option<TaskCursor>) {
    match mutation {
        Mutation::WrongNext => {
            *next = rows.first().map(TaskCursor::from);
        }
        #[cfg(feature = "sqlite")]
        Mutation::WrongNextAndCleanup(_, _) => {
            *next = rows.first().map(TaskCursor::from);
        }
        Mutation::DuplicatePage if rows.len() > 1 => {
            rows[1] = rows[0].clone();
        }
        _ => {}
    }
}

#[tokio::test]
async fn test_memory_core_contract() {
    let report = verify_core_contract(&MemoryFixture)
        .await
        .expect("memory satisfies core contracts");
    assert!(report.checks.contains(&"history_pagination"));
}
#[tokio::test]
async fn test_memory_recovery_is_an_explicit_violation() {
    let error = verify_recovery_contract(&MemoryFixture)
        .await
        .expect_err("unsupported recovery must not pass");
    assert_eq!(error.check, "recovery_capability");
}
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_sqlite_core_contract() {
    let report = verify_core_contract(&SqliteFixture::new())
        .await
        .expect("sqlite satisfies core contracts");
    assert!(report.checks.contains(&"history_pagination"));
}
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_sqlite_recovery_contract() {
    let report = verify_recovery_contract(&SqliteFixture::new())
        .await
        .expect("sqlite satisfies recovery contracts");
    assert!(report.checks.contains(&"recovery_pagination"));
}
#[tokio::test]
async fn test_harness_rejects_nonatomic_idempotency() {
    let error = verify_core_contract(&BrokenFixture {
        fixture: MemoryFixture,
        mutation: Mutation::Idempotency,
    })
    .await
    .expect_err("both acceptances must not be Accepted");
    assert_eq!(error.check, "atomic_idempotency");
}
#[tokio::test]
async fn test_harness_rejects_stale_version_acceptance() {
    let error = verify_core_contract(&BrokenFixture {
        fixture: MemoryFixture,
        mutation: Mutation::StaleVersion,
    })
    .await
    .expect_err("stale CAS must not succeed");
    assert_eq!(error.check, "atomic_transition");
}
#[tokio::test]
async fn test_harness_rejects_wrong_history_next() {
    let error = verify_core_contract(&BrokenFixture {
        fixture: MemoryFixture,
        mutation: Mutation::WrongNext,
    })
    .await
    .expect_err("wrong next must not pass");
    assert_eq!(error.check, "history_pagination");
}
#[tokio::test]
async fn test_harness_rejects_duplicate_history_rows() {
    let error = verify_core_contract(&BrokenFixture {
        fixture: MemoryFixture,
        mutation: Mutation::DuplicatePage,
    })
    .await
    .expect_err("duplicate rows must not pass");
    assert_eq!(error.check, "history_pagination");
}
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_harness_rejects_wrong_recovery_next_and_releases_owner() {
    let fixture = BrokenFixture {
        fixture: SqliteFixture::new(),
        mutation: Mutation::WrongNext,
    };
    let error = verify_recovery_contract(&fixture)
        .await
        .expect_err("wrong recovery next must not pass");
    assert_eq!(error.check, "recovery_pagination");
    let store = fixture.fixture.open().await.expect("failed suite released ownership");
    let epoch = store.acquire_owner().await.expect("new owner after failure");
    store.release_owner(epoch).await.expect("release probe owner");
}
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_harness_rejects_duplicate_recovery_rows_and_releases_owner() {
    let fixture = BrokenFixture {
        fixture: SqliteFixture::new(),
        mutation: Mutation::DuplicatePage,
    };
    let error = verify_recovery_contract(&fixture)
        .await
        .expect_err("duplicate recovery rows must not pass");
    assert_eq!(error.check, "recovery_pagination");
    let store = fixture.fixture.open().await.expect("failed suite released ownership");
    let epoch = store.acquire_owner().await.expect("new owner after failure");
    store.release_owner(epoch).await.expect("release probe owner");
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_harness_core_failure_releases_owner() {
    let fixture = BrokenFixture {
        fixture: SqliteFixture::new(),
        mutation: Mutation::WrongNext,
    };
    let error = verify_core_contract(&fixture)
        .await
        .expect_err("wrong history cursor must fail the owned suite");
    assert_eq!(error.check, "history_pagination");
    let store = fixture
        .fixture
        .open()
        .await
        .expect("failed core suite released ownership");
    let epoch = store
        .acquire_owner()
        .await
        .expect("replacement owner after core failure");
    store.release_owner(epoch).await.expect("release probe owner");
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_core_failure_preserves_cleanup_diagnostic() {
    let calls = Arc::new(AtomicUsize::new(0));
    let fixture = BrokenFixture {
        fixture: SqliteFixture::new(),
        mutation: Mutation::WrongNextAndCleanup(Arc::clone(&calls), 1),
    };
    let error = verify_core_contract(&fixture)
        .await
        .expect_err("both history and cleanup fail");
    assert_eq!(error.check, "history_pagination");
    assert!(error.message.contains("history rows"), "primary diagnostic: {error}");
    assert!(
        error.message.contains("owner_release cleanup failed") && error.message.contains("injected cleanup fault"),
        "cleanup diagnostic: {error}"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1, "owner release was awaited once");
    let store = fixture
        .fixture
        .open()
        .await
        .expect("real owner release completed before injected error");
    let epoch = store
        .acquire_owner()
        .await
        .expect("replacement owner after dual failure");
    store.release_owner(epoch).await.expect("release probe owner");
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_recovery_failure_preserves_cleanup_diagnostic() {
    let calls = Arc::new(AtomicUsize::new(0));
    let fixture = BrokenFixture {
        fixture: SqliteFixture::new(),
        mutation: Mutation::WrongNextAndCleanup(Arc::clone(&calls), 2),
    };
    let error = verify_recovery_contract(&fixture)
        .await
        .expect_err("both recovery page and cleanup fail");
    assert_eq!(error.check, "recovery_pagination");
    assert!(error.message.contains("recovery rows"), "primary diagnostic: {error}");
    assert!(
        error.message.contains("owner_release cleanup failed") && error.message.contains("injected cleanup fault"),
        "cleanup diagnostic: {error}"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2, "both owner generations were released");
    let store = fixture
        .fixture
        .open()
        .await
        .expect("real recovery owner release completed");
    let epoch = store
        .acquire_owner()
        .await
        .expect("replacement owner after recovery dual failure");
    store.release_owner(epoch).await.expect("release probe owner");
}

#[tokio::test]
async fn test_volatile_owner_release_failure_is_reported() {
    let calls = Arc::new(AtomicUsize::new(0));
    let fixture = BrokenFixture {
        fixture: MemoryFixture,
        mutation: Mutation::UnexpectedVolatileOwner(Arc::clone(&calls)),
    };
    let error = verify_core_contract(&fixture)
        .await
        .expect_err("an acquired volatile owner must be released");
    assert_eq!(error.check, "owner_release");
    assert!(
        error.message.contains("unexpected owner cleanup fault"),
        "release diagnostic: {error}"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1, "unexpected owner release was awaited");
}

#[tokio::test]
async fn test_unexpected_shared_owner_preserves_both_cleanup_diagnostics() {
    let calls = Arc::new(AtomicUsize::new(0));
    let fixture = BrokenFixture {
        fixture: MemoryFixture,
        mutation: Mutation::UnexpectedSharedOwner(Arc::clone(&calls)),
    };
    let error = verify_recovery_contract(&fixture)
        .await
        .expect_err("second owner must conflict");
    assert_eq!(error.check, "owner_exclusivity");
    assert!(error.message.contains("second instance"), "primary diagnostic: {error}");
    assert_eq!(
        error.message.matches("owner_release cleanup failed").count(),
        2,
        "both owner cleanup failures: {error}"
    );
    assert!(
        error.message.contains("unexpected owner cleanup fault"),
        "cleanup diagnostic: {error}"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2, "both unexpected owners were released");
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_recovery_contract_accepts_legal_short_pages() {
    let fixture = BrokenFixture {
        fixture: SqliteFixture::new(),
        mutation: Mutation::ShortRecoveryPage,
    };
    let report = verify_recovery_contract(&fixture)
        .await
        .expect("128-row recovery pages satisfy the bounded contract");
    assert!(report.checks.contains(&"recovery_pagination"));
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_recovery_contract_rejects_oversized_empty_and_premature_pages() {
    for (mutation, diagnostic) in [
        (Mutation::OversizedRecoveryPage, "exceeds 256 rows"),
        (Mutation::EmptyRecoveryPage, "ended before expected snapshots"),
        (Mutation::PrematureRecoveryEnd, "recovery rows or next differ"),
    ] {
        let fixture = BrokenFixture {
            fixture: SqliteFixture::new(),
            mutation,
        };
        let error = verify_recovery_contract(&fixture)
            .await
            .expect_err("invalid page must not pass the short-page oracle");
        assert_eq!(error.check, "recovery_pagination");
        assert!(error.message.contains(diagnostic), "{error}");
    }
}
