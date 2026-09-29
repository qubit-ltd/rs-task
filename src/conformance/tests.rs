// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//    SPDX-License-Identifier: Apache-2.0
// =============================================================================
//! Fault-injection tests for conformance diagnostics and owner cleanup.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use super::StoreFixture;
use super::verify_core_contract;
use super::verify_recovery_contract;
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
use crate::model::TaskStateKind;
use crate::model::TaskSummary;
use crate::model::TransitionCommand;
use crate::store::MemoryTaskStore;
use crate::store::StoreError;
use crate::store::TaskFuture;
use crate::store::TaskStore;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Operation {
    Open,
    Acquire,
    Release,
    Accept,
    GetByKey,
    GetSummaryByKey,
    GetSummary,
    Transition,
    Get,
    List,
    Count,
    Scan,
}

struct FaultFixture {
    control: Arc<FaultControl>,
    inner: Arc<MemoryTaskStore>,
}

struct FaultControl {
    failed_operation: Operation,
    fail_at: usize,
    calls: AtomicUsize,
    owner_active: AtomicBool,
    next_epoch: AtomicU64,
}

impl FaultFixture {
    fn new(failed_operation: Operation, fail_at: usize) -> Self {
        Self {
            control: Arc::new(FaultControl {
                failed_operation,
                fail_at,
                calls: AtomicUsize::new(0),
                owner_active: AtomicBool::new(false),
                next_epoch: AtomicU64::new(1),
            }),
            inner: Arc::new(MemoryTaskStore::new(1024)),
        }
    }

    fn store(&self) -> Arc<FaultStore> {
        Arc::new(FaultStore {
            inner: Arc::clone(&self.inner),
            control: Arc::clone(&self.control),
            owns: AtomicBool::new(false),
        })
    }
}

impl FaultControl {
    fn should_fail(&self, operation: Operation) -> bool {
        if self.failed_operation != operation {
            return false;
        }
        self.calls.fetch_add(1, Ordering::Relaxed) + 1 == self.fail_at
    }
}

impl StoreFixture for FaultFixture {
    fn open<'a>(&'a self) -> TaskFuture<'a, Result<Arc<dyn TaskStore>, StoreError>> {
        Box::pin(async move {
            if self.control.should_fail(Operation::Open) {
                return Err(StoreError::Failure("injected open failure".into()));
            }
            Ok(self.store() as Arc<dyn TaskStore>)
        })
    }
}

struct FaultStore {
    inner: Arc<MemoryTaskStore>,
    control: Arc<FaultControl>,
    owns: AtomicBool,
}

impl FaultStore {
    fn should_fail(&self, operation: Operation) -> bool {
        self.control.should_fail(operation)
    }

    fn injected_error() -> StoreError {
        StoreError::Failure("injected operation failure".into())
    }
}

impl TaskStore for FaultStore {
    fn capabilities(&self) -> StoreCapabilities {
        // Tests deliberately select the durable contract branch to exercise
        // its diagnostics. Each recovery test injects a failure before the
        // suite can pass, so this synthetic wrapper is never a conformance
        // claim for the in-memory backend.
        StoreCapabilities {
            persistent_history: true,
            restart_recovery: true,
        }
    }

    fn accept<'a>(&'a self, id: TaskId, request: TaskRequest) -> TaskFuture<'a, Result<AcceptOutcome, StoreError>> {
        Box::pin(async move {
            if !self.owns.load(Ordering::Acquire) {
                return Err(StoreError::OwnerConflict);
            }
            if self.should_fail(Operation::Accept) {
                return Err(Self::injected_error());
            }
            self.inner.accept(id, request).await
        })
    }

    fn get_by_idempotency_key<'a>(&'a self, key: &'a str) -> TaskFuture<'a, Result<Option<TaskRecord>, StoreError>> {
        Box::pin(async move {
            if self.should_fail(Operation::GetByKey) {
                return Err(Self::injected_error());
            }
            self.inner.get_by_idempotency_key(key).await
        })
    }

    fn get_summary_by_idempotency_key<'a>(
        &'a self,
        key: &'a str,
    ) -> TaskFuture<'a, Result<Option<TaskSummary>, StoreError>> {
        Box::pin(async move {
            if self.should_fail(Operation::GetSummaryByKey) {
                return Err(Self::injected_error());
            }
            self.inner.get_summary_by_idempotency_key(key).await
        })
    }

    fn transition<'a>(&'a self, command: TransitionCommand) -> TaskFuture<'a, Result<TaskSummary, StoreError>> {
        Box::pin(async move {
            if !self.owns.load(Ordering::Acquire) {
                return Err(StoreError::OwnerConflict);
            }
            if self.should_fail(Operation::Transition) {
                return Err(Self::injected_error());
            }
            self.inner.transition(command).await
        })
    }

    fn get_summary<'a>(&'a self, id: TaskId) -> TaskFuture<'a, Result<Option<TaskSummary>, StoreError>> {
        Box::pin(async move {
            if self.should_fail(Operation::GetSummary) {
                return Err(Self::injected_error());
            }
            self.inner.get_summary(id).await
        })
    }

    fn get<'a>(&'a self, id: TaskId) -> TaskFuture<'a, Result<Option<TaskRecord>, StoreError>> {
        Box::pin(async move {
            if self.should_fail(Operation::Get) {
                return Err(Self::injected_error());
            }
            self.inner.get(id).await
        })
    }

    fn list<'a>(&'a self, query: TaskQuery) -> TaskFuture<'a, Result<TaskPage, StoreError>> {
        Box::pin(async move {
            if self.should_fail(Operation::List) {
                return Err(Self::injected_error());
            }
            self.inner.list(query).await
        })
    }

    fn count_states<'a>(&'a self) -> TaskFuture<'a, Result<TaskStateCounts, StoreError>> {
        self.inner.count_states()
    }

    fn acquire_owner<'a>(&'a self) -> TaskFuture<'a, Result<OwnerEpoch, StoreError>> {
        Box::pin(async move {
            if self.should_fail(Operation::Acquire) {
                return Err(Self::injected_error());
            }
            if self
                .control
                .owner_active
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                return Err(StoreError::OwnerConflict);
            }
            self.owns.store(true, Ordering::Release);
            Ok(OwnerEpoch(self.control.next_epoch.fetch_add(1, Ordering::Relaxed)))
        })
    }

    fn has_unfinished_over_limit<'a>(&'a self, limit: usize) -> TaskFuture<'a, Result<bool, StoreError>> {
        Box::pin(async move {
            // Count the bounded recovery scans and let later fault positions
            // target each of the two contract-level count error mappings.
            if self.should_fail(Operation::Scan) {
                return Err(Self::injected_error());
            }
            if self.should_fail(Operation::Count) {
                return Err(Self::injected_error());
            }
            self.inner.has_unfinished_over_limit(limit).await
        })
    }

    fn scan_unfinished<'a>(&'a self, cursor: Option<TaskCursor>) -> TaskFuture<'a, Result<RecoveryPage, StoreError>> {
        Box::pin(async move {
            if self.should_fail(Operation::Scan) {
                return Err(Self::injected_error());
            }
            let page = self
                .inner
                .list(TaskQuery {
                    states: vec![TaskStateKind::Queued, TaskStateKind::Running],
                    limit: 256,
                    after: cursor,
                    correlation_key: None,
                })
                .await?;
            Ok(RecoveryPage {
                tasks: page.records,
                next: page.next,
            })
        })
    }

    fn release_owner<'a>(&'a self, epoch: OwnerEpoch) -> TaskFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            if self.should_fail(Operation::Release) {
                return Err(Self::injected_error());
            }
            let _ = epoch;
            self.owns.store(false, Ordering::Release);
            self.control.owner_active.store(false, Ordering::Release);
            Ok(())
        })
    }
}

#[tokio::test]
async fn test_core_contract_faults_preserve_check_and_cleanup() {
    for (operation, expected_check, fail_at) in [
        (Operation::Open, "open", 1),
        (Operation::Acquire, "owner_acquisition", 1),
        (Operation::Release, "owner_release", 1),
        // The two concurrent calls share the operation counter; either error
        // must be reported as the atomic idempotency check.
        (Operation::Accept, "atomic_idempotency", 1),
        (Operation::Accept, "atomic_idempotency", 2),
        (Operation::Accept, "acceptance", 4),
        (Operation::Transition, "acceptance", 4),
        (Operation::Get, "summary_consistency", 1),
        (Operation::Get, "summary_consistency", 2),
        (Operation::GetSummary, "summary_consistency", 1),
        (Operation::GetSummary, "summary_consistency", 2),
        (Operation::GetByKey, "summary_consistency", 1),
        (Operation::GetByKey, "summary_consistency", 2),
        (Operation::GetSummaryByKey, "summary_consistency", 1),
        (Operation::GetSummaryByKey, "summary_consistency", 2),
        (Operation::List, "history_pagination", 1),
        // The first history query uses twelve single-row pages. Its final
        // terminal-cursor probe is the thirteenth list call and the second
        // list error-mapping site in verify_history.
        (Operation::List, "history_pagination", 13),
    ] {
        let fixture = FaultFixture::new(operation, fail_at);
        let error = verify_core_contract(&fixture)
            .await
            .expect_err("injected operation must violate the selected contract");
        assert_eq!(error.check, expected_check, "operation: {operation:?}");
        let expected_message = if operation == Operation::Open {
            "injected open failure"
        } else {
            "injected operation failure"
        };
        assert!(error.message.contains(expected_message), "operation: {operation:?}");
        if operation == Operation::Release {
            assert_eq!(fixture.control.calls.load(Ordering::Relaxed), 1);
        }
    }
}

#[tokio::test]
async fn test_recovery_contract_faults_report_open_accept_and_scan_errors() {
    for (operation, fail_at, expected_check) in [
        (Operation::Open, 2, "owner_exclusivity"),
        (Operation::Open, 3, "recovery_persistence"),
        (Operation::Accept, 1, "recovery_acceptance"),
        (Operation::Accept, 514, "recovery_acceptance"),
        (Operation::Acquire, 3, "owner_acquisition"),
        (Operation::Acquire, 2, "owner_exclusivity"),
        (Operation::Release, 1, "owner_release"),
        (Operation::Transition, 1, "recovery_acceptance"),
        // 171 transitions seed the 513 recoverable rows; the next transition
        // is the first excluded terminal record.
        (Operation::Transition, 172, "recovery_acceptance"),
        (Operation::Scan, 1, "recovery_pagination"),
        (Operation::Scan, 7, "recovery_count"),
        (Operation::Scan, 8, "recovery_count"),
        (Operation::Get, 1, "recovery_persistence"),
        (Operation::Count, 1, "recovery_count"),
        (Operation::Count, 2, "recovery_count"),
    ] {
        let fixture = FaultFixture::new(operation, fail_at);
        let error = verify_recovery_contract(&fixture)
            .await
            .expect_err("injected recovery operation must fail the contract");
        assert_eq!(error.check, expected_check, "operation: {operation:?}");
        let expected_message = if operation == Operation::Open {
            "injected open failure"
        } else {
            "injected operation failure"
        };
        assert!(error.message.contains(expected_message), "operation: {operation:?}");
    }
}
