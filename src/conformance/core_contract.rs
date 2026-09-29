// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//    SPDX-License-Identifier: Apache-2.0
// =============================================================================
use futures::join;

use super::ContractReport;
use super::ContractViolation;
use super::StoreFixture;
use crate::model::AcceptOutcome;
use crate::model::OwnerEpoch;
use crate::model::TaskCursor;
use crate::model::TaskId;
use crate::model::TaskQuery;
use crate::model::TaskRecord;
use crate::model::TaskRequest;
use crate::model::TaskState;
use crate::model::TaskStateKind;
use crate::model::TaskSummary;
use crate::model::TransitionCommand;
use crate::store::StoreError;
use crate::store::TaskStore;

/// Verifies atomic acceptance, CAS, reads, and history queries in a fresh
/// namespace.
///
/// Uses twelve small records and at most four terminal records, so a fixture
/// must retain this dataset. Persistent stores are owned before writes. The
/// suite awaits release on ordinary success and failure; callers must await the
/// entire suite rather than cancel its future while it owns a store.
///
/// # Errors
/// Returns the first violated contract or backend operation failure, including
/// owner cleanup failure. When both checks and cleanup fail, the original
/// check is preserved and cleanup diagnostics are appended. Requires the
/// backend's async runtime and writes disposable fixture data.
pub async fn verify_core_contract(fixture: &dyn StoreFixture) -> Result<ContractReport, ContractViolation> {
    let store = fixture.open().await.map_err(|error| violation("open", error))?;
    let owner = acquire_for_core(store.as_ref()).await?;
    let result = check_core(store.as_ref()).await;
    finish_owner(store.as_ref(), owner, result).await
}

/// Acquires durable ownership, or checks volatile ownership/recovery rejection.
/// Unexpected acquired ownership is released before returning a violation.
async fn acquire_for_core(store: &dyn TaskStore) -> Result<Option<OwnerEpoch>, ContractViolation> {
    let capabilities = store.capabilities();
    if capabilities.restart_recovery && !capabilities.persistent_history {
        return Err(violation(
            "capabilities",
            "restart recovery requires persistent history",
        ));
    }
    if capabilities.persistent_history {
        return store
            .acquire_owner()
            .await
            .map(Some)
            .map_err(|error| violation("owner_acquisition", error));
    }
    match store.acquire_owner().await {
        Err(StoreError::UnsupportedCapability) => {}
        Ok(epoch) => {
            return finish_owner(
                store,
                Some(epoch),
                Err(violation(
                    "capabilities",
                    "volatile ownership must report UnsupportedCapability",
                )),
            )
            .await;
        }
        Err(error) => return Err(violation("capabilities", error)),
    }
    if !matches!(
        store.scan_unfinished(None).await,
        Err(StoreError::UnsupportedCapability)
    ) || !matches!(
        store.release_owner(OwnerEpoch(0)).await,
        Err(StoreError::UnsupportedCapability)
    ) {
        return Err(violation(
            "capabilities",
            "volatile recovery and release must report UnsupportedCapability",
        ));
    }
    Ok(None)
}

/// Awaits owner release for any suite result, preserving the original check
/// and appending cleanup failures rather than discarding either diagnostic.
pub(super) async fn finish_owner<T>(
    store: &dyn TaskStore,
    owner: Option<OwnerEpoch>,
    result: Result<T, ContractViolation>,
) -> Result<T, ContractViolation> {
    if let Some(epoch) = owner
        && let Err(error) = store.release_owner(epoch).await
    {
        return match result {
            Ok(_) => Err(violation("owner_release", error)),
            Err(mut primary) => {
                primary
                    .message
                    .push_str(&format!("; owner_release cleanup failed: {error}"));
                Err(primary)
            }
        };
    }
    result
}

/// Exercises the owned namespace; all stored snapshots form the pagination
/// oracle.
async fn check_core(store: &dyn TaskStore) -> Result<ContractReport, ContractViolation> {
    let request = request(Some("conformance-idempotency"), "group-a");
    let first_id = TaskId::generate();
    let second_id = TaskId::generate();
    let (first, second) = join!(
        store.accept(first_id, request.clone()),
        store.accept(second_id, request.clone())
    );
    let first = first.map_err(|error| violation("atomic_idempotency", error))?;
    let second = second.map_err(|error| violation("atomic_idempotency", error))?;
    let same_id = outcome_record(&first).id == outcome_record(&second).id;
    let one_new = matches!(
        (&first, &second),
        (AcceptOutcome::Accepted(_), AcceptOutcome::Existing(_))
            | (AcceptOutcome::Existing(_), AcceptOutcome::Accepted(_))
    );
    ensure(
        same_id && one_new,
        "atomic_idempotency",
        "two identical concurrent submissions must produce one Accepted and one Existing with the same ID",
    )?;
    ensure(
        outcome_record(&first) == outcome_record(&second),
        "atomic_idempotency",
        "idempotent acceptance must return identical snapshots",
    )?;
    let mut record = outcome_record(&first).clone();
    ensure(
        record.state == TaskState::Queued && record.state_version == 0 && record.attempt == 0,
        "acceptance",
        "new acceptance must be queued at revision and attempt zero",
    )?;
    ensure(
        record.request == request && [first_id, second_id].contains(&record.id),
        "atomic_idempotency",
        "accepted record must retain the request and one proposed ID",
    )?;
    let mut different = request.clone();
    different.payload.push(2);
    ensure(
        matches!(
            store.accept(TaskId::generate(), different).await,
            Err(StoreError::IdempotencyConflict)
        ),
        "idempotency_conflict",
        "different request with same key must return IdempotencyConflict",
    )?;

    let command = transition(&record.summary(), TaskState::Running);
    let (first, second) = join!(store.transition(command.clone()), store.transition(command.clone()));
    let updated = match (first, second) {
        (Ok(summary), Err(StoreError::Conflict)) | (Err(StoreError::Conflict), Ok(summary)) => summary,
        outcomes => {
            return Err(violation(
                "atomic_transition",
                format!("same-version CAS must yield one success and one Conflict: {outcomes:?}"),
            ));
        }
    };
    ensure(
        updated.state == TaskState::Running
            && updated.state_version == record.state_version + 1
            && updated.attempt == record.attempt + 1,
        "atomic_transition",
        "successful transition must advance revision and running attempt",
    )?;
    ensure(
        matches!(store.transition(command).await, Err(StoreError::Conflict)),
        "atomic_transition",
        "stale version must return Conflict",
    )?;
    record = store
        .get(record.id)
        .await
        .map_err(|error| violation("summary_consistency", error))?
        .ok_or_else(|| violation("summary_consistency", "accepted record is missing"))?;
    ensure(
        record.summary() == updated,
        "summary_consistency",
        "transition and full read snapshots differ",
    )?;
    ensure(
        store
            .get_summary(record.id)
            .await
            .map_err(|error| violation("summary_consistency", error))?
            == Some(updated.clone()),
        "summary_consistency",
        "summary and record differ",
    )?;
    ensure(
        store
            .get_by_idempotency_key("conformance-idempotency")
            .await
            .map_err(|error| violation("summary_consistency", error))?
            == Some(record),
        "summary_consistency",
        "idempotency lookup and record differ",
    )?;
    ensure(
        store
            .get_summary_by_idempotency_key("conformance-idempotency")
            .await
            .map_err(|error| violation("summary_consistency", error))?
            == Some(updated.clone()),
        "summary_consistency",
        "idempotency summary differs",
    )?;
    let missing_id = TaskId::generate();
    ensure(
        store
            .get(missing_id)
            .await
            .map_err(|error| violation("summary_consistency", error))?
            .is_none()
            && store
                .get_summary(missing_id)
                .await
                .map_err(|error| violation("summary_consistency", error))?
                .is_none()
            && store
                .get_by_idempotency_key("absent-key")
                .await
                .map_err(|error| violation("summary_consistency", error))?
                .is_none()
            && store
                .get_summary_by_idempotency_key("absent-key")
                .await
                .map_err(|error| violation("summary_consistency", error))?
                .is_none(),
        "summary_consistency",
        "missing reads must return None",
    )?;

    let mut expected = vec![updated];
    for index in 0..11 {
        let id = TaskId::generate();
        let group = if index % 2 == 0 { "group-a" } else { "group-b" };
        let outcome = store
            .accept(id, self::request(None, group))
            .await
            .map_err(|error| violation("acceptance", error))?;
        let mut summary = match outcome {
            AcceptOutcome::Accepted(record) if record.id == id && record.request == self::request(None, group) => {
                record.summary()
            }
            other => {
                return Err(violation(
                    "acceptance",
                    format!("unique request must be Accepted with proposed ID and exact request: {other:?}"),
                ));
            }
        };
        let state = match index % 3 {
            0 => TaskState::Cancelled,
            1 => TaskState::Blocked {
                reason: "fixture".into(),
            },
            _ => TaskState::Running,
        };
        summary = store
            .transition(transition(&summary, state.clone()))
            .await
            .map_err(|error| violation("acceptance", error))?;
        ensure(
            summary.state == state,
            "acceptance",
            "dataset transition must apply its requested state",
        )?;
        let read = store
            .get(id)
            .await
            .map_err(|error| violation("summary_consistency", error))?;
        ensure(
            read.as_ref().map(TaskRecord::summary) == Some(summary.clone()),
            "summary_consistency",
            "dataset summary and full read differ",
        )?;
        expected.push(summary);
    }
    expected.sort_by_key(|summary| TaskCursor::from(summary));
    for limit in [0, 1, 2, 256] {
        verify_history(
            store,
            &expected,
            TaskQuery {
                limit,
                ..TaskQuery::default()
            },
        )
        .await?;
    }
    for states in [
        vec![TaskStateKind::Running],
        vec![
            TaskStateKind::Cancelled,
            TaskStateKind::Blocked,
            TaskStateKind::Cancelled,
        ],
        vec![TaskStateKind::Queued],
    ] {
        for group in [None, Some("group-a"), Some("group-b"), Some("absent-group")] {
            verify_history(
                store,
                &expected,
                TaskQuery {
                    states: states.clone(),
                    correlation_key: group.map(str::to_owned),
                    limit: 2,
                    after: None,
                },
            )
            .await?;
        }
    }
    verify_history(
        store,
        &expected,
        TaskQuery {
            correlation_key: Some("group-a".into()),
            limit: 1,
            ..TaskQuery::default()
        },
    )
    .await?;
    ensure(
        matches!(
            store
                .list(TaskQuery {
                    limit: 257,
                    ..TaskQuery::default()
                })
                .await,
            Err(StoreError::InvalidRequest(_))
        ),
        "history_limit",
        "oversized history limit must return InvalidRequest",
    )?;
    Ok(ContractReport {
        checks: vec![
            "capabilities",
            "atomic_idempotency",
            "idempotency_conflict",
            "atomic_transition",
            "summary_consistency",
            "history_pagination",
            "history_filters",
            "history_limit",
        ],
    })
}

/// Compares every query page and cursor with the sorted acceptance oracle.
/// Bounded expected-page iteration rejects premature termination and phantom
/// tails.
async fn verify_history(
    store: &dyn TaskStore,
    expected: &[TaskSummary],
    mut query: TaskQuery,
) -> Result<(), ContractViolation> {
    let filtered: Vec<_> = expected
        .iter()
        .filter(|row| {
            (query.states.is_empty() || query.states.contains(&row.state.kind()))
                && query
                    .correlation_key
                    .as_ref()
                    .is_none_or(|key| row.request.correlation_key.as_ref() == Some(key))
        })
        .cloned()
        .collect();
    let limit = query.limit.max(1);
    let mut offset = 0;
    loop {
        let page = store
            .list(query.clone())
            .await
            .map_err(|error| violation("history_pagination", error))?;
        let end = (offset + limit).min(filtered.len());
        let expected_next = if end < filtered.len() {
            filtered.get(end - 1).map(TaskCursor::from)
        } else {
            None
        };
        ensure(
            page.records == filtered[offset..end] && page.next == expected_next,
            "history_pagination",
            "history rows, filters, limit, ordering, or continuation differ from retained snapshots",
        )?;
        offset = end;
        if expected_next.is_none() {
            break;
        }
        query.after = expected_next;
    }
    let last = filtered.last().map(TaskCursor::from);
    if let Some(last) = last {
        query.after = Some(last);
        let page = store
            .list(query)
            .await
            .map_err(|error| violation("history_pagination", error))?;
        ensure(
            page.records.is_empty() && page.next.is_none(),
            "history_pagination",
            "exclusive last cursor must return an empty terminal page",
        )?;
    }
    Ok(())
}

/// Builds a valid small request with optional idempotency and correlation keys.
pub(super) fn request(key: Option<&str>, correlation: &str) -> TaskRequest {
    let mut request = TaskRequest::new("conformance.task", "1", vec![1]);
    request.idempotency_key = key.map(str::to_owned);
    request.correlation_key = Some(correlation.to_owned());
    request
}

/// Builds a CAS command from a snapshot and the proposed lifecycle state.
pub(super) fn transition(summary: &TaskSummary, state: TaskState) -> TransitionCommand {
    TransitionCommand {
        id: summary.id,
        expected_version: summary.state_version,
        expected_attempt: summary.attempt,
        state,
        retry_not_before_ms: None,
        output: None,
        assigned_resources: Vec::new(),
        cancel_requested: false,
    }
}

/// Borrows the record in either atomic acceptance outcome.
pub(super) fn outcome_record(outcome: &AcceptOutcome) -> &TaskRecord {
    match outcome {
        AcceptOutcome::Accepted(record) | AcceptOutcome::Existing(record) => record,
    }
}

/// Returns a diagnostic for a failed boolean contract condition.
pub(super) fn ensure(condition: bool, check: &'static str, message: &str) -> Result<(), ContractViolation> {
    if condition {
        Ok(())
    } else {
        Err(violation(check, message))
    }
}

/// Creates contextual diagnostics without using backend strings as assertions.
pub(super) fn violation(check: &'static str, message: impl std::fmt::Display) -> ContractViolation {
    ContractViolation {
        check,
        message: message.to_string(),
    }
}
