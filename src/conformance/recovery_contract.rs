// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//    SPDX-License-Identifier: Apache-2.0
// =============================================================================
use std::sync::Arc;

use futures::future::join_all;

use super::ContractReport;
use super::ContractViolation;
use super::StoreFixture;
use super::core_contract::ensure;
use super::core_contract::finish_owner;
use super::core_contract::request;
use super::core_contract::transition;
use super::core_contract::violation;
use crate::model::AcceptOutcome;
use crate::model::OwnerEpoch;
use crate::model::TaskCursor;
use crate::model::TaskId;
use crate::model::TaskState;
use crate::model::TaskSummary;
use crate::store::StoreError;
use crate::store::TaskStore;

/// Verifies durable recovery, ownership fencing, and strict recovery pages.
///
/// The fresh fixture must retain 513 unfinished records plus two excluded
/// records. Repeated opens must address the same durable namespace. This suite
/// requires the backend runtime, writes small fixture requests, and awaits
/// owner release even after an ordinary violation. Await the whole suite; do
/// not cancel its future while it owns a store.
///
/// Black-box success does not prove write draining after cancellation or
/// transaction failure. Backend authors must also supply controlled barrier
/// and fault-injection tests, as described in the module documentation.
///
/// # Errors
/// Unsupported recovery is a `recovery_capability` violation, never a pass or
/// skip. Other failures identify the first violated contract or operation.
pub async fn verify_recovery_contract(fixture: &dyn StoreFixture) -> Result<ContractReport, ContractViolation> {
    let store = fixture.open().await.map_err(|error| violation("open", error))?;
    let capabilities = store.capabilities();
    ensure(
        capabilities.restart_recovery && capabilities.persistent_history,
        "recovery_capability",
        "recovery requires both restart_recovery and persistent_history capabilities",
    )?;
    let epoch = store
        .acquire_owner()
        .await
        .map_err(|error| violation("owner_acquisition", error))?;
    let mut owner = Some(epoch);
    let result = check_first_owner(fixture, &store, epoch, &mut owner).await;
    finish_owner(store.as_ref(), owner, result).await
}

/// Verifies exclusivity and seeds the namespace before releasing the first
/// owner. The caller retains cleanup responsibility until release succeeds.
async fn check_first_owner(
    fixture: &dyn StoreFixture,
    store: &Arc<dyn TaskStore>,
    epoch: OwnerEpoch,
    owner: &mut Option<OwnerEpoch>,
) -> Result<ContractReport, ContractViolation> {
    match fixture.open().await {
        Err(StoreError::OwnerConflict) => {}
        Ok(other) => match other.acquire_owner().await {
            Err(StoreError::OwnerConflict) => {}
            Ok(other_epoch) => {
                let _ = other.release_owner(other_epoch).await;
                return Err(violation(
                    "owner_exclusivity",
                    "second instance acquired ownership while first owner remained active",
                ));
            }
            Err(error) => return Err(violation("owner_exclusivity", error)),
        },
        Err(error) => return Err(violation("owner_exclusivity", error)),
    }
    let ids: Vec<_> = (0..513).map(|_| TaskId::generate()).collect();
    let outcomes = join_all(ids.iter().map(|&id| store.accept(id, request(None, "recovery")))).await;
    let mut expected = Vec::with_capacity(ids.len());
    for (id, outcome) in ids.into_iter().zip(outcomes) {
        let record = match outcome.map_err(|error| violation("recovery_acceptance", error))? {
            AcceptOutcome::Accepted(record) if record.id == id && record.request == request(None, "recovery") => record,
            other => {
                return Err(violation(
                    "recovery_acceptance",
                    format!("unique recovery task must be Accepted with proposed ID and exact request: {other:?}"),
                ));
            }
        };
        let mut summary = record.summary();
        if expected.len() % 3 == 0 {
            summary = store
                .transition(transition(&summary, TaskState::Running))
                .await
                .map_err(|error| violation("recovery_acceptance", error))?;
            ensure(
                summary.state == TaskState::Running,
                "recovery_acceptance",
                "recovery dataset must include Running rows",
            )?;
        }
        expected.push(summary);
    }
    let mut excluded = Vec::with_capacity(2);
    for state in [
        TaskState::Blocked {
            reason: "excluded fixture".into(),
        },
        TaskState::Cancelled,
    ] {
        let outcome = store
            .accept(TaskId::generate(), request(None, "excluded"))
            .await
            .map_err(|error| violation("recovery_acceptance", error))?;
        let record = match outcome {
            AcceptOutcome::Accepted(record) => record,
            other => {
                return Err(violation(
                    "recovery_acceptance",
                    format!("excluded fixture task must be new: {other:?}"),
                ));
            }
        };
        excluded.push(
            store
                .transition(transition(&record.summary(), state))
                .await
                .map_err(|error| violation("recovery_acceptance", error))?,
        );
    }
    expected.sort_by_key(|summary| TaskCursor::from(summary));
    store
        .release_owner(epoch)
        .await
        .map_err(|error| violation("owner_release", error))?;
    *owner = None;
    verify_fenced(store.as_ref(), &expected[0]).await?;
    let reopened = fixture
        .open()
        .await
        .map_err(|error| violation("recovery_persistence", error))?;
    let new_epoch = reopened
        .acquire_owner()
        .await
        .map_err(|error| violation("owner_acquisition", error))?;
    let result = check_reopened(
        reopened.as_ref(),
        store.as_ref(),
        epoch,
        new_epoch,
        &expected,
        &excluded,
    )
    .await;
    finish_owner(reopened.as_ref(), Some(new_epoch), result).await
}

/// Checks that an old instance rejects both acceptance and lifecycle writes.
/// OwnerConflict and general Failure are valid rejection categories;
/// unsupported capabilities, CAS conflicts, and request errors do not establish
/// fencing.
async fn verify_fenced(store: &dyn TaskStore, summary: &TaskSummary) -> Result<(), ContractViolation> {
    let accepted = store.accept(TaskId::generate(), request(None, "fenced")).await;
    ensure(
        matches!(accepted, Err(StoreError::OwnerConflict | StoreError::Failure(_))),
        "owner_fencing",
        "released instance must reject acceptance with an ownership or operational failure",
    )?;
    let updated = store.transition(transition(summary, TaskState::Cancelled)).await;
    ensure(
        matches!(updated, Err(StoreError::OwnerConflict | StoreError::Failure(_))),
        "owner_fencing",
        "released instance must reject transitions with an ownership or operational failure",
    )
}

/// Checks durable snapshots and bounded recovery under the replacement owner.
async fn check_reopened(
    store: &dyn TaskStore,
    old_store: &dyn TaskStore,
    old_epoch: OwnerEpoch,
    new_epoch: OwnerEpoch,
    expected: &[TaskSummary],
    excluded: &[TaskSummary],
) -> Result<ContractReport, ContractViolation> {
    ensure(
        store.capabilities().restart_recovery && store.capabilities().persistent_history,
        "recovery_capability",
        "capability claims changed on reopen",
    )?;
    let OwnerEpoch(old_generation) = old_epoch;
    let OwnerEpoch(new_generation) = new_epoch;
    ensure(
        new_generation > old_generation,
        "owner_epoch",
        "replacement owner must hold a fresh epoch",
    )?;
    verify_fenced(old_store, &expected[0]).await?;
    for summary in expected.iter().chain(excluded) {
        let record = store
            .get(summary.id)
            .await
            .map_err(|error| violation("recovery_persistence", error))?;
        ensure(
            record.as_ref().map(|record| record.summary()) == Some(summary.clone()),
            "recovery_persistence",
            "durable record lifecycle changed or disappeared on reopen",
        )?;
        ensure(
            store
                .get_summary(summary.id)
                .await
                .map_err(|error| violation("recovery_persistence", error))?
                == Some(summary.clone()),
            "recovery_persistence",
            "durable summary changed or disappeared on reopen",
        )?;
    }
    verify_pages(store, expected, None).await?;
    // Also require an exactly full terminal page (256) and two full pages (512).
    verify_pages(store, &expected[257..], Some(TaskCursor::from(&expected[256]))).await?;
    verify_pages(store, &expected[1..], Some(TaskCursor::from(&expected[0]))).await?;
    ensure(
        !store
            .has_unfinished_over_limit(513)
            .await
            .map_err(|error| violation("recovery_count", error))?
            && store
                .has_unfinished_over_limit(512)
                .await
                .map_err(|error| violation("recovery_count", error))?,
        "recovery_count",
        "unfinished count must exclude blocked/terminal records and use an exclusive limit",
    )?;
    let last = expected
        .last()
        .ok_or_else(|| violation("recovery_pagination", "fixture has no unfinished records"))?;
    verify_pages(store, &[], Some(TaskCursor::from(last))).await?;
    // A fresh write proves the reopened instance actually holds usable ownership.
    ensure(
        matches!(
            store.accept(TaskId::generate(), request(None, "new-owner")).await,
            Ok(AcceptOutcome::Accepted(_))
        ),
        "owner_epoch",
        "replacement owner must accept a fresh task",
    )?;
    Ok(ContractReport {
        checks: vec![
            "recovery_capability",
            "owner_exclusivity",
            "owner_fencing",
            "owner_epoch",
            "recovery_persistence",
            "recovery_pagination",
            "recovery_count",
        ],
    })
}

/// Compares strict 256-row pages to the durable oracle, including exact next
/// keys. Bounded iteration catches duplicate, unordered, missing, extra, and
/// excluded rows.
async fn verify_pages(
    store: &dyn TaskStore,
    expected: &[TaskSummary],
    mut after: Option<TaskCursor>,
) -> Result<(), ContractViolation> {
    let mut offset = 0;
    loop {
        let page = store
            .scan_unfinished(after)
            .await
            .map_err(|error| violation("recovery_pagination", error))?;
        let end = (offset + 256).min(expected.len());
        let next = if end < expected.len() {
            expected.get(end - 1).map(TaskCursor::from)
        } else {
            None
        };
        ensure(
            page.tasks == expected[offset..end] && page.next == next,
            "recovery_pagination",
            "recovery rows or next differ from strict sorted unfinished snapshots",
        )?;
        let mut previous = after;
        for row in &page.tasks {
            let key = TaskCursor::from(row);
            ensure(
                matches!(row.state, TaskState::Queued | TaskState::Running) && previous.is_none_or(|lower| key > lower),
                "recovery_pagination",
                "recovery keys must strictly increase above after and states must be Queued/Running",
            )?;
            previous = Some(key);
        }
        offset = end;
        if next.is_none() {
            break;
        }
        after = next;
    }
    Ok(())
}
