// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Service-level acceptance scenarios for the managed operation handler.
//!
//! These tests use explicit watch gates for each externally visible stage.
//! Timeouts are diagnostic bounds only; no elapsed delay is treated as proof
//! that an operation, input, or cleanup has been released.

use std::future::Future;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use qubit_operation::AsyncOperationManager;
use qubit_operation::ManagerLimits;
use qubit_operation::ManagerOptions;
use qubit_operation::ShutdownMode;
use qubit_task::model::TaskState;
use qubit_task::service::CancelOutcome;
use tokio::sync::watch;

use super::super::external_job::AttemptGates;
use super::super::fixture;
use super::super::probe_request;

const CASE_TIMEOUT: Duration = Duration::from_secs(10);
const FAILURE_DRAIN_TIMEOUT: Duration = Duration::from_secs(2);

fn manager(active_operations: usize) -> AsyncOperationManager {
    let mut options = ManagerOptions::default();
    options.limits = ManagerLimits {
        active_operations,
        ..ManagerLimits::default()
    };
    AsyncOperationManager::new(options).expect("test operation manager")
}

async fn gate(receiver: &mut watch::Receiver<bool>) {
    receiver
        .wait_for(|ready| *ready)
        .await
        .expect("gate sender remains alive");
}

fn open(sender: &watch::Sender<bool>) {
    sender.send_replace(true);
}

fn gate_state(gates: &[&AttemptGates]) -> String {
    format!(
        "{}",
        gates
            .iter()
            .enumerate()
            .map(|(index, gates)| format!(
                "attempt[{index}]={{activated={}, stop_seen={}, input_released={}, domain_settled={}, allow_outcome={}, allow_input_release={}, allow_domain_settlement={}, allow_cleanup={}, parent_outcome_seen={}, cleanup_started={}, cleanup_finished={}, retryable_failure={}}}",
                *gates.activated.borrow(),
                *gates.stop_seen.borrow(),
                *gates.input_released.borrow(),
                *gates.domain_settled.borrow(),
                *gates.allow_outcome.borrow(),
                *gates.allow_input_release.borrow(),
                *gates.allow_domain_settlement.borrow(),
                *gates.allow_cleanup.borrow(),
                *gates.parent_outcome_seen.borrow(),
                *gates.cleanup_started.borrow(),
                *gates.cleanup_finished.borrow(),
                *gates.retryable_failure.borrow(),
            ))
            .collect::<Vec<_>>()
            .join(", "),
    )
}

async fn with_timeout_diagnostics<F>(
    label: &str,
    future: F,
    service: &qubit_task::TaskExecutionService,
    id: qubit_task::TaskId,
    gates: &[&AttemptGates],
    manager: &AsyncOperationManager,
) where
    F: Future<Output = ()>,
{
    if tokio::time::timeout(CASE_TIMEOUT, future).await.is_err() {
        let summary = service.get(id).await.ok().flatten();
        let operation_snapshot = manager.request_shutdown(ShutdownMode::Drain).snapshot();
        for gates in gates {
            open(&gates.allow_outcome);
            open(&gates.allow_input_release);
            open(&gates.allow_domain_settlement);
            open(&gates.allow_cleanup);
        }
        // This is best-effort cleanup after the scenario has already timed out;
        // it never changes the recorded release evidence or the test outcome.
        let _ = tokio::time::timeout(FAILURE_DRAIN_TIMEOUT, service.shutdown()).await;
        let shutdown = manager.request_shutdown(ShutdownMode::Drain);
        let _ = tokio::time::timeout(FAILURE_DRAIN_TIMEOUT, shutdown.wait()).await;
        panic!(
            "{label} timed out: task_summary={summary:?}, gates={}, operation_snapshot={operation_snapshot:?}",
            gate_state(gates),
        );
    }
}

async fn activated(gates: &AttemptGates) {
    gate(&mut gates.activated.subscribe()).await;
}

async fn terminal(
    service: &qubit_task::TaskExecutionService,
    id: qubit_task::model::typed::TaskId,
) -> qubit_task::model::typed::TaskSummary {
    loop {
        if let Some(summary) = service.get(id).await.expect("read task summary")
            && summary.state.is_terminal()
        {
            return summary;
        }
        tokio::task::yield_now().await;
    }
}

async fn shutdown_manager(manager: &AsyncOperationManager) {
    let report = manager
        .request_shutdown(ShutdownMode::Drain)
        .wait()
        .await
        .expect("operation manager drains");
    let snapshot = report.snapshot;
    assert_eq!(snapshot.active_operations, 0);
    assert_eq!(snapshot.queued_operations, 0);
    assert_eq!(snapshot.unconfirmed_users, 0);
    assert_eq!(snapshot.cleaning_resources, 0);
    assert_eq!(snapshot.recovery_groups, 0);
    assert_eq!(snapshot.service_resources, 0);
}

#[tokio::test]
async fn test_cancel_holds_resources_until_cleanup() {
    let manager = manager(4);
    let gates = AttemptGates::new();
    let (service, request) = fixture(manager.clone(), vec![Arc::clone(&gates)], NonZeroU32::new(1).unwrap())
        .await
        .expect("service fixture");
    let accepted = service.submit(request).await.expect("submit managed task");
    with_timeout_diagnostics(
        "cancel_holds_resources_until_cleanup",
        async {
            activated(&gates).await;
            assert_eq!(
                service.cancel(accepted.id).await.expect("request cancellation"),
                CancelOutcome::CancellationRequested
            );
            gate(&mut gates.stop_seen.subscribe()).await;
            gate(&mut gates.parent_outcome_seen.subscribe()).await;
            assert_eq!(
                service.get(accepted.id).await.unwrap().unwrap().state,
                TaskState::Running
            );

            let probe = service.submit(probe_request()).await.expect("submit resource probe");
            // The managed attempt has reached its parent outcome but still owns
            // the task CPU reservation. Observe the probe remain queued while the
            // attempt's settlement gates are deliberately closed.
            let probe_now = service.get(probe.id).await.unwrap().unwrap();
            assert_eq!(probe_now.state, TaskState::Queued);
            assert_eq!(
                service.get(accepted.id).await.unwrap().unwrap().state,
                TaskState::Running
            );

            open(&gates.allow_input_release);
            gate(&mut gates.input_released.subscribe()).await;
            assert_eq!(
                service.get(accepted.id).await.unwrap().unwrap().state,
                TaskState::Running
            );
            open(&gates.allow_domain_settlement);
            gate(&mut gates.domain_settled.subscribe()).await;
            assert_eq!(
                service.get(accepted.id).await.unwrap().unwrap().state,
                TaskState::Running
            );
            open(&gates.allow_cleanup);
            gate(&mut gates.cleanup_finished.subscribe()).await;

            let cancelled = terminal(&service, accepted.id).await;
            assert_eq!(cancelled.state, TaskState::Cancelled);
            let probe_done = terminal(&service, probe.id).await;
            assert_eq!(probe_done.state, TaskState::Succeeded);
            service.shutdown().await.expect("task service shutdown");
            shutdown_manager(&manager).await;
        },
        &service,
        accepted.id,
        &[&gates],
        &manager,
    )
    .await;
}

#[tokio::test]
async fn test_drain_waits_for_managed_attempt() {
    let manager = manager(4);
    let gates = AttemptGates::new();
    let (service, request) = fixture(manager.clone(), vec![Arc::clone(&gates)], NonZeroU32::new(1).unwrap())
        .await
        .expect("service fixture");
    let accepted = service.submit(request).await.expect("submit managed task");
    with_timeout_diagnostics(
        "drain_waits_for_managed_attempt",
        async {
            activated(&gates).await;
            let shutdown = service.shutdown();
            tokio::pin!(shutdown);
            assert!(futures::poll!(shutdown.as_mut()).is_pending());
            // Shutdown closes task admission before waiting for in-flight work.
            let rejected = service.submit(probe_request()).await;
            assert!(rejected.is_err(), "shutdown must reject new submissions");
            assert_eq!(
                service.get(accepted.id).await.unwrap().unwrap().state,
                TaskState::Running
            );

            open(&gates.allow_outcome);
            gate(&mut gates.parent_outcome_seen.subscribe()).await;
            open(&gates.allow_input_release);
            gate(&mut gates.input_released.subscribe()).await;
            open(&gates.allow_domain_settlement);
            gate(&mut gates.domain_settled.subscribe()).await;
            assert!(futures::poll!(shutdown.as_mut()).is_pending());
            let manager_shutdown = manager.request_shutdown(ShutdownMode::Drain);
            assert!(
                manager_shutdown.snapshot().active_operations > 0,
                "the manager stays active while task service waits for cleanup"
            );

            open(&gates.allow_cleanup);
            gate(&mut gates.cleanup_finished.subscribe()).await;
            shutdown.await.expect("service shutdown");
            shutdown_manager(&manager).await;
        },
        &service,
        accepted.id,
        &[&gates],
        &manager,
    )
    .await;
}

#[tokio::test]
async fn test_retry_starts_only_after_settlement() {
    let manager = manager(4);
    let first = AttemptGates::new();
    let second = AttemptGates::new();
    first.retryable_failure.send_replace(true);
    let (service, request) = fixture(
        manager.clone(),
        vec![Arc::clone(&first), Arc::clone(&second)],
        NonZeroU32::new(2).unwrap(),
    )
    .await
    .expect("service fixture");
    let accepted = service.submit(request).await.expect("submit managed task");
    with_timeout_diagnostics(
        "retry_starts_only_after_settlement",
        async {
            activated(&first).await;
            open(&first.allow_outcome);
            gate(&mut first.parent_outcome_seen.subscribe()).await;
            assert_eq!(service.get(accepted.id).await.unwrap().unwrap().attempt, 1);
            assert!(!*second.activated.borrow(), "attempt 2 waits for attempt 1 cleanup");

            open(&first.allow_input_release);
            gate(&mut first.input_released.subscribe()).await;
            open(&first.allow_domain_settlement);
            gate(&mut first.domain_settled.subscribe()).await;
            assert_eq!(service.get(accepted.id).await.unwrap().unwrap().attempt, 1);
            assert!(!*second.activated.borrow(), "settlement alone does not bypass cleanup");
            open(&first.allow_cleanup);
            gate(&mut first.cleanup_finished.subscribe()).await;
            activated(&second).await;
            open(&second.allow_outcome);
            open(&second.allow_input_release);
            open(&second.allow_domain_settlement);
            open(&second.allow_cleanup);
            gate(&mut second.cleanup_finished.subscribe()).await;
            assert_eq!(terminal(&service, accepted.id).await.state, TaskState::Succeeded);
            service.shutdown().await.expect("task service shutdown");
            shutdown_manager(&manager).await;
        },
        &service,
        accepted.id,
        &[&first, &second],
        &manager,
    )
    .await;
}

#[tokio::test]
async fn test_cancel_after_outcome_suppresses_retry() {
    let manager = manager(4);
    let first = AttemptGates::new();
    let second = AttemptGates::new();
    first.retryable_failure.send_replace(true);
    let (service, request) = fixture(
        manager.clone(),
        vec![Arc::clone(&first), Arc::clone(&second)],
        NonZeroU32::new(2).unwrap(),
    )
    .await
    .expect("service fixture");
    let accepted = service.submit(request).await.expect("submit managed task");
    with_timeout_diagnostics(
        "cancel_after_outcome_suppresses_retry",
        async {
            activated(&first).await;
            open(&first.allow_outcome);
            gate(&mut first.parent_outcome_seen.subscribe()).await;
            assert_eq!(
                service.cancel(accepted.id).await.expect("persist late cancellation"),
                CancelOutcome::CancellationRequested
            );
            assert!(service.get(accepted.id).await.unwrap().unwrap().cancel_requested);
            open(&first.allow_input_release);
            open(&first.allow_domain_settlement);
            gate(&mut first.domain_settled.subscribe()).await;
            open(&first.allow_cleanup);
            gate(&mut first.cleanup_finished.subscribe()).await;
            let failed = terminal(&service, accepted.id).await;
            assert!(matches!(failed.state, TaskState::Failed { .. }));
            assert!(!*second.activated.borrow(), "late cancellation suppresses retry");
            service.shutdown().await.expect("task service shutdown");
            shutdown_manager(&manager).await;
        },
        &service,
        accepted.id,
        &[&first, &second],
        &manager,
    )
    .await;
}

#[tokio::test]
async fn test_rejected_root_does_not_activate_job() {
    let manager = manager(4);
    manager.close_admission();
    let gates = AttemptGates::new();
    let (service, request) = fixture(manager.clone(), vec![Arc::clone(&gates)], NonZeroU32::new(1).unwrap())
        .await
        .expect("service fixture");
    let accepted = service.submit(request).await.expect("submit managed task");
    with_timeout_diagnostics(
        "rejected_root_does_not_activate_job",
        async {
            let failed = terminal(&service, accepted.id).await;
            assert!(
                matches!(failed.state, TaskState::Failed { ref category, .. } if category == "operation_admission")
            );
            assert!(!*gates.activated.borrow(), "rejected root does not start external work");
            let probe = service
                .submit(probe_request())
                .await
                .expect("probe retains service capacity");
            assert_eq!(terminal(&service, probe.id).await.state, TaskState::Succeeded);
            service.shutdown().await.expect("task service shutdown");
            shutdown_manager(&manager).await;
        },
        &service,
        accepted.id,
        &[&gates],
        &manager,
    )
    .await;
}

#[tokio::test]
async fn test_rejected_child_does_not_activate_job() {
    let mut options = ManagerOptions::default();
    options.limits = ManagerLimits {
        unconsumed_outcomes: 1,
        ..ManagerLimits::default()
    };
    let manager = AsyncOperationManager::new(options).expect("test operation manager");
    let gates = AttemptGates::new();
    open(&gates.allow_cleanup);
    let (service, request) = fixture(manager.clone(), vec![Arc::clone(&gates)], NonZeroU32::new(1).unwrap())
        .await
        .expect("service fixture");
    let accepted = service.submit(request).await.expect("submit managed task");
    with_timeout_diagnostics(
        "rejected_child_does_not_activate_job",
        async {
            gate(&mut gates.cleanup_started.subscribe()).await;
            gate(&mut gates.input_released.subscribe()).await;
            gate(&mut gates.cleanup_finished.subscribe()).await;
            let failed = terminal(&service, accepted.id).await;
            assert!(
                matches!(failed.state, TaskState::Failed { ref category, .. } if category == "operation_admission")
            );
            assert!(!*gates.activated.borrow(), "rejected child does not activate the job");
            assert!(*gates.cleanup_started.borrow(), "root operation was admitted");
            assert!(*gates.input_released.borrow(), "unstarted child returned its input");
            service.shutdown().await.expect("task service shutdown");
            shutdown_manager(&manager).await;
        },
        &service,
        accepted.id,
        &[&gates],
        &manager,
    )
    .await;
}
