//! Service-level acceptance scenarios for the managed operation handler.
//!
//! These tests use explicit watch gates for each externally visible stage.
//! Timeouts are diagnostic bounds only; no elapsed delay is treated as proof
//! that an operation, input, or cleanup has been released.

use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use qubit_operation::AsyncOperationManager;
use qubit_operation::ManagerLimits;
use qubit_operation::ManagerOptions;
use qubit_operation::ShutdownMode;
use qubit_task::model::TaskState;
use tokio::sync::watch;

use super::external_job::AttemptGates;
use super::fixture;
use super::probe_request;

const CASE_TIMEOUT: Duration = Duration::from_secs(10);

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

async fn activated(gates: &AttemptGates) {
    gate(&mut gates.activated.subscribe()).await;
}

async fn terminal(
    service: &qubit_task::TaskExecutionService,
    id: qubit_task::model::typed::TaskId,
) -> qubit_task::model::typed::TaskSummary {
    tokio::time::timeout(CASE_TIMEOUT, async {
        loop {
            if let Some(summary) = service.get(id).await.expect("read task summary")
                && summary.state.is_terminal()
            {
                return summary;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("task reaches a terminal state")
}

async fn shutdown_manager(manager: &AsyncOperationManager) {
    manager
        .request_shutdown(ShutdownMode::Drain)
        .wait()
        .await
        .expect("operation manager drains");
}

#[tokio::test]
async fn cancel_holds_resources_until_cleanup() {
    tokio::time::timeout(CASE_TIMEOUT, async {
        let manager = manager(4);
        let gates = AttemptGates::new();
        let (service, request) = fixture(
            manager.clone(),
            vec![Arc::clone(&gates)],
            NonZeroU32::new(1).unwrap(),
        )
        .await
        .expect("service fixture");
        let accepted = service.submit(request).await.expect("submit managed task");
        activated(&gates).await;
        service
            .cancel(accepted.id)
            .await
            .expect("request cancellation");
        gate(&mut gates.stop_seen.subscribe()).await;
        gate(&mut gates.parent_outcome_seen.subscribe()).await;
        assert_eq!(
            service.get(accepted.id).await.unwrap().unwrap().state,
            TaskState::Running
        );

        let probe = service
            .submit(probe_request())
            .await
            .expect("submit resource probe");
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
    })
    .await
    .expect("cancel/resource scenario completes");
}

#[tokio::test]
async fn drain_waits_for_managed_attempt() {
    tokio::time::timeout(CASE_TIMEOUT, async {
        let manager = manager(4);
        let gates = AttemptGates::new();
        let (service, request) = fixture(
            manager.clone(),
            vec![Arc::clone(&gates)],
            NonZeroU32::new(1).unwrap(),
        )
        .await
        .expect("service fixture");
        let accepted = service.submit(request).await.expect("submit managed task");
        activated(&gates).await;
        let service_for_shutdown = service.clone();
        let shutdown = tokio::spawn(async move { service_for_shutdown.shutdown().await });

        // Shutdown closes task admission before waiting for in-flight work.
        let rejected = service.submit(probe_request()).await;
        assert!(rejected.is_err(), "shutdown must reject new submissions");
        assert!(
            !shutdown.is_finished(),
            "shutdown waits for the active handler"
        );
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
        assert!(
            !shutdown.is_finished(),
            "closed cleanup gate keeps service draining"
        );
        let manager_shutdown = manager.request_shutdown(ShutdownMode::Drain);
        assert!(
            manager_shutdown.snapshot().active_operations > 0,
            "the manager stays active while task service waits for cleanup"
        );

        open(&gates.allow_cleanup);
        gate(&mut gates.cleanup_finished.subscribe()).await;
        shutdown
            .await
            .expect("join service shutdown")
            .expect("service shutdown");
        shutdown_manager(&manager).await;
    })
    .await
    .expect("drain scenario completes");
}

#[tokio::test]
async fn retry_starts_only_after_settlement() {
    tokio::time::timeout(CASE_TIMEOUT, async {
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
        activated(&first).await;
        open(&first.allow_outcome);
        gate(&mut first.parent_outcome_seen.subscribe()).await;
        assert_eq!(service.get(accepted.id).await.unwrap().unwrap().attempt, 1);
        assert!(
            !*second.activated.borrow(),
            "attempt 2 waits for attempt 1 cleanup"
        );

        open(&first.allow_input_release);
        gate(&mut first.input_released.subscribe()).await;
        open(&first.allow_domain_settlement);
        gate(&mut first.domain_settled.subscribe()).await;
        assert_eq!(service.get(accepted.id).await.unwrap().unwrap().attempt, 1);
        assert!(
            !*second.activated.borrow(),
            "settlement alone does not bypass cleanup"
        );
        open(&first.allow_cleanup);
        gate(&mut first.cleanup_finished.subscribe()).await;
        activated(&second).await;
        open(&second.allow_outcome);
        open(&second.allow_input_release);
        open(&second.allow_domain_settlement);
        open(&second.allow_cleanup);
        gate(&mut second.cleanup_finished.subscribe()).await;
        assert_eq!(
            terminal(&service, accepted.id).await.state,
            TaskState::Succeeded
        );
        service.shutdown().await.expect("task service shutdown");
        shutdown_manager(&manager).await;
    })
    .await
    .expect("retry scenario completes");
}

#[tokio::test]
async fn cancel_after_outcome_suppresses_retry() {
    tokio::time::timeout(CASE_TIMEOUT, async {
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
        activated(&first).await;
        open(&first.allow_outcome);
        gate(&mut first.parent_outcome_seen.subscribe()).await;
        service
            .cancel(accepted.id)
            .await
            .expect("persist late cancellation");
        assert!(
            service
                .get(accepted.id)
                .await
                .unwrap()
                .unwrap()
                .cancel_requested
        );
        open(&first.allow_input_release);
        open(&first.allow_domain_settlement);
        gate(&mut first.domain_settled.subscribe()).await;
        open(&first.allow_cleanup);
        gate(&mut first.cleanup_finished.subscribe()).await;
        let failed = terminal(&service, accepted.id).await;
        assert!(matches!(failed.state, TaskState::Failed { .. }));
        assert!(
            !*second.activated.borrow(),
            "late cancellation suppresses retry"
        );
        service.shutdown().await.expect("task service shutdown");
        shutdown_manager(&manager).await;
    })
    .await
    .expect("late cancellation scenario completes");
}

#[tokio::test]
async fn rejected_root_does_not_activate_job() {
    tokio::time::timeout(CASE_TIMEOUT, async {
        let manager = manager(4);
        manager.close_admission();
        let gates = AttemptGates::new();
        let (service, request) = fixture(manager.clone(), vec![Arc::clone(&gates)], NonZeroU32::new(1).unwrap())
            .await
            .expect("service fixture");
        let accepted = service.submit(request).await.expect("submit managed task");
        let failed = terminal(&service, accepted.id).await;
        assert!(matches!(failed.state, TaskState::Failed { ref category, .. } if category == "operation_admission"));
        assert!(!*gates.activated.borrow(), "rejected root does not start external work");
        let probe = service.submit(probe_request()).await.expect("probe retains service capacity");
        assert_eq!(terminal(&service, probe.id).await.state, TaskState::Succeeded);
        service.shutdown().await.expect("task service shutdown");
        shutdown_manager(&manager).await;
    })
    .await
    .expect("root rejection scenario completes");
}

#[tokio::test]
async fn rejected_child_does_not_activate_job() {
    tokio::time::timeout(CASE_TIMEOUT, async {
        // One active slot admits the root but rejects its required child.
        let manager = manager(1);
        let gates = AttemptGates::new();
        open(&gates.allow_cleanup);
        let (service, request) = fixture(manager.clone(), vec![Arc::clone(&gates)], NonZeroU32::new(1).unwrap())
            .await
            .expect("service fixture");
        let accepted = service.submit(request).await.expect("submit managed task");
        let failed = terminal(&service, accepted.id).await;
        assert!(matches!(failed.state, TaskState::Failed { ref category, .. } if category == "operation_admission"));
        assert!(!*gates.activated.borrow(), "rejected child does not activate the job");
        service.shutdown().await.expect("task service shutdown");
        shutdown_manager(&manager).await;
    })
    .await
    .expect("child rejection scenario completes");
}
