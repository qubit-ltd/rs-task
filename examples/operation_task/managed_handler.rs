//! Managed operation orchestration for the controlled `operation_task` demo.
//!
//! If an operation report cannot prove recursive release, this handler keeps
//! its attempt and operation handle alive and waits indefinitely. A timeout is
//! diagnostic only and must never be used to synthesize release evidence. The
//! adapter here is a local controlled job; production providers need durable
//! reconciliation and recovery before using this pattern.

use std::sync::Arc;
use std::sync::Mutex;

use futures::FutureExt;
use qubit_operation::AsyncCleanup;
use qubit_operation::AsyncOperationManager;
use qubit_operation::ChildOutcomeKind;
use qubit_operation::ChildRequirement;
use qubit_operation::CleanupContext;
use qubit_operation::CleanupFuture;
use qubit_operation::CleanupOutcome;
use qubit_operation::OperationOptions;
use qubit_operation::OperationOutcome;
use qubit_operation::PreparedExternalOperation;
use qubit_operation::ResourceScopeOptions;
use qubit_operation::SettlementReport;
use qubit_operation::StopReason;
use qubit_task::handler::TaskContext;
use qubit_task::handler::TaskHandler;
use qubit_task::handler::TaskRunOutcome;
use qubit_task::handler::TaskRunResult;
use qubit_task::model::TaskOutput;
use qubit_task::model::TaskRunError;
use qubit_task::store::TaskFuture;

use crate::ManagedPayload;
use crate::external_job::AttemptGates;
use crate::external_job::ExternalJob;

/// Handler that submits one root operation per typed task attempt.
pub(super) struct ManagedHandler {
    manager: AsyncOperationManager,
    attempts: Arc<Mutex<Vec<Arc<AttemptGates>>>>,
}

impl ManagedHandler {
    pub(super) fn new(manager: AsyncOperationManager, attempts: Arc<Mutex<Vec<Arc<AttemptGates>>>>) -> Self {
        Self { manager, attempts }
    }
}

#[derive(Debug)]
enum ManagedError {
    Admission,
    RetryableBusiness,
    Execution(String),
}

struct CleanupPolicy {
    gates: Arc<AttemptGates>,
}

impl AsyncCleanup<Vec<u8>> for CleanupPolicy {
    fn cleanup(self: Arc<Self>, resource: Vec<u8>, _context: CleanupContext) -> CleanupFuture<Vec<u8>> {
        Box::pin(async move {
            AttemptGates::open(&self.gates.cleanup_started);
            AttemptGates::wait(&self.gates.allow_cleanup).await;
            drop(resource);
            AttemptGates::open(&self.gates.cleanup_finished);
            CleanupOutcome::Released
        })
    }
}

impl TaskHandler<ManagedPayload> for ManagedHandler {
    fn run<'a>(&'a self, _payload: ManagedPayload, context: TaskContext) -> TaskFuture<'a, TaskRunResult> {
        Box::pin(async move {
            let attempt_index = usize::try_from(context.attempt().saturating_sub(1)).unwrap_or(usize::MAX);
            let gates = self
                .attempts
                .lock()
                .expect("attempt gate list poisoned")
                .get(attempt_index)
                .cloned()
                .unwrap_or_else(AttemptGates::new);

            // The cancellation signal is durable for this attempt, including
            // requests made before this future first gets polled.
            if context.wait_cancel_requested().now_or_never().is_some() {
                return Ok(TaskRunOutcome::Cancelled);
            }

            let runtime = tokio::runtime::Handle::current();
            let child_id = Arc::new(Mutex::new(None));
            let child_id_in_root = Arc::clone(&child_id);
            let gates_in_root = Arc::clone(&gates);
            let runtime_in_root = runtime.clone();
            let job_gates = Arc::clone(&gates);
            let job_slot = Arc::clone(&child_id);
            let root = self.manager.try_submit::<Vec<u8>, ManagedError, _, _>(
                OperationOptions::default(),
                move |scope| async move {
                    let resources = scope
                        .resource_scope(ResourceScopeOptions { capacity: 1 })
                        .map_err(|error| ManagedError::Execution(format!("resource scope: {error}")))?;
                    let input = resources
                        .manage_with(
                            vec![0_u8; 32],
                            CleanupPolicy {
                                gates: Arc::clone(&gates_in_root),
                            },
                        )
                        .map_err(|error| ManagedError::Execution(format!("resource registration: {error:?}")))?;
                    let job = ExternalJob::new(runtime_in_root.clone(), input, Arc::clone(&job_gates));
                    let child_manager = scope.manager().as_async();
                    let child_options = OperationOptions {
                        requirement: ChildRequirement::Required,
                        ..OperationOptions::default()
                    };
                    let mut child = match child_manager
                        .try_submit_external::<Vec<u8>, String, _>(child_options, Arc::clone(&job))
                    {
                        Ok(child) => child,
                        Err(error) => {
                            let (reason, unstarted_job) = error.into_parts();
                            eprintln!("operation child admission rejected: {reason}");
                            unstarted_job.abandon();
                            resources.end();
                            return Err(ManagedError::Admission);
                        }
                    };
                    *job_slot.lock().expect("child identity poisoned") = Some(child.id());
                    let child_result = child.wait_outcome().await;
                    // Transfer the consumed receiver to a non-stopping observer:
                    // dropping an active outcome handle would request stop.
                    let _observer = child.into_observer();
                    resources.end();
                    match child_result {
                        Ok(OperationOutcome::Succeeded(output)) => {
                            if *gates_in_root.retryable_failure.borrow() {
                                Err(ManagedError::RetryableBusiness)
                            } else {
                                Ok(output)
                            }
                        }
                        Ok(OperationOutcome::Stopped(_)) => Ok(Vec::new()),
                        Ok(OperationOutcome::Failed(error)) => Err(ManagedError::Execution(error)),
                        Ok(OperationOutcome::ChildFailed(error)) => Err(ManagedError::Execution(format!(
                            "nested child failed: {:?}",
                            error.operation_id
                        ))),
                        Ok(OperationOutcome::Panicked(error)) => {
                            Err(ManagedError::Execution(format!("nested child panicked: {error:?}")))
                        }
                        Ok(OperationOutcome::ExecutionFailed(error)) => Err(ManagedError::Execution(format!(
                            "nested child execution failed: {error:?}"
                        ))),
                        Err(error) => Err(ManagedError::Execution(format!("child outcome observation: {error}"))),
                    }
                },
            );

            let mut root = match root {
                Ok(handle) => handle,
                Err(error) => {
                    eprintln!("operation root admission rejected: {}", error.reason());
                    return Err(task_error(
                        "operation_admission",
                        "root operation was not admitted",
                        false,
                    ));
                }
            };

            // Drop the cancellation future before borrowing the handle mutably
            // to request stop. The biased select preserves a published result
            // when cancellation and the result become ready together.
            let first = {
                tokio::select! {
                    biased;
                    result = root.wait_outcome() => First::Outcome(result),
                    _ = context.wait_cancel_requested() => First::Cancel,
                }
            };
            let (root_result, local_stop_requested) = match first {
                First::Outcome(result) => (result, false),
                First::Cancel => {
                    let _status = root.request_stop(StopReason::UserCancelled);
                    (root.wait_outcome().await, true)
                }
            };
            let snapshot = root.snapshot();
            AttemptGates::open(&gates.parent_outcome_seen);
            let root_operation_id = root.id();
            let settlement = root.wait_settlement().await;
            if !tree_released(&settlement) {
                eprintln!(
                    "operation settlement incomplete: task={:?} attempt={} operation={:?} status={:?}",
                    context.task_id(),
                    context.attempt(),
                    root_operation_id,
                    settlement.status,
                );
                // Keep `root` in this future, retaining the result owner and the
                // attempt while uncertain external ownership is unresolved.
                loop {
                    std::future::pending::<()>().await;
                    let _still_owned = root.snapshot();
                }
            }

            if root_result.is_err() {
                return Err(task_error(
                    "operation_observation",
                    "root operation outcome could not be observed",
                    false,
                ));
            }
            match root_result.expect("checked above") {
                OperationOutcome::Succeeded(output) => Ok(TaskRunOutcome::Succeeded(TaskOutput { summary: output })),
                OperationOutcome::Failed(ManagedError::RetryableBusiness) => Err(task_error(
                    "managed_business",
                    "scenario requested a retryable business failure after child success",
                    true,
                )),
                OperationOutcome::Failed(ManagedError::Admission) => Err(task_error(
                    "operation_admission",
                    "required child operation was not admitted",
                    false,
                )),
                OperationOutcome::Failed(ManagedError::Execution(message)) => {
                    Err(task_error("operation_execution", &message, false))
                }
                OperationOutcome::Stopped(report)
                    if local_stop_requested && report.reason == StopReason::UserCancelled =>
                {
                    Ok(TaskRunOutcome::Cancelled)
                }
                OperationOutcome::ChildFailed(failure)
                    if is_cancelled_child_failure(
                        failure.outcome.kind,
                        local_stop_requested,
                        snapshot.stop.as_ref().map(|stop| stop.reason),
                        *child_id_in_root.lock().expect("child identity poisoned") == Some(failure.operation_id),
                        *gates.stop_seen.borrow(),
                    ) =>
                {
                    Ok(TaskRunOutcome::Cancelled)
                }
                OperationOutcome::Stopped(_) => Err(task_error(
                    "operation_stopped",
                    "operation stopped without a locally bridged user cancellation",
                    false,
                )),
                OperationOutcome::ChildFailed(failure) => Err(task_error(
                    "operation_child_failed",
                    &format!("required child {:?} failed", failure.operation_id),
                    false,
                )),
                OperationOutcome::Panicked(error) => Err(task_error(
                    "operation_panicked",
                    &format!("operation panicked: {error:?}"),
                    false,
                )),
                OperationOutcome::ExecutionFailed(error) => Err(task_error(
                    "operation_execution",
                    &format!("operation execution failed: {error:?}"),
                    false,
                )),
            }
        })
    }
}

fn is_cancelled_child_failure(
    child_kind: ChildOutcomeKind,
    local_stop_requested: bool,
    parent_stop: Option<StopReason>,
    child_identity_matches: bool,
    stop_seen: bool,
) -> bool {
    local_stop_requested
        && parent_stop == Some(StopReason::UserCancelled)
        && child_identity_matches
        && child_kind == ChildOutcomeKind::Stopped
        && stop_seen
}

enum First<T, E> {
    Outcome(Result<OperationOutcome<T, E>, qubit_operation::OutcomeError>),
    Cancel,
}

/// Returns whether every retained level of the report tree proves release.
pub(super) fn tree_released(report: &SettlementReport) -> bool {
    report.all_released()
        && !report.children_truncated
        && report.children.iter().all(|child| tree_released(&child.settlement))
        && report.external_settlement.as_deref().is_none_or(tree_released)
}

fn task_error(category: &str, message: &str, retryable: bool) -> TaskRunError {
    TaskRunError {
        category: category.into(),
        message: message.into(),
        retryable,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use qubit_operation::AsyncOperationManager;
    use qubit_operation::ChildOutcomeKind;
    use qubit_operation::ChildOutcomeSummary;
    use qubit_operation::ChildSettlement;
    use qubit_operation::DiagnosticSummary;
    use qubit_operation::ManagerOptions;
    use qubit_operation::OperationId;
    use qubit_operation::OperationOptions;
    use qubit_operation::SettlementReport;
    use qubit_operation::SettlementStatus;
    use qubit_operation::StopReason;

    use super::is_cancelled_child_failure;
    use super::tree_released;

    fn report(id: OperationId, status: SettlementStatus) -> SettlementReport {
        SettlementReport {
            operation_id: id,
            status,
            released_count: usize::from(status == SettlementStatus::Released),
            transferred_count: usize::from(status == SettlementStatus::Transferred),
            recovery_count: usize::from(status == SettlementStatus::RecoveryRequired),
            children: Arc::from([]),
            children_total: 0,
            children_truncated: false,
            cleanup_failures: Arc::from([]),
            cleanup_failures_total: 0,
            cleanup_failures_truncated: false,
            resource_reports: Arc::from([]),
            resource_reports_total: 0,
            resource_reports_truncated: false,
            transfers: Arc::from([]),
            transfers_total: 0,
            transfers_truncated: false,
            recoveries: Arc::from([]),
            recoveries_total: 0,
            recoveries_truncated: false,
            effects: Arc::from([]),
            effects_total: 0,
            effect_overflow: 0,
            external_settlement: None,
        }
    }

    async fn operation_ids() -> (OperationId, OperationId) {
        let manager = AsyncOperationManager::new(ManagerOptions::default()).expect("manager");
        let mut first = manager
            .try_submit(OperationOptions::default(), |_| async { Ok::<_, ()>(()) })
            .expect("first operation");
        let mut second = manager
            .try_submit(OperationOptions::default(), |_| async { Ok::<_, ()>(()) })
            .expect("second operation");
        let ids = (first.id(), second.id());
        let _ = first.wait_outcome().await.expect("first outcome");
        let _ = second.wait_outcome().await.expect("second outcome");
        let _ = first.wait_settlement().await;
        let _ = second.wait_settlement().await;
        let drain = manager
            .request_shutdown(qubit_operation::ShutdownMode::Drain)
            .wait()
            .await
            .expect("manager drain");
        assert_eq!(drain.snapshot.active_operations, 0);
        assert_eq!(drain.snapshot.queued_operations, 0);
        assert_eq!(drain.snapshot.unconfirmed_users, 0);
        assert_eq!(drain.snapshot.cleaning_resources, 0);
        assert_eq!(drain.snapshot.recovery_groups, 0);
        assert_eq!(drain.snapshot.service_resources, 0);
        ids
    }

    #[tokio::test]
    async fn release_evidence_rejects_truncated_children_and_nested_recovery_or_transfer() {
        let (parent_id, child_id) = operation_ids().await;
        let mut complete = report(parent_id, SettlementStatus::Released);
        assert!(tree_released(&complete));

        complete.children_truncated = true;
        assert!(!tree_released(&complete));

        let mut parent = report(parent_id, SettlementStatus::Released);
        let child = report(child_id, SettlementStatus::RecoveryRequired);
        parent.children = Arc::from([ChildSettlement {
            operation_id: child.operation_id,
            outcome: ChildOutcomeSummary {
                operation_id: child.operation_id,
                kind: ChildOutcomeKind::Succeeded,
                summary: DiagnosticSummary::new("nested report"),
            },
            settlement: Arc::new(child),
        }]);
        parent.children_total = 1;
        assert!(!tree_released(&parent));

        let mut external = report(parent_id, SettlementStatus::Released);
        external.external_settlement = Some(Arc::new(report(child_id, SettlementStatus::Transferred)));
        assert!(!tree_released(&external));
    }

    #[test]
    fn child_stop_is_cancelled_only_when_every_bridge_fact_matches() {
        assert!(is_cancelled_child_failure(
            ChildOutcomeKind::Stopped,
            true,
            Some(StopReason::UserCancelled),
            true,
            true,
        ));
        assert!(!is_cancelled_child_failure(
            ChildOutcomeKind::Stopped,
            false,
            Some(StopReason::UserCancelled),
            true,
            true,
        ));
        assert!(!is_cancelled_child_failure(
            ChildOutcomeKind::Stopped,
            true,
            None,
            true,
            true,
        ));
        assert!(!is_cancelled_child_failure(
            ChildOutcomeKind::Stopped,
            true,
            Some(StopReason::UserCancelled),
            false,
            true,
        ));
        assert!(!is_cancelled_child_failure(
            ChildOutcomeKind::Stopped,
            true,
            Some(StopReason::UserCancelled),
            true,
            false,
        ));
        assert!(!is_cancelled_child_failure(
            ChildOutcomeKind::Failed,
            true,
            Some(StopReason::UserCancelled),
            true,
            true,
        ));
    }
}
