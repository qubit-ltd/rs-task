// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Controlled Tokio work used by the `operation_task` example.
//!
//! The adapter keeps outcome, input release, and domain settlement as separate
//! facts. In particular, publishing an outcome never proves that the input
//! has been released or that domain cleanup has completed.

use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use qubit_operation::ChildOutcomeKind;
use qubit_operation::ChildOutcomeSummary;
use qubit_operation::ChildRelease;
use qubit_operation::DiagnosticSummary;
use qubit_operation::EffectFact;
use qubit_operation::EffectRecord;
use qubit_operation::ExecutionFailure;
use qubit_operation::ExecutionPhase;
use qubit_operation::ExternalBindError;
use qubit_operation::OperationControl;
use qubit_operation::OperationId;
use qubit_operation::OperationOutcome;
use qubit_operation::OperationScope;
use qubit_operation::PreparedExternalOperation;
use qubit_operation::ResourceRef;
use qubit_operation::SettlementReport;
use qubit_operation::SettlementStatus;
use qubit_operation::StopReason;
use qubit_operation::StopReport;
use tokio::runtime::Handle;
use tokio::sync::watch;

/// Independent gates that let example scenarios advance one lifecycle fact.
pub(super) struct AttemptGates {
    pub(super) activated: watch::Sender<bool>,
    pub(super) stop_seen: watch::Sender<bool>,
    pub(super) input_released: watch::Sender<bool>,
    pub(super) domain_settled: watch::Sender<bool>,
    pub(super) allow_outcome: watch::Sender<bool>,
    pub(super) allow_input_release: watch::Sender<bool>,
    pub(super) allow_domain_settlement: watch::Sender<bool>,
    pub(super) allow_cleanup: watch::Sender<bool>,
    pub(super) parent_outcome_seen: watch::Sender<bool>,
    pub(super) cleanup_started: watch::Sender<bool>,
    pub(super) cleanup_finished: watch::Sender<bool>,
    pub(super) retryable_failure: watch::Sender<bool>,
}

impl AttemptGates {
    pub(super) fn new() -> Arc<Self> {
        fn gate() -> watch::Sender<bool> {
            watch::channel(false).0
        }
        Arc::new(Self {
            activated: gate(),
            stop_seen: gate(),
            input_released: gate(),
            domain_settled: gate(),
            allow_outcome: gate(),
            allow_input_release: gate(),
            allow_domain_settlement: gate(),
            allow_cleanup: gate(),
            parent_outcome_seen: gate(),
            cleanup_started: gate(),
            cleanup_finished: gate(),
            retryable_failure: gate(),
        })
    }

    pub(super) fn open(gate: &watch::Sender<bool>) {
        gate.send_replace(true);
    }

    pub(super) async fn wait(gate: &watch::Sender<bool>) {
        let mut receiver = gate.subscribe();
        let _ = receiver.wait_for(|ready| *ready).await;
    }
}

#[derive(Default)]
struct JobState {
    id: Option<OperationId>,
    activated: bool,
    abandoned: bool,
    stop: Option<StopReason>,
    summary: Option<ChildOutcomeSummary>,
    outcome: Option<OperationOutcome<Vec<u8>, String>>,
    input_released: bool,
    settlement: Option<Arc<SettlementReport>>,
    outcome_waker: Option<Waker>,
    summary_waker: Option<Waker>,
    release_waker: Option<Waker>,
    settlement_waker: Option<Waker>,
    input: Option<ResourceRef<Vec<u8>>>,
}

/// Prepared external job whose Tokio execution is started only by `activate`.
pub(super) struct ExternalJob {
    runtime: Handle,
    gates: Arc<AttemptGates>,
    state: Mutex<JobState>,
    stop_tx: watch::Sender<Option<StopReason>>,
}

impl ExternalJob {
    pub(super) fn new(runtime: Handle, input: ResourceRef<Vec<u8>>, gates: Arc<AttemptGates>) -> Arc<Self> {
        let (stop_tx, _) = watch::channel(None);
        Arc::new(Self {
            runtime,
            gates,
            state: Mutex::new(JobState {
                input: Some(input),
                ..JobState::default()
            }),
            stop_tx,
        })
    }

    fn publish_outcome(&self, id: OperationId, outcome: OperationOutcome<Vec<u8>, String>) {
        let (summary, outcome_waker) = {
            let mut state = self.state.lock().expect("external job state poisoned");
            let kind = match &outcome {
                OperationOutcome::Succeeded(_) => ChildOutcomeKind::Succeeded,
                OperationOutcome::Stopped(_) => ChildOutcomeKind::Stopped,
                _ => ChildOutcomeKind::Failed,
            };
            state.summary = Some(ChildOutcomeSummary {
                operation_id: id,
                kind,
                summary: DiagnosticSummary::new("controlled Tokio scan completed"),
            });
            state.outcome = Some(outcome);
            (state.summary_waker.take(), state.outcome_waker.take())
        };
        for waker in [summary, outcome_waker].into_iter().flatten() {
            waker.wake();
        }
    }

    async fn run(
        self: Arc<Self>,
        id: OperationId,
        mut stop_rx: watch::Receiver<Option<StopReason>>,
        input: ResourceRef<Vec<u8>>,
    ) {
        self.gates.activated.send_replace(true);
        let preexisting_stop = *stop_rx.borrow_and_update();
        let stopped = if let Some(reason) = preexisting_stop {
            Some(reason)
        } else {
            tokio::select! {
                biased;
                changed = stop_rx.changed() => {
                    if changed.is_ok() { *stop_rx.borrow_and_update() } else { None }
                }
                _ = AttemptGates::wait(&self.gates.allow_outcome) => None,
            }
        };

        let outcome = if let Some(reason) = stopped {
            self.gates.stop_seen.send_replace(true);
            OperationOutcome::Stopped(StopReport {
                reason,
                accepted_at: None,
            })
        } else {
            OperationOutcome::Succeeded(input.to_vec())
        };
        self.publish_outcome(id, outcome);

        AttemptGates::wait(&self.gates.allow_input_release).await;
        drop(input);
        let waker = {
            let mut state = self.state.lock().expect("external job state poisoned");
            state.input_released = true;
            state.release_waker.take()
        };
        self.gates.input_released.send_replace(true);
        if let Some(waker) = waker {
            waker.wake();
        }

        AttemptGates::wait(&self.gates.allow_domain_settlement).await;
        let report = Arc::new(SettlementReport {
            operation_id: id,
            status: SettlementStatus::Released,
            released_count: 0,
            transferred_count: 0,
            recovery_count: 0,
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
            effects: Arc::from([EffectRecord {
                operation_id: id,
                resource_id: None,
                fact: EffectFact::Known,
                description: DiagnosticSummary::new("controlled scan settled"),
            }]),
            effects_total: 1,
            effect_overflow: 0,
            external_settlement: None,
        });
        let waker = {
            let mut state = self.state.lock().expect("external job state poisoned");
            state.settlement = Some(report);
            state.settlement_waker.take()
        };
        self.gates.domain_settled.send_replace(true);
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

impl OperationControl for ExternalJob {
    fn request_stop(&self, reason: StopReason) {
        let first = {
            let mut state = self.state.lock().expect("external job state poisoned");
            if state.stop.is_none() {
                state.stop = Some(reason);
            }
            state.stop
        };
        self.stop_tx.send_replace(first);
    }

    fn snapshot(&self) -> qubit_operation::ChildSnapshot {
        let state = self.state.lock().expect("external job state poisoned");
        qubit_operation::ChildSnapshot {
            operation_id: state.id.expect("manager snapshots only bound jobs"),
            execution: if state.input_released {
                ExecutionPhase::Finished
            } else if state.activated {
                ExecutionPhase::Running
            } else {
                ExecutionPhase::Queued
            },
            stop: state.stop.map(|reason| StopReport {
                reason,
                accepted_at: None,
            }),
            outcome: state.summary.clone(),
            input_released: state.input_released,
            settlement: state.settlement.clone(),
        }
    }

    fn poll_outcome(&self, cx: &mut Context<'_>) -> Poll<ChildOutcomeSummary> {
        let mut state = self.state.lock().expect("external job state poisoned");
        if let Some(summary) = state.summary.clone() {
            Poll::Ready(summary)
        } else {
            state.summary_waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }

    fn poll_release(&self, cx: &mut Context<'_>) -> Poll<ChildRelease> {
        let mut state = self.state.lock().expect("external job state poisoned");
        if state.input_released {
            Poll::Ready(ChildRelease {
                operation_id: state.id.expect("bound"),
                confirmed_at: None,
            })
        } else {
            state.release_waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }

    fn poll_settlement(&self, cx: &mut Context<'_>) -> Poll<Arc<SettlementReport>> {
        let mut state = self.state.lock().expect("external job state poisoned");
        if let Some(report) = state.settlement.clone() {
            Poll::Ready(report)
        } else {
            state.settlement_waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

impl PreparedExternalOperation<Vec<u8>, String> for ExternalJob {
    fn bind(&self, id: OperationId) -> Result<(), ExternalBindError> {
        let mut state = self.state.lock().expect("external job state poisoned");
        if state.id.is_some() {
            return Err(ExternalBindError::AlreadyBound);
        }
        state.id = Some(id);
        Ok(())
    }

    fn activate(self: Arc<Self>, _scope: OperationScope) -> Result<(), ExecutionFailure> {
        let (id, input) = {
            let mut state = self.state.lock().expect("external job state poisoned");
            let id = state.id.expect("manager binds before activation");
            state.activated = true;
            let input = state.input.take().expect("input is claimed once on activation");
            (id, input)
        };
        let stop_rx = self.stop_tx.subscribe();
        self.runtime.spawn(Arc::clone(&self).run(id, stop_rx, input));
        Ok(())
    }

    fn abandon(self: Arc<Self>) {
        let input = {
            let mut state = self.state.lock().expect("external job state poisoned");
            state.abandoned = true;
            state.input.take()
        };
        // Publish release only once the unstarted input is genuinely returned.
        drop(input);
        let waker = {
            let mut state = self.state.lock().expect("external job state poisoned");
            state.input_released = true;
            state.release_waker.take()
        };
        self.gates.input_released.send_replace(true);
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    fn poll_take_outcome(&self, cx: &mut Context<'_>) -> Poll<OperationOutcome<Vec<u8>, String>> {
        let mut state = self.state.lock().expect("external job state poisoned");
        if let Some(outcome) = state.outcome.take() {
            Poll::Ready(outcome)
        } else {
            state.outcome_waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Wake;
    use std::task::Waker;

    use qubit_operation::ManagerOptions;
    use qubit_operation::OperationControl;
    use qubit_operation::OperationManager;
    use qubit_operation::OperationOptions;
    use qubit_operation::OperationOutcome;
    use qubit_operation::PreparedExternalOperation;
    use qubit_operation::StopReason;
    use tokio::sync::oneshot;

    use super::AttemptGates;
    use super::ExternalJob;

    struct NoopWake;
    impl Wake for NoopWake {
        fn wake(self: Arc<Self>) {}
    }

    fn context() -> Context<'static> {
        let waker = Box::leak(Box::new(Waker::from(Arc::new(NoopWake))));
        Context::from_waker(waker)
    }

    #[tokio::test]
    async fn test_early_stop_and_each_lifecycle_fact_are_independent() {
        let manager = OperationManager::new(ManagerOptions::default()).expect("manager");
        let (tx, rx) = oneshot::channel();
        let runtime = tokio::runtime::Handle::current();
        let mut root = manager
            .as_async()
            .try_submit::<(), String, _, _>(OperationOptions::default(), move |scope| async move {
                let input = scope.manage(vec![1_u8, 2, 3]).expect("input resource");
                let gates = AttemptGates::new();
                let job = ExternalJob::new(runtime, input, Arc::clone(&gates));
                job.bind(scope.id()).expect("first bind");
                assert!(job.bind(scope.id()).is_err(), "second claim is rejected");
                job.request_stop(StopReason::UserCancelled);
                job.clone().activate(scope).expect("activation");
                assert!(tx.send((Arc::clone(&job), gates)).is_ok());
                Ok(())
            })
            .expect("root admission");

        let (job, gates) = rx.await.expect("adapter published");
        AttemptGates::wait(&gates.stop_seen).await;
        let mut cx = context();
        assert!(matches!(job.poll_release(&mut cx), Poll::Pending));
        assert!(matches!(job.poll_settlement(&mut cx), Poll::Pending));
        let outcome = std::future::poll_fn(|cx| job.poll_take_outcome(cx)).await;
        assert!(matches!(outcome, OperationOutcome::Stopped(_)));
        assert!(
            matches!(job.poll_take_outcome(&mut cx), Poll::Pending),
            "typed outcome is one-shot"
        );

        AttemptGates::open(&gates.allow_input_release);
        AttemptGates::wait(&gates.input_released).await;
        assert!(matches!(job.poll_release(&mut cx), Poll::Ready(_)));
        assert!(matches!(job.poll_settlement(&mut cx), Poll::Pending));
        AttemptGates::open(&gates.allow_domain_settlement);
        AttemptGates::wait(&gates.domain_settled).await;
        assert!(matches!(job.poll_settlement(&mut cx), Poll::Ready(_)));

        drop(job);
        assert!(matches!(
            root.wait_outcome().await.expect("root outcome"),
            OperationOutcome::Succeeded(())
        ));
        let report = root.wait_settlement().await;
        assert!(report.all_released());
    }

    #[tokio::test]
    async fn test_abandoning_a_bound_job_returns_input_without_starting_it() {
        let manager = OperationManager::new(ManagerOptions::default()).expect("manager");
        let (tx, rx) = oneshot::channel();
        let runtime = tokio::runtime::Handle::current();
        let mut root = manager
            .as_async()
            .try_submit::<(), String, _, _>(OperationOptions::default(), move |scope| async move {
                let input = scope.manage(vec![7_u8]).expect("input resource");
                let gates = AttemptGates::new();
                let job = ExternalJob::new(runtime, input, Arc::clone(&gates));
                job.bind(scope.id()).expect("bind");
                job.clone().abandon();
                assert!(tx.send(gates).is_ok());
                Ok(())
            })
            .expect("root admission");

        let gates = rx.await.expect("abandon published");
        AttemptGates::wait(&gates.input_released).await;
        assert!(!*gates.activated.borrow(), "abandon must not start a Tokio job");
        assert!(matches!(
            root.wait_outcome().await.expect("root outcome"),
            OperationOutcome::Succeeded(())
        ));
        assert!(root.wait_settlement().await.all_released());
    }
}
