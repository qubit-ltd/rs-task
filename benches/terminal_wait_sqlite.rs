// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Matched SQLite checkpoint-polling comparison for terminal-wait intervals.

use std::collections::HashMap;
use std::future::Future;
use std::num::NonZeroUsize;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use qubit_codec::ValueBytesCodecDescriptor;
use qubit_codec::ValueBytesCodecRegistration;
use qubit_codec::ValueBytesCodecRegistry;
use qubit_codec::ValueCodecId;
use qubit_codec::ValueCodecRegistrationSource;
use qubit_codec::ValueDecoder;
use qubit_codec::ValueEncoder;
use qubit_model_id::HasModelId;
use qubit_model_id::ModelId;
use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::TaskHandlerDescriptor;
use qubit_task::TaskId;
use qubit_task::TaskKind;
use qubit_task::handler::TaskRunOutcome;
use qubit_task::model::TransitionCommand;
use qubit_task::model::typed::TaskStateCheckpoint;
use qubit_task::service::WaitTaskError;
use qubit_task::store::SqliteTaskStore;
use qubit_task::store::StoreError;
use qubit_task::store::TaskFuture;
use qubit_task::store::TaskStore;
use qubit_task::store::delegate_task_store;
use rusqlite::Connection;
use tokio::runtime::Builder;
use tokio::sync::Barrier;
use tokio::task::JoinSet;

const ROUNDS: usize = 5;
const COMMIT_DELAY: Duration = Duration::from_millis(80);
const TIMEOUT: Duration = Duration::from_millis(400);
const PROBE_INTERVAL: Duration = Duration::from_millis(1);
const OBSERVATION_WINDOW: Duration = Duration::from_secs(2);
const SUBMISSION_DRAIN: Duration = Duration::from_secs(5);

#[derive(Clone, Copy)]
enum Strategy {
    Fixed,
    Backoff,
}

impl Strategy {
    fn name(self) -> &'static str {
        match self {
            Self::Fixed => "fixed-25ms",
            Self::Backoff => "current-backoff",
        }
    }

    fn first_interval(self) -> Duration {
        match self {
            Self::Fixed | Self::Backoff => Duration::from_millis(25),
        }
    }

    fn next_interval(self, current: Duration) -> Duration {
        match self {
            Self::Fixed => Duration::from_millis(25),
            Self::Backoff => current.saturating_mul(2).min(Duration::from_millis(100)),
        }
    }
}

#[derive(Default)]
struct Outcome {
    startup_us: Vec<u128>,
    observation_us: Vec<u128>,
    checkpoint_reads: usize,
    timeouts: usize,
    errors: usize,
    successes: usize,
    overlapping_commit: usize,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::args().any(|arg| arg == "--self-test-probe-launch") {
        let runtime = Builder::new_current_thread().enable_all().build()?;
        runtime.block_on(async {
            let (second_started, observe_second) = tokio::sync::oneshot::channel::<Instant>();
            let gate = Arc::new(tokio::sync::Semaphore::new(0));
            let release_gate = Arc::clone(&gate);
            let second_started = Arc::new(Mutex::new(Some(second_started)));
            let launch = Arc::new(move |sequence, _planned_at, _launched_at| {
                let gate = Arc::clone(&gate);
                let second_started = Arc::clone(&second_started);
                async move {
                    if sequence == 1 {
                        if let Ok(permit) = gate.acquire().await {
                            permit.forget();
                        }
                    } else if let Some(sender) = second_started.lock().expect("self-test sender lock").take() {
                        let _ = sender.send(Instant::now());
                    }
                    false
                }
            });
            let start = Instant::now();
            let (plans, mut tasks) = launch_probe_schedule(
                3,
                start,
                Duration::from_millis(20),
                start + Duration::from_secs(1),
                launch,
            )
            .await;
            let second_at = tokio::time::timeout(Duration::from_millis(100), observe_second).await??;
            if second_at.duration_since(start) >= Duration::from_millis(120) {
                return Err("later due probe was delayed while the first probe remained pending".into());
            }
            release_gate.add_permits(1);
            while tasks.join_next().await.is_some() {}
            if plans.len() != 3 {
                return Err("probe schedule did not launch all planned probes".into());
            }
            println!("probe launch self-test passed: later due probe started while first probe remained pending");
            Ok::<(), Box<dyn std::error::Error>>(())
        })?;
        return Ok(());
    }
    if std::env::args().any(|arg| arg == "--self-test-open-failure") {
        let invalid_path = std::env::temp_dir().join(format!(
            "qubit-task-missing-parent-{}/benchmark.sqlite",
            std::process::id()
        ));
        for participant in ["reader-0", "writer"] {
            let error = open_participant(&invalid_path, 7, Strategy::Fixed, participant)
                .expect_err("opening beneath a missing directory must fail");
            if !error.contains("round=7 strategy=fixed-25ms") || !error.contains(participant) {
                return Err(format!("open failure lacks participant context: {error}").into());
            }
        }
        println!("open failure self-test passed: reader and writer errors include context");
        return Ok(());
    }
    if std::env::args().any(|arg| arg == "--self-test-terminal-samples") {
        let commit_start = Instant::now();
        let commit_end = commit_start + Duration::from_millis(2);
        let mut observed = vec![commit_start + Duration::from_millis(1)];
        observed.extend((0..639).map(|_| commit_end + Duration::from_millis(1)));
        let (effective, overlapping) = classify_terminal_samples(&observed, Some((commit_start, commit_end)))?;
        if effective.len() != 639 || overlapping != 1 {
            return Err("639 effective samples plus one overlap did not reconcile to 640".into());
        }
        if !classify_terminal_samples(&[], None)?.0.is_empty() {
            return Err("empty observations must produce zero effective samples".into());
        }
        if classify_terminal_samples(&[commit_end], None).is_ok() {
            return Err("successful observations without a commit interval must fail".into());
        }
        println!(
            "terminal sample self-test passed: 639 effective + 1 overlap = 640; zero and missing interval cases checked"
        );
        return Ok(());
    }
    if std::env::args().any(|arg| arg == "--self-test-probe-timing") {
        self_test_probe_timing()?;
        println!("probe timing self-test passed: late submit, cutoff equality, overlap, and accounting checked");
        return Ok(());
    }
    let runtime = Builder::new_multi_thread().worker_threads(2).enable_all().build()?;
    let temp = TemporaryDirectory::new()?;
    println!(
        "sqlite={} rounds={ROUNDS} workers=2 commit_delay_ms=80 timeout_ms=400",
        rusqlite::version()
    );
    for round in 0..ROUNDS {
        for waiter_count in [0, 32, 128, 512] {
            let strategies = if round % 2 == 0 {
                [Strategy::Fixed, Strategy::Backoff]
            } else {
                [Strategy::Backoff, Strategy::Fixed]
            };
            for strategy in strategies {
                let outcome = runtime.block_on(run_round(&temp, round, waiter_count, strategy))?;
                println!(
                    "round={round} waiters={waiter_count} strategy={} checkpoint_reads={} success={} timeout={} errors={} overlapping_commit={} observed_samples={} startup_p95_us={} observation_p50_us={} observation_p95_us={}",
                    strategy.name(),
                    outcome.checkpoint_reads,
                    outcome.successes,
                    outcome.timeouts,
                    outcome.errors,
                    outcome.overlapping_commit,
                    outcome.observation_us.len(),
                    percentile(&outcome.startup_us, 95),
                    percentile(&outcome.observation_us, 50),
                    percentile(&outcome.observation_us, 95),
                );
            }
        }
    }
    println!("real_store workload (5 rounds; target=80ms, probes=waiter count, runtime workers=2)");
    for waiters in [0, 32, 128, 512] {
        let mut samples = [Vec::new(), Vec::new()];
        let mut target_startup = [Vec::new(), Vec::new()];
        let mut probe_startup = [Vec::new(), Vec::new()];
        let mut reads = [0_usize; 2];
        let mut read_returns = [0_usize; 2];
        let mut read_errors = [0_usize; 2];
        let mut timeouts = [0_usize; 2];
        let mut errors = [0_usize; 2];
        let mut overlaps = [0_usize; 2];
        let mut probe_totals = [ProbeMetrics::default(), ProbeMetrics::default()];
        for round in 0..ROUNDS {
            let order = if round % 2 == 0 { [0, 1] } else { [1, 0] };
            for strategy_index in order {
                let backoff_first = strategy_index == 1;
                let (latency, startup, count, returned, read_error, timeout, error, overlap, probes) =
                    runtime.block_on(run_real_store_round(&temp, round, waiters, backoff_first))?;
                samples[strategy_index].extend(latency);
                for (probe, duration) in startup {
                    if probe {
                        probe_startup[strategy_index].push(duration);
                    } else {
                        target_startup[strategy_index].push(duration);
                    }
                }
                reads[strategy_index] += count;
                read_returns[strategy_index] += returned;
                read_errors[strategy_index] += read_error;
                timeouts[strategy_index] += timeout;
                errors[strategy_index] += error;
                overlaps[strategy_index] += overlap;
                probe_totals[strategy_index].merge(probes);
            }
        }
        for strategy_index in 0..2 {
            println!(
                "real_store waiters={waiters} strategy={} checkpoint_requests_started={reads} checkpoint_requests_returned={read_requests_returned} checkpoint_read_errors={read_errors} successful_observations={successes} effective_latency_samples={observations} timeout={timeouts} errors={errors} overlapping_commit={overlaps} startup_p95_us={startup_p95} probe_startup_p95_us={probe_startup_p95} observation_p95_us={observation_p95} probes_planned={probes_planned} probes_not_dispatched={not_dispatched} probes_launched={probes_launched} probes_submitted={probes_submitted} probes_not_submitted={not_submitted} probes_accepted_in_window={probes_accepted} accepted_outside_window={accepted_outside_window} admission_window_overlap={admission_overlap} submit_failed={submit_failed} admission_unresolved={admission_unresolved} handler_started_in_window={handler_started_in_window} handler_not_started_in_window={handler_not_started_in_window} dispatch_lag_p95_us={launch_lag_p95_us} submit_lag_p95_us={submit_lag_p95_us} submit_to_store_accept_return_p95_us={admission_p95_us} submit_to_public_reply_p95_us={submit_reply_p95_us} submit_to_handler_start_p95_us={admission_to_handler_p95_us} admission_handler_overlap={admission_handler_overlap}",
                if strategy_index == 1 {
                    "current-backoff"
                } else {
                    "fixed-25ms"
                },
                startup_p95 = percentile(&target_startup[strategy_index], 95),
                probe_startup_p95 = percentile(&probe_startup[strategy_index], 95),
                observation_p95 = percentile(&samples[strategy_index], 95),
                reads = reads[strategy_index],
                read_requests_returned = read_returns[strategy_index],
                read_errors = read_errors[strategy_index],
                successes = samples[strategy_index].len() + overlaps[strategy_index],
                observations = samples[strategy_index].len(),
                timeouts = timeouts[strategy_index],
                errors = errors[strategy_index],
                overlaps = overlaps[strategy_index],
                probes_planned = probe_totals[strategy_index].planned,
                probes_launched = probe_totals[strategy_index].launched,
                not_dispatched = probe_totals[strategy_index]
                    .planned
                    .saturating_sub(probe_totals[strategy_index].launched),
                probes_submitted = probe_totals[strategy_index].submitted,
                not_submitted = probe_totals[strategy_index].not_submitted,
                probes_accepted = probe_totals[strategy_index].accepted,
                accepted_outside_window = probe_totals[strategy_index].accepted_outside,
                admission_overlap = probe_totals[strategy_index].admission_overlap,
                submit_failed = probe_totals[strategy_index].failed,
                admission_unresolved = probe_totals[strategy_index].unresolved,
                handler_started_in_window = probe_totals[strategy_index].started_in_window,
                handler_not_started_in_window = probe_totals[strategy_index].not_started_in_window,
                launch_lag_p95_us = percentile(&probe_totals[strategy_index].launch_lag_us, 95),
                submit_lag_p95_us = percentile(&probe_totals[strategy_index].submit_lag_us, 95),
                admission_p95_us = percentile(&probe_totals[strategy_index].admission_us, 95),
                submit_reply_p95_us = percentile(&probe_totals[strategy_index].submit_reply_us, 95),
                admission_to_handler_p95_us = percentile(&probe_totals[strategy_index].admission_handler_us, 95),
                admission_handler_overlap = probe_totals[strategy_index].handler_admission_overlap,
            );
        }
    }
    Ok(())
}

struct ProbePlan {
    sequence: u32,
    planned_at: Instant,
    launched_at: Instant,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum AdmissionClass {
    ConfirmedInWindow,
    ConfirmedOutside,
    Overlap,
    Failed,
}

fn is_before_window_end(at: Instant, window_end: Instant) -> bool {
    at < window_end
}

fn classify_admission(started: Instant, returned: Instant, succeeded: bool, window_end: Instant) -> AdmissionClass {
    if !succeeded {
        AdmissionClass::Failed
    } else if returned < window_end {
        AdmissionClass::ConfirmedInWindow
    } else if started >= window_end {
        AdmissionClass::ConfirmedOutside
    } else {
        AdmissionClass::Overlap
    }
}

fn self_test_probe_timing() -> Result<(), Box<dyn std::error::Error>> {
    let cutoff = Instant::now();
    let before = cutoff - Duration::from_micros(1);
    let after = cutoff + Duration::from_micros(1);
    if is_before_window_end(cutoff, cutoff)
        || is_before_window_end(after, cutoff)
        || classify_admission(before, before, true, cutoff) != AdmissionClass::ConfirmedInWindow
        || classify_admission(after, after, true, cutoff) != AdmissionClass::ConfirmedOutside
        || classify_admission(before, cutoff, true, cutoff) != AdmissionClass::Overlap
        || classify_admission(before, after, true, cutoff) != AdmissionClass::Overlap
        || classify_admission(before, after, false, cutoff) != AdmissionClass::Failed
    {
        return Err("probe admission classifications mishandled the half-open cutoff".into());
    }
    let metrics = ProbeMetrics {
        planned: 5,
        launched: 4,
        not_submitted: 1,
        submitted: 3,
        accepted: 1,
        accepted_outside: 1,
        admission_overlap: 1,
        unresolved: 0,
        started_in_window: 1,
        not_started_in_window: 0,
        ..ProbeMetrics::default()
    };
    metrics.validate()?;
    let mut aggregate = ProbeMetrics::default();
    aggregate.merge(metrics);
    if aggregate.not_submitted != 1 {
        return Err("probe aggregation lost not-submitted probes".into());
    }
    aggregate.validate()?;
    let submit_started = cutoff - Duration::from_millis(4);
    let store_started = cutoff - Duration::from_millis(2);
    let store_returned = cutoff - Duration::from_millis(1);
    if store_returned.duration_since(submit_started) <= store_returned.duration_since(store_started) {
        return Err("submit-to-return duration must include time before the store call".into());
    }
    Ok(())
}

fn classify_terminal_samples(
    observed: &[Instant],
    commit: Option<(Instant, Instant)>,
) -> Result<(Vec<u128>, usize), &'static str> {
    if observed.is_empty() {
        return Ok((Vec::new(), 0));
    }
    let (_before, after) = commit.ok_or("successful terminal observations have no commit interval")?;
    let mut effective = Vec::with_capacity(observed.len());
    let mut overlapping = 0;
    for observed_at in observed {
        if *observed_at < after {
            overlapping += 1;
        } else {
            effective.push(observed_at.duration_since(after).as_micros());
        }
    }
    Ok((effective, overlapping))
}

async fn launch_probe_schedule<F, Fut>(
    count: usize,
    start: Instant,
    interval: Duration,
    window_end: Instant,
    launch: Arc<F>,
) -> (Vec<ProbePlan>, JoinSet<(u32, bool)>)
where
    F: Fn(u32, Instant, Instant) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = bool> + Send + 'static,
{
    let mut plans = Vec::with_capacity(count);
    let mut tasks = JoinSet::new();
    for index in 0..count {
        let sequence = u32::try_from(index + 1).expect("probe count fits sequence");
        let planned_at = start + interval.saturating_mul(index as u32);
        tokio::time::sleep_until(tokio::time::Instant::from_std(planned_at)).await;
        let launched_at = Instant::now();
        if launched_at >= window_end {
            break;
        }
        let future = launch(sequence, planned_at, launched_at);
        tasks.spawn(async move { (sequence, future.await) });
        plans.push(ProbePlan {
            sequence,
            planned_at,
            launched_at,
        });
    }
    (plans, tasks)
}

#[derive(Default)]
struct ProbeMetrics {
    planned: usize,
    launched: usize,
    submitted: usize,
    accepted: usize,
    accepted_outside: usize,
    admission_overlap: usize,
    not_submitted: usize,
    failed: usize,
    unresolved: usize,
    started_in_window: usize,
    not_started_in_window: usize,
    handler_admission_overlap: usize,
    launch_lag_us: Vec<u128>,
    submit_lag_us: Vec<u128>,
    admission_us: Vec<u128>,
    submit_reply_us: Vec<u128>,
    admission_handler_us: Vec<u128>,
}

impl ProbeMetrics {
    fn validate(&self) -> Result<(), &'static str> {
        if self.launched > self.planned
            || self.launched != self.not_submitted + self.submitted
            || self.submitted
                != self.accepted + self.accepted_outside + self.admission_overlap + self.failed + self.unresolved
            || self.started_in_window + self.not_started_in_window != self.accepted
        {
            return Err("probe counts do not reconcile");
        }
        Ok(())
    }

    fn merge(&mut self, mut other: Self) {
        self.planned += other.planned;
        self.launched += other.launched;
        self.not_submitted += other.not_submitted;
        self.accepted += other.accepted;
        self.submitted += other.submitted;
        self.accepted_outside += other.accepted_outside;
        self.admission_overlap += other.admission_overlap;
        self.failed += other.failed;
        self.unresolved += other.unresolved;
        self.started_in_window += other.started_in_window;
        self.not_started_in_window += other.not_started_in_window;
        self.handler_admission_overlap += other.handler_admission_overlap;
        self.launch_lag_us.append(&mut other.launch_lag_us);
        self.submit_lag_us.append(&mut other.submit_lag_us);
        self.admission_us.append(&mut other.admission_us);
        self.submit_reply_us.append(&mut other.submit_reply_us);
        self.admission_handler_us.append(&mut other.admission_handler_us);
    }
}

#[derive(Clone)]
struct BenchmarkInput {
    sequence: u32,
    duration_ms: u32,
}
impl HasModelId for BenchmarkInput {
    const MODEL_ID: ModelId = ModelId::new("qubit.task.BenchmarkInput");
}
#[derive(Default)]
struct BenchmarkCodec;
impl ValueEncoder<BenchmarkInput> for BenchmarkCodec {
    type Output = Vec<u8>;
    type Error = std::convert::Infallible;
    fn encode(&mut self, value: &BenchmarkInput) -> Result<Vec<u8>, Self::Error> {
        let mut bytes = value.sequence.to_le_bytes().to_vec();
        bytes.extend_from_slice(&value.duration_ms.to_le_bytes());
        Ok(bytes)
    }
}
impl ValueDecoder<[u8]> for BenchmarkCodec {
    type Output = BenchmarkInput;
    type Error = std::io::Error;
    fn decode(&mut self, bytes: &[u8]) -> Result<BenchmarkInput, Self::Error> {
        let sequence = u32::from_le_bytes(
            bytes
                .get(..4)
                .ok_or_else(|| std::io::Error::other("short benchmark payload"))?
                .try_into()
                .unwrap(),
        );
        let duration_ms = u32::from_le_bytes(
            bytes
                .get(4..8)
                .ok_or_else(|| std::io::Error::other("short benchmark payload"))?
                .try_into()
                .unwrap(),
        );
        Ok(BenchmarkInput { sequence, duration_ms })
    }
}
static BENCHMARK_CODEC: ValueBytesCodecDescriptor = ValueBytesCodecDescriptor::of::<BenchmarkCodec, BenchmarkInput>();
static BENCHMARK_CODEC_REGISTRATION: ValueBytesCodecRegistration = ValueBytesCodecRegistration::new(
    ValueCodecId::new("example.codec"),
    &BENCHMARK_CODEC,
    ValueCodecRegistrationSource::new(
        "qubit-task",
        "terminal_wait_sqlite",
        "benches/terminal_wait_sqlite.rs",
        1,
    ),
);

type AcceptInterval = (Instant, Instant, bool);
type AcceptIntervals = Arc<Mutex<HashMap<u32, AcceptInterval>>>;

struct CountingStore {
    inner: Arc<SqliteTaskStore>,
    target: Mutex<Option<TaskId>>,
    read_started: std::sync::atomic::AtomicUsize,
    read_returned: std::sync::atomic::AtomicUsize,
    read_errors: std::sync::atomic::AtomicUsize,
    accept_intervals: AcceptIntervals,
    commit: Mutex<Option<(Instant, Instant)>>,
}
impl TaskStore for CountingStore {
    delegate_task_store!(inner; except [accept_encoded, get_state_checkpoint, transition_encoded]);

    fn accept_encoded<'a>(
        &'a self,
        id: TaskId,
        request: qubit_task::model::typed::StoredTaskRequest,
    ) -> TaskFuture<'a, Result<qubit_task::model::typed::AcceptOutcome, StoreError>> {
        let sequence = BenchmarkCodec
            .decode(&request.payload.bytes)
            .map(|input| input.sequence)
            .unwrap_or(0);
        let intervals = Arc::clone(&self.accept_intervals);
        let future = self.inner.accept_encoded(id, request);
        Box::pin(async move {
            let before = Instant::now();
            let result = future.await;
            let after = Instant::now();
            if sequence != 0 {
                intervals
                    .lock()
                    .expect("accept interval lock")
                    .insert(sequence, (before, after, result.is_ok()));
            }
            result
        })
    }

    fn get_state_checkpoint<'a>(
        &'a self,
        id: TaskId,
    ) -> TaskFuture<'a, Result<Option<TaskStateCheckpoint>, StoreError>> {
        let future = self.inner.get_state_checkpoint(id);
        Box::pin(async move {
            let tracked = self.target.lock().expect("target lock").as_ref() == Some(&id);
            if tracked {
                self.read_started.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            let result = future.await;
            if tracked {
                self.read_returned.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if result.is_err() {
                    self.read_errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
            result
        })
    }

    fn transition_encoded<'a>(
        &'a self,
        command: TransitionCommand,
    ) -> TaskFuture<'a, Result<qubit_task::model::typed::TaskSummary, StoreError>> {
        let is_target_terminal =
            self.target.lock().expect("target lock").as_ref() == Some(&command.id) && command.state.is_terminal();
        let future = self.inner.transition_encoded(command);
        Box::pin(async move {
            if is_target_terminal {
                let before = Instant::now();
                let result = future.await;
                let after = Instant::now();
                if result.is_ok() {
                    *self.commit.lock().expect("commit lock") = Some((before, after));
                }
                result
            } else {
                future.await
            }
        })
    }
}

async fn run_real_store_round(
    temp: &TemporaryDirectory,
    round: usize,
    waiters: usize,
    use_backoff: bool,
) -> Result<
    (
        Vec<u128>,
        Vec<(bool, u128)>,
        usize,
        usize,
        usize,
        usize,
        usize,
        usize,
        ProbeMetrics,
    ),
    Box<dyn std::error::Error>,
> {
    let path = temp.join(format!("real-{round}-{waiters}-{use_backoff}.sqlite"));
    let inner = Arc::new(SqliteTaskStore::open(&path)?);
    let store = Arc::new(CountingStore {
        inner,
        target: Mutex::new(None),
        read_started: std::sync::atomic::AtomicUsize::new(0),
        read_returned: std::sync::atomic::AtomicUsize::new(0),
        read_errors: std::sync::atomic::AtomicUsize::new(0),
        accept_intervals: Arc::new(Mutex::new(HashMap::new())),
        commit: Mutex::new(None),
    });
    let submitted_at = Arc::new(Mutex::new(HashMap::<u32, Instant>::new()));
    let handler_at = Arc::new(Mutex::new(HashMap::<u32, Instant>::new()));
    let handler_startup = Arc::new(Mutex::new(Vec::<(bool, u128)>::new()));
    let codecs = Arc::new(ValueBytesCodecRegistry::from_registrations([
        &BENCHMARK_CODEC_REGISTRATION,
    ])?);
    let ids = Arc::new(qubit_id::SnowflakeGenerator::new(0)?);
    let mut builder = TaskExecutionServiceBuilder::new(store.clone(), codecs, ids)
        .max_running_tasks(NonZeroUsize::new(2).expect("two workers is non-zero"));
    let handler_times = Arc::clone(&handler_startup);
    let submissions = Arc::clone(&submitted_at);
    let handlers = Arc::clone(&handler_at);
    builder.register_handler_fn::<BenchmarkInput, _, _>(
        TaskHandlerDescriptor::for_payload::<BenchmarkInput>("terminal-wait-bench", 1),
        move |input, _context| {
            let handler_times = Arc::clone(&handler_times);
            let submissions = Arc::clone(&submissions);
            let handlers = Arc::clone(&handlers);
            async move {
                let started = Instant::now();
                if let Some(submitted) = submissions.lock().expect("submission lock").get(&input.sequence) {
                    handler_times
                        .lock()
                        .expect("handler times lock")
                        .push((input.sequence != 0, started.duration_since(*submitted).as_micros()));
                }
                handlers
                    .lock()
                    .expect("handler timestamps lock")
                    .insert(input.sequence, started);
                tokio::time::sleep(Duration::from_millis(u64::from(input.duration_ms))).await;
                Ok(TaskRunOutcome::success())
            }
        },
    )?;
    let service = builder.build().await?;
    submitted_at.lock().expect("submission lock").insert(0, Instant::now());
    let target = service
        .submit(
            TaskKind::<BenchmarkInput>::new("terminal-wait-bench", 1, ValueCodecId::new("example.codec")).request(
                BenchmarkInput {
                    sequence: 0,
                    duration_ms: 80,
                },
            ),
        )
        .await?;
    *store.target.lock().expect("target lock") = Some(target.summary.id);
    let mut tasks = Vec::with_capacity(waiters);
    for _ in 0..waiters {
        let service = service.clone();
        let id = target.summary.id;
        tasks.push(tokio::spawn(async move {
            if use_backoff {
                service
                    .wait_terminal(id, TIMEOUT)
                    .await
                    .map(|_| Instant::now())
                    .map_err(|error| matches!(error, WaitTaskError::Timeout(_)))
            } else {
                tokio::time::timeout(TIMEOUT, async {
                    loop {
                        match service.get_state_checkpoint(id).await {
                            Ok(Some(checkpoint)) if checkpoint.state.is_terminal() => {
                                return Ok(Instant::now());
                            }
                            Ok(Some(_)) => {}
                            Ok(None) | Err(_) => return Err(false),
                        }
                        tokio::time::sleep(Duration::from_millis(25)).await;
                    }
                })
                .await
                .unwrap_or(Err(true))
            }
        }));
    }
    let probe_start = Instant::now();
    let window_end = probe_start + OBSERVATION_WINDOW;
    let mut probes = ProbeMetrics {
        planned: waiters,
        ..ProbeMetrics::default()
    };
    let submit_replies = Arc::new(Mutex::new(HashMap::<u32, (Instant, bool)>::new()));
    let probe_service = service.clone();
    let probe_submissions = Arc::clone(&submitted_at);
    let probe_replies = Arc::clone(&submit_replies);
    let launch = Arc::new(move |sequence, _planned_at, _launched_at| {
        let service = probe_service.clone();
        let replies = Arc::clone(&probe_replies);
        let submissions = Arc::clone(&probe_submissions);
        async move {
            {
                let mut submissions = submissions.lock().expect("submission lock");
                let submitted_at = Instant::now();
                if !is_before_window_end(submitted_at, window_end) {
                    return false;
                }
                submissions.insert(sequence, submitted_at);
            }
            let result = service
                .submit(
                    TaskKind::<BenchmarkInput>::new("terminal-wait-bench", 1, ValueCodecId::new("example.codec"))
                        .request(BenchmarkInput {
                            sequence,
                            duration_ms: 2,
                        }),
                )
                .await;
            replies
                .lock()
                .expect("submit reply lock")
                .insert(sequence, (Instant::now(), result.is_err()));
            result.is_err()
        }
    });
    let (plans, mut submit_tasks) =
        launch_probe_schedule(waiters, probe_start, PROBE_INTERVAL, window_end, launch).await;
    probes.launched = plans.len();
    // Stop launching at the observation-window boundary, then give dispatched
    // submissions a separate bounded drain period. Missing store intervals
    // remain explicitly unresolved.
    tokio::time::sleep_until(tokio::time::Instant::from_std(window_end)).await;
    let drain_deadline = tokio::time::Instant::now() + SUBMISSION_DRAIN;
    while !submit_tasks.is_empty() {
        match tokio::time::timeout_at(drain_deadline, submit_tasks.join_next()).await {
            Ok(Some(Ok((_sequence, _failed)))) => {}
            Ok(Some(Err(_join_error))) => {}
            Ok(None) => break,
            Err(_) => {
                submit_tasks.abort_all();
                while submit_tasks.join_next().await.is_some() {}
                break;
            }
        }
    }
    let accept_intervals = store.accept_intervals.lock().expect("accept interval lock").clone();
    let public_submit_replies = submit_replies.lock().expect("submit reply lock").clone();
    let actual_submissions = submitted_at.lock().expect("submission lock").clone();
    for plan in &plans {
        probes
            .launch_lag_us
            .push(plan.launched_at.duration_since(plan.planned_at).as_micros());
        if let Some(submitted_at) = actual_submissions.get(&plan.sequence) {
            probes.submitted += 1;
            probes
                .submit_lag_us
                .push(submitted_at.duration_since(plan.planned_at).as_micros());
            match accept_intervals
                .get(&plan.sequence)
                .map(|(started, returned, succeeded)| classify_admission(*started, *returned, *succeeded, window_end))
            {
                Some(AdmissionClass::ConfirmedInWindow) => probes.accepted += 1,
                Some(AdmissionClass::ConfirmedOutside) => probes.accepted_outside += 1,
                Some(AdmissionClass::Overlap) => probes.admission_overlap += 1,
                Some(AdmissionClass::Failed) => probes.failed += 1,
                None if public_submit_replies
                    .get(&plan.sequence)
                    .is_some_and(|(_, failed)| *failed) =>
                {
                    probes.failed += 1
                }
                None => probes.unresolved += 1,
            }
        } else {
            probes.not_submitted += 1;
        }
    }
    for (sequence, (reply, _failed)) in public_submit_replies {
        if let Some(submitted) = actual_submissions.get(&sequence) {
            probes
                .submit_reply_us
                .push(reply.duration_since(*submitted).as_micros());
        }
    }
    let mut observations = Vec::with_capacity(waiters);
    let mut timeouts = 0;
    let mut errors = 0;
    for task in tasks {
        match task.await? {
            Ok(observed) => observations.push(observed),
            Err(true) => timeouts += 1,
            Err(false) => errors += 1,
        }
    }
    tokio::time::sleep_until(tokio::time::Instant::from_std(window_end)).await;
    let successful_observations = observations.len();
    let commit = *store.commit.lock().expect("commit lock");
    let (observations, overlapping) = classify_terminal_samples(&observations, commit)?;
    let accepted = accept_intervals;
    let handlers = handler_at.lock().expect("handler timestamps lock").clone();
    let submissions = submitted_at.lock().expect("submission timestamps lock").clone();
    for (sequence, (_started, returned, ok)) in accepted {
        if !ok {
            continue;
        }
        if returned < window_end {
            if let Some(submitted) = actual_submissions.get(&sequence) {
                probes
                    .admission_us
                    .push(returned.duration_since(*submitted).as_micros());
            }
            if let Some(handler) = handlers.get(&sequence).filter(|at| **at < window_end) {
                probes.started_in_window += 1;
                if *handler >= returned {
                    if let Some(submitted) = actual_submissions.get(&sequence) {
                        probes
                            .admission_handler_us
                            .push(handler.duration_since(*submitted).as_micros());
                    }
                } else {
                    probes.handler_admission_overlap += 1;
                }
            }
        }
    }
    probes.not_started_in_window = probes.accepted.saturating_sub(probes.started_in_window);
    probes.validate()?;
    if observations.len() + overlapping != successful_observations
        || successful_observations + timeouts + errors != waiters
    {
        return Err("terminal wait counts do not reconcile".into());
    }
    let reads = store.read_started.load(std::sync::atomic::Ordering::Relaxed);
    let read_returned = store.read_returned.load(std::sync::atomic::Ordering::Relaxed);
    let read_errors = store.read_errors.load(std::sync::atomic::Ordering::Relaxed);
    debug_assert!(read_returned <= reads && read_errors <= read_returned);
    let mut startup = handler_startup
        .lock()
        .expect("handler times lock")
        .iter()
        .filter(|(is_probe, _)| !is_probe)
        .copied()
        .collect::<Vec<_>>();
    startup.extend(handlers.iter().filter_map(|(sequence, started)| {
        if *sequence == 0 || *started >= window_end {
            return None;
        }
        submissions
            .get(sequence)
            .map(|submitted| (true, started.duration_since(*submitted).as_micros()))
    }));
    service.shutdown().await?;
    Ok((
        observations,
        startup,
        reads,
        read_returned,
        read_errors,
        timeouts,
        errors,
        overlapping,
        probes,
    ))
}

struct PollResult {
    first_read: Instant,
    observed: Instant,
}

enum PollError {
    Timeout,
    Read,
}

async fn run_round(
    temp: &TemporaryDirectory,
    round: usize,
    waiters: usize,
    strategy: Strategy,
) -> Result<Outcome, Box<dyn std::error::Error>> {
    let db = temp.join(format!("wait-{round}-{waiters}-{}.sqlite", strategy.name()));
    initialize(&db)?;
    // Open every participant before starting the timer or releasing the barrier.
    // A failed open therefore returns directly instead of stranding waiters.
    let mut readers = Vec::with_capacity(waiters);
    for index in 0..waiters {
        let connection = open_participant(&db, round, strategy, &format!("reader-{index}"))?;
        connection.busy_timeout(Duration::from_secs(2))?;
        readers.push(connection);
    }
    let writer_connection = open_participant(&db, round, strategy, "writer")?;
    writer_connection.busy_timeout(Duration::from_secs(2))?;
    let start: Arc<OnceLock<Instant>> = Arc::new(OnceLock::new());
    let committed_at = Arc::new(Mutex::new(None));
    let barrier = Arc::new(Barrier::new(waiters.max(1) + 1));
    let mut tasks = Vec::with_capacity(waiters);
    let read_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    for connection in readers {
        let barrier = Arc::clone(&barrier);
        let reads = Arc::clone(&read_count);
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            let first_read = Instant::now();
            let began = first_read;
            let mut interval = strategy.first_interval();
            let deadline = began + TIMEOUT;
            loop {
                reads.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let terminal: i64 = connection
                    .query_row("SELECT terminal FROM checkpoint WHERE id=1", [], |row| row.get(0))
                    .map_err(|_| PollError::Read)?;
                if terminal != 0 {
                    return Ok::<_, PollError>(PollResult {
                        first_read,
                        observed: Instant::now(),
                    });
                }
                let now = Instant::now();
                if now >= deadline {
                    return Err(PollError::Timeout);
                }
                tokio::time::sleep(interval.min(deadline - now)).await;
                interval = strategy.next_interval(interval);
            }
        }));
    }

    let writer_committed = Arc::clone(&committed_at);
    let writer_start = Arc::clone(&start);
    let writer = std::thread::spawn(move || -> Result<(), rusqlite::Error> {
        while writer_start.get().is_none() {
            std::thread::yield_now();
        }
        let common_start = *writer_start.get().expect("common start is published");
        std::thread::sleep(COMMIT_DELAY.saturating_sub(common_start.elapsed()));
        let before = Instant::now();
        writer_connection.execute("UPDATE checkpoint SET terminal=1 WHERE id=1", [])?;
        let after = Instant::now();
        *writer_committed.lock().expect("commit timestamp lock") = Some((before, after));
        Ok(())
    });
    let common_start = Instant::now();
    start.set(common_start).map_err(|_| "common start was already set")?;
    if waiters > 0 {
        barrier.wait().await;
    }
    let mut outcome = Outcome::default();
    let mut observed_at = Vec::with_capacity(waiters);
    for task in tasks {
        match task.await? {
            Ok(result) => observed_at.push(result),
            Err(PollError::Timeout) => outcome.timeouts += 1,
            Err(PollError::Read) => outcome.errors += 1,
        }
    }
    writer.join().map_err(|_| "writer thread panicked")??;
    let commit = committed_at
        .lock()
        .expect("commit timestamp lock")
        .ok_or("writer completed without a commit interval")?;
    for result in observed_at {
        outcome.successes += 1;
        outcome
            .startup_us
            .push(result.first_read.duration_since(common_start).as_micros());
        if result.observed < commit.1 {
            outcome.overlapping_commit += 1;
            // Keep these samples separate: assigning zero would bias the
            // latency quantile.
        } else {
            outcome
                .observation_us
                .push(result.observed.duration_since(commit.1).as_micros());
        }
    }
    outcome.checkpoint_reads = read_count.load(std::sync::atomic::Ordering::Relaxed);
    Ok(outcome)
}

fn open_participant(path: &Path, round: usize, strategy: Strategy, participant: &str) -> Result<Connection, String> {
    Connection::open(path).map_err(|error| {
        format!(
            "round={round} strategy={} participant={participant} open failed: {error}",
            strategy.name()
        )
    })
}

fn percentile(samples: &[u128], percent: usize) -> String {
    if samples.is_empty() {
        return "N/A".to_owned();
    }
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    sorted[(sorted.len() - 1) * percent / 100].to_string()
}

fn initialize(path: &Path) -> Result<(), rusqlite::Error> {
    let connection = Connection::open(path)?;
    connection.execute_batch("PRAGMA journal_mode=WAL; CREATE TABLE checkpoint(id INTEGER PRIMARY KEY, terminal INTEGER NOT NULL); INSERT INTO checkpoint VALUES(1,0);")
}

struct TemporaryDirectory(PathBuf);

impl TemporaryDirectory {
    fn new() -> Result<Self, std::io::Error> {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock after epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("qubit-task-terminal-wait-{nonce}"));
        std::fs::create_dir(&path)?;
        Ok(Self(path))
    }
}

impl std::ops::Deref for TemporaryDirectory {
    type Target = Path;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Drop for TemporaryDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
