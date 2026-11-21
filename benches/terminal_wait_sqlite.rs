// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Matched SQLite checkpoint-polling comparison for terminal-wait intervals.

use std::collections::HashMap;
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

const ROUNDS: usize = 5;
const COMMIT_DELAY: Duration = Duration::from_millis(80);
const TIMEOUT: Duration = Duration::from_millis(400);

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
        let mut timeouts = [0_usize; 2];
        let mut errors = [0_usize; 2];
        let mut overlaps = [0_usize; 2];
        for round in 0..ROUNDS {
            let order = if round % 2 == 0 { [0, 1] } else { [1, 0] };
            for strategy_index in order {
                let backoff_first = strategy_index == 1;
                let (latency, startup, count, timeout, error, overlap) =
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
                timeouts[strategy_index] += timeout;
                errors[strategy_index] += error;
                overlaps[strategy_index] += overlap;
            }
        }
        for strategy_index in 0..2 {
            println!(
                "real_store waiters={waiters} strategy={} reads={reads} observations={} startup_p95_us={} probe_startup_p95_us={} observation_p95_us={} timeout={timeouts} errors={errors} overlapping_commit={overlaps}",
                if strategy_index == 1 {
                    "current-backoff"
                } else {
                    "fixed-25ms"
                },
                samples[strategy_index].len(),
                percentile(&target_startup[strategy_index], 95),
                percentile(&probe_startup[strategy_index], 95),
                percentile(&samples[strategy_index], 95),
                reads = reads[strategy_index],
                timeouts = timeouts[strategy_index],
                errors = errors[strategy_index],
                overlaps = overlaps[strategy_index],
            );
        }
    }
    Ok(())
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

struct CountingStore {
    inner: Arc<SqliteTaskStore>,
    target: Mutex<Option<TaskId>>,
    reads: std::sync::atomic::AtomicUsize,
    commit: Mutex<Option<(Instant, Instant)>>,
}
impl TaskStore for CountingStore {
    delegate_task_store!(inner; except [get_state_checkpoint, transition_encoded]);

    fn get_state_checkpoint<'a>(
        &'a self,
        id: TaskId,
    ) -> TaskFuture<'a, Result<Option<TaskStateCheckpoint>, StoreError>> {
        let future = self.inner.get_state_checkpoint(id);
        Box::pin(async move {
            if self.target.lock().expect("target lock").as_ref() == Some(&id) {
                self.reads.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            future.await
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
) -> Result<(Vec<u128>, Vec<(bool, u128)>, usize, usize, usize, usize), Box<dyn std::error::Error>> {
    let path = temp.join(format!("real-{round}-{waiters}-{use_backoff}.sqlite"));
    let inner = Arc::new(SqliteTaskStore::open(&path)?);
    let store = Arc::new(CountingStore {
        inner,
        target: Mutex::new(None),
        reads: std::sync::atomic::AtomicUsize::new(0),
        commit: Mutex::new(None),
    });
    let submitted_at = Arc::new(Mutex::new(HashMap::<u32, Instant>::new()));
    let handler_startup = Arc::new(Mutex::new(Vec::<(bool, u128)>::new()));
    let codecs = Arc::new(ValueBytesCodecRegistry::from_registrations([
        &BENCHMARK_CODEC_REGISTRATION,
    ])?);
    let ids = Arc::new(qubit_id::SnowflakeGenerator::new(0)?);
    let mut builder = TaskExecutionServiceBuilder::new(store.clone(), codecs, ids)
        .max_running_tasks(NonZeroUsize::new(2).expect("two workers is non-zero"));
    let handler_times = Arc::clone(&handler_startup);
    let submissions = Arc::clone(&submitted_at);
    builder.register_handler_fn::<BenchmarkInput, _, _>(
        TaskHandlerDescriptor::for_payload::<BenchmarkInput>("terminal-wait-bench", 1),
        move |input, _context| {
            let handler_times = Arc::clone(&handler_times);
            let submissions = Arc::clone(&submissions);
            async move {
                let started = Instant::now();
                if let Some(submitted) = submissions.lock().expect("submission lock").get(&input.sequence) {
                    handler_times
                        .lock()
                        .expect("handler times lock")
                        .push((input.sequence != 0, started.duration_since(*submitted).as_micros()));
                }
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
                let deadline = Instant::now() + TIMEOUT;
                loop {
                    match service.get_state_checkpoint(id).await {
                        Ok(Some(checkpoint)) if checkpoint.state.is_terminal() => return Ok(Instant::now()),
                        Ok(Some(_)) => {}
                        Ok(None) | Err(_) => return Err(false),
                    }
                    let now = Instant::now();
                    if now >= deadline {
                        return Err(true);
                    }
                    tokio::time::sleep(Duration::from_millis(25).min(deadline - now)).await;
                }
            }
        }));
    }
    for sequence in 1..=u32::try_from(waiters)? {
        submitted_at
            .lock()
            .expect("submission lock")
            .insert(sequence, Instant::now());
        let _ = service
            .submit(
                TaskKind::<BenchmarkInput>::new("terminal-wait-bench", 1, ValueCodecId::new("example.codec")).request(
                    BenchmarkInput {
                        sequence,
                        duration_ms: 2,
                    },
                ),
            )
            .await?;
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
    let commit = *store.commit.lock().expect("commit lock");
    let mut overlapping = 0;
    let observations = observations
        .into_iter()
        .map(|observed| {
            commit.map_or(0, |(_before, after)| {
                if observed < after {
                    overlapping += 1;
                    0
                } else {
                    observed.duration_since(after).as_micros()
                }
            })
        })
        .collect();
    let reads = store.reads.load(std::sync::atomic::Ordering::Relaxed);
    let startup = handler_startup.lock().expect("handler times lock").clone();
    service.shutdown().await?;
    Ok((observations, startup, reads, timeouts, errors, overlapping))
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
    for task in tasks {
        match task.await? {
            Ok(result) => {
                outcome.successes += 1;
                outcome
                    .startup_us
                    .push(result.first_read.duration_since(common_start).as_micros());
                if let Some((_before, after)) = *committed_at.lock().expect("commit timestamp lock") {
                    if result.observed < after {
                        outcome.overlapping_commit += 1;
                        outcome.observation_us.push(0);
                    } else {
                        outcome
                            .observation_us
                            .push(result.observed.duration_since(after).as_micros());
                    }
                }
            }
            Err(PollError::Timeout) => outcome.timeouts += 1,
            Err(PollError::Read) => outcome.errors += 1,
        }
    }
    writer.join().map_err(|_| "writer thread panicked")??;
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
