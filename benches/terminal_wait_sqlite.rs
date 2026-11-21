// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Matched SQLite checkpoint-polling comparison for terminal-wait intervals.

use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

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
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
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
                    "round={round} waiters={waiter_count} strategy={} checkpoint_reads={} startup_p95_us={} observation_p50_us={} observation_p95_us={} timeout={} errors={}",
                    strategy.name(),
                    outcome.checkpoint_reads,
                    percentile(&outcome.startup_us, 95),
                    percentile(&outcome.observation_us, 50),
                    percentile(&outcome.observation_us, 95),
                    outcome.timeouts,
                    outcome.errors,
                );
            }
        }
    }
    Ok(())
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
    let start = Instant::now();
    let committed_at = Arc::new(Mutex::new(None));
    let barrier = Arc::new(Barrier::new(waiters.max(1) + 1));
    let mut tasks = Vec::with_capacity(waiters);
    let read_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    for _ in 0..waiters {
        let db = db.clone();
        let barrier = Arc::clone(&barrier);
        let reads = Arc::clone(&read_count);
        let round_started = start;
        tasks.push(tokio::spawn(async move {
            let connection = Connection::open(db).map_err(|_| PollError::Read)?;
            barrier.wait().await;
            let first_read = Instant::now();
            let _startup_latency = first_read.duration_since(round_started);
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

    let writer_db = db.clone();
    let writer_committed = Arc::clone(&committed_at);
    let writer = std::thread::spawn(move || -> Result<(), rusqlite::Error> {
        std::thread::sleep(COMMIT_DELAY);
        let connection = Connection::open(writer_db)?;
        connection.execute("UPDATE checkpoint SET terminal=1 WHERE id=1", [])?;
        *writer_committed.lock().expect("commit timestamp lock") = Some(Instant::now());
        Ok(())
    });
    if waiters > 0 {
        barrier.wait().await;
    }
    let mut outcome = Outcome::default();
    for task in tasks {
        match task.await? {
            Ok(result) => {
                outcome
                    .startup_us
                    .push(result.first_read.duration_since(start).as_micros());
                if let Some(committed) = *committed_at.lock().expect("commit timestamp lock") {
                    outcome
                        .observation_us
                        .push(result.observed.duration_since(committed).as_micros());
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

fn percentile(samples: &[u128], percent: usize) -> u128 {
    if samples.is_empty() {
        return 0;
    }
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    sorted[(sorted.len() - 1) * percent / 100]
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
