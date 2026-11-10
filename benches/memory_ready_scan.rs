// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Measures complete ready-queue scans with varying terminal history sizes.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use qubit_id::Id;
use qubit_metadata::Metadata;
use qubit_model_id::ModelIdBuf;
use qubit_task::model::TaskState;
use qubit_task::model::typed::ResourceRequest;
use qubit_task::model::typed::StartCommand;
use qubit_task::model::typed::StoredPayload;
use qubit_task::model::typed::StoredTaskRequest;
use qubit_task::model::typed::TaskCursor;
use qubit_task::model::typed::TaskId;
use qubit_task::model::typed::TransitionCommand;
use qubit_task::store::MemoryTaskStore;
use qubit_task::store::TaskStore;
use tokio::sync::oneshot;

const SIZES: [u64; 4] = [0, 256, 4_096, 32_768];
const PAGE: usize = 128;
const WARMUPS: usize = 3;
const SAMPLES: usize = 9;
const BUDGET: usize = 64 * 1024 * 1024;

fn request() -> StoredTaskRequest {
    StoredTaskRequest {
        kind_id: "memory.ready_scan.benchmark".to_owned(),
        category: None,
        payload: StoredPayload {
            type_id: ModelIdBuf::parse("qubit_task.bench.Payload").expect("valid model ID"),
            schema_version: 1,
            codec_id: "qubit.bytes.json".to_owned(),
            bytes: vec![0x5a; 64],
        },
        metadata: Metadata::new(),
        resource_limit: ResourceRequest::default(),
        correlation_key: None,
        idempotency_key: None,
    }
}

async fn add(store: &MemoryTaskStore, id: u64, req: &StoredTaskRequest, terminal: bool) {
    let task_id = TaskId::from_id(Id::new(id));
    TaskStore::accept_encoded(store, task_id, req.clone())
        .await
        .expect("task accepted");
    if terminal {
        let started = TaskStore::start_encoded(
            store,
            StartCommand {
                id: task_id,
                expected_state_version: 0,
                started_at_ms: 1,
            },
        )
        .await
        .expect("task starts");
        TaskStore::transition_encoded(
            store,
            TransitionCommand {
                id: task_id,
                expected_state_version: started.state_version,
                expected_attempt: started.attempt,
                retry_not_before_ms: None,
                state: TaskState::Succeeded,
                cancel_requested: false,
                cancel_error: None,
                output: None,
                finished_at_ms: Some(2),
            },
        )
        .await
        .expect("task finishes");
    }
}

async fn scan(store: &MemoryTaskStore, now_ms: u64) -> usize {
    let mut after: Option<TaskCursor> = None;
    let mut records = 0;
    loop {
        let page = TaskStore::list_ready_queued(
            store,
            after,
            NonZeroUsize::new(PAGE).expect("positive page size"),
            now_ms,
        )
        .await
        .expect("ready scan succeeds");
        records += page.records.len();
        after = page.next;
        if after.is_none() {
            return records;
        }
    }
}

async fn scan_with_probe(store: Arc<MemoryTaskStore>, started: oneshot::Sender<()>) -> usize {
    let mut after: Option<TaskCursor> = None;
    let mut records = 0;
    let mut started = Some(started);
    loop {
        let page = TaskStore::list_ready_queued(
            store.as_ref(),
            after,
            NonZeroUsize::new(PAGE).expect("positive page size"),
            u64::MAX,
        )
        .await
        .expect("ready scan succeeds");
        records += page.records.len();
        if let Some(started) = started.take() {
            let _ = started.send(());
            // Let the concurrent lookup run after a scan page has released the
            // store lock and before this complete scan returns.
            tokio::task::yield_now().await;
        }
        after = page.next;
        if after.is_none() {
            return records;
        }
    }
}

async fn concurrent_probe_samples(store: &Arc<MemoryTaskStore>, expected: usize) -> Vec<Duration> {
    let mut samples = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let (started_tx, started_rx) = oneshot::channel();
        let scan_store = Arc::clone(store);
        let scan_task = tokio::spawn(async move { scan_with_probe(scan_store, started_tx).await });
        let query_store = Arc::clone(store);
        let query_task = tokio::spawn(async move {
            started_rx.await.expect("scan reached its first page");
            let start = Instant::now();
            let _ = TaskStore::get_encoded_task(query_store.as_ref(), TaskId::from_id(Id::new(1)))
                .await
                .expect("concurrent task lookup succeeds");
            start.elapsed()
        });
        assert_eq!(scan_task.await.expect("scan task joins"), expected);
        samples.push(query_task.await.expect("query task joins"));
    }
    samples
}

fn percentile(sorted: &[Duration], n: usize, d: usize) -> Duration {
    sorted[sorted.len().saturating_mul(n).div_ceil(d).max(1) - 1]
}

async fn run(history: u64, queued: u64) {
    let store = Arc::new(MemoryTaskStore::with_limits(
        usize::try_from(history).expect("history fits usize"),
        NonZeroUsize::new(BUDGET).expect("positive budget"),
        NonZeroUsize::new(usize::try_from(queued).expect("queue fits usize").max(1)).expect("positive queue limit"),
    ));
    let req = request();
    for id in 1..=history {
        add(&store, id, &req, true).await;
    }
    for offset in 1..=queued {
        add(&store, history + offset, &req, false).await;
    }
    let expected = usize::try_from(queued).expect("queue fits usize");
    let pages = expected.div_ceil(PAGE).max(1);
    let baseline_entry_visits = pages * (history + queued) as usize;
    let indexed_entry_visits = if expected == 0 { 0 } else { expected + pages - 1 };
    let mut samples = Vec::with_capacity(SAMPLES);
    for sample in 0..WARMUPS + SAMPLES {
        let start = Instant::now();
        let count = scan(&store, u64::MAX).await;
        let elapsed = start.elapsed();
        assert_eq!(count, expected);
        if sample >= WARMUPS {
            samples.push(elapsed);
        }
    }
    samples.sort_unstable();
    let mut probe_samples = concurrent_probe_samples(&store, expected).await;
    probe_samples.sort_unstable();
    println!(
        "history={history} queued={queued} page={PAGE} baseline_entry_visits={baseline_entry_visits} indexed_entry_visits={indexed_entry_visits} samples_ns={:?} median_ns={} p95_ns={} concurrent_probe_samples_ns={:?} concurrent_probe_median_ns={} concurrent_probe_p95_ns={}",
        samples.iter().map(Duration::as_nanos).collect::<Vec<_>>(),
        percentile(&samples, 1, 2).as_nanos(),
        percentile(&samples, 95, 100).as_nanos(),
        probe_samples.iter().map(Duration::as_nanos).collect::<Vec<_>>(),
        percentile(&probe_samples, 1, 2).as_nanos(),
        percentile(&probe_samples, 95, 100).as_nanos()
    );
}

fn main() {
    eprintln!(
        "memory_ready_scan mode=release target={}-{} available_cpus={} runtime_workers=4 page={} warmups={} samples={} payload_bytes=64 budget_bytes={} sizes={:?}",
        std::env::consts::ARCH,
        std::env::consts::OS,
        std::thread::available_parallelism().map_or(1, usize::from),
        PAGE,
        WARMUPS,
        SAMPLES,
        BUDGET,
        SIZES
    );
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("runtime builds");
    for history in SIZES {
        for queued in SIZES {
            rt.block_on(run(history, queued));
        }
    }
}
