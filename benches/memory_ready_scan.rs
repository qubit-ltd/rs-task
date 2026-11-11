// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Measures ready pages, retry-deadline queries, scheduler wait cycles, and
//! concurrent store access under varying future and ready queue sizes.

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
const READY_SIZES: [u64; 3] = [0, 1, 128];
const PAGE: usize = 128;
const WARMUPS: usize = 5;
const SAMPLES: usize = 31;
const BUDGET: usize = 64 * 1024 * 1024;
const NOW_MS: u64 = 10_000;
const FUTURE_MS: u64 = NOW_MS + 60_000;

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

async fn add_future(store: &MemoryTaskStore, id: u64, req: &StoredTaskRequest, deadline: u64) {
    let task_id = TaskId::from_id(Id::new(id));
    let accepted = TaskStore::accept_encoded(store, task_id, req.clone())
        .await
        .expect("task accepted");
    let started = TaskStore::start_encoded(
        store,
        StartCommand {
            id: task_id,
            expected_state_version: accepted.summary.state_version,
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
            retry_not_before_ms: Some(deadline),
            state: TaskState::Queued,
            cancel_requested: false,
            cancel_error: None,
            output: None,
            finished_at_ms: None,
        },
    )
    .await
    .expect("task is requeued with a deadline");
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

async fn scan_with_probe(store: Arc<MemoryTaskStore>, started: Vec<oneshot::Sender<()>>) -> usize {
    let mut after: Option<TaskCursor> = None;
    let mut records = 0;
    let mut started = Some(started);
    loop {
        let page = TaskStore::list_ready_queued(
            store.as_ref(),
            after,
            NonZeroUsize::new(PAGE).expect("positive page size"),
            NOW_MS,
        )
        .await
        .expect("ready scan succeeds");
        records += page.records.len();
        if let Some(started) = started.take() {
            for sender in started {
                let _ = sender.send(());
            }
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

async fn concurrent_probe_samples(
    store: &Arc<MemoryTaskStore>,
    expected: usize,
    first_probe_id: u64,
    req: &StoredTaskRequest,
) -> (Vec<Duration>, Vec<Duration>) {
    let mut get_samples = Vec::with_capacity(SAMPLES);
    let mut accept_samples = Vec::with_capacity(SAMPLES);
    for sample in 0..SAMPLES {
        let (started_tx, started_rx) = oneshot::channel();
        let (accept_started_tx, accept_started_rx) = oneshot::channel();
        let scan_store = Arc::clone(store);
        let scan_task =
            tokio::spawn(async move { scan_with_probe(scan_store, vec![started_tx, accept_started_tx]).await });
        let query_store = Arc::clone(store);
        let get_task = tokio::spawn(async move {
            started_rx.await.expect("scan reached its first page");
            let start = Instant::now();
            let _ = TaskStore::get_encoded_task(query_store.as_ref(), TaskId::from_id(Id::new(1)))
                .await
                .expect("concurrent task lookup succeeds");
            start.elapsed()
        });
        let accept_store = Arc::clone(store);
        let accept_request = req.clone();
        let accept_task = tokio::spawn(async move {
            accept_started_rx.await.expect("scan reached its first page");
            let id = TaskId::from_id(Id::new(first_probe_id + sample as u64));
            let start = Instant::now();
            TaskStore::accept_encoded(accept_store.as_ref(), id, accept_request)
                .await
                .expect("concurrent accept succeeds");
            start.elapsed()
        });
        let count = scan_task.await.expect("scan task joins");
        let prior_accepts = expected + sample;
        assert!(count == prior_accepts || count == prior_accepts + 1);
        get_samples.push(get_task.await.expect("get task joins"));
        accept_samples.push(accept_task.await.expect("accept task joins"));
    }
    (get_samples, accept_samples)
}

fn percentile(sorted: &[Duration], n: usize, d: usize) -> Duration {
    sorted[sorted.len().saturating_mul(n).div_ceil(d).max(1) - 1]
}

async fn deadline(store: &MemoryTaskStore, now_ms: u64) -> Option<u64> {
    TaskStore::next_retry_deadline(store, now_ms)
        .await
        .expect("retry deadline query succeeds")
}

async fn run(future: u64, ready: u64) {
    let total = future + ready;
    let store = Arc::new(MemoryTaskStore::with_limits(
        0,
        NonZeroUsize::new(BUDGET).expect("positive budget"),
        NonZeroUsize::new(usize::try_from(total + 2 * SAMPLES as u64 + 1).expect("queue fits usize"))
            .expect("positive queue limit"),
    ));
    let req = request();
    for offset in 1..=future {
        // Alternate two stable deadlines to cover duplicate-key ordering and
        // ensure the query returns the minimum future value.
        let due = if offset % 2 == 0 { FUTURE_MS + 1 } else { FUTURE_MS };
        add_future(&store, offset, &req, due).await;
    }
    for offset in 1..=ready {
        add(&store, future + offset, &req, false).await;
    }
    let expected = usize::try_from(ready).expect("ready count fits usize");
    let mut page_samples = Vec::with_capacity(SAMPLES);
    let mut deadline_samples = Vec::with_capacity(SAMPLES);
    let mut cycle_samples = Vec::with_capacity(SAMPLES);
    for sample in 0..WARMUPS + SAMPLES {
        let start = Instant::now();
        let count = scan(&store, NOW_MS).await;
        let page_elapsed = start.elapsed();
        assert_eq!(count, expected);
        let start = Instant::now();
        assert_eq!(deadline(&store, NOW_MS).await, (future > 0).then_some(FUTURE_MS));
        let deadline_elapsed = start.elapsed();
        let start = Instant::now();
        let count = scan(&store, NOW_MS).await;
        let _next = deadline(&store, NOW_MS).await;
        // This measures the complete scan-and-select path through the point
        // where scheduler_loop would enter sleep; wall-clock sleep is excluded.
        let cycle_elapsed = start.elapsed();
        assert_eq!(count, expected);
        if sample >= WARMUPS {
            page_samples.push(page_elapsed);
            deadline_samples.push(deadline_elapsed);
            cycle_samples.push(cycle_elapsed);
        }
    }
    page_samples.sort_unstable();
    deadline_samples.sort_unstable();
    cycle_samples.sort_unstable();

    let mut retry_transition_samples = Vec::with_capacity(SAMPLES);
    for sample in 0..SAMPLES as u64 {
        let id = TaskId::from_id(Id::new(total + sample + 1));
        let accepted = TaskStore::accept_encoded(store.as_ref(), id, req.clone())
            .await
            .expect("retry timing task is accepted");
        let running = TaskStore::start_encoded(
            store.as_ref(),
            StartCommand {
                id,
                expected_state_version: accepted.summary.state_version,
                started_at_ms: 1,
            },
        )
        .await
        .expect("retry timing task starts");
        let start = Instant::now();
        TaskStore::transition_encoded(
            store.as_ref(),
            TransitionCommand {
                id,
                expected_state_version: running.state_version,
                expected_attempt: running.attempt,
                retry_not_before_ms: Some(FUTURE_MS),
                state: TaskState::Queued,
                cancel_requested: false,
                cancel_error: None,
                output: None,
                finished_at_ms: None,
            },
        )
        .await
        .expect("retry timing task is requeued");
        retry_transition_samples.push(start.elapsed());
    }
    retry_transition_samples.sort_unstable();

    let (mut get_samples, mut accept_samples) =
        concurrent_probe_samples(&store, expected, total + SAMPLES as u64 + 1, &req).await;
    get_samples.sort_unstable();
    accept_samples.sort_unstable();
    println!(
        "future={future} ready={ready} now_ms={NOW_MS} future_deadline_ms={FUTURE_MS} page={PAGE} ready_scan_visit_upper_bound={} ready_page_ns={:?} ready_page_median_ns={} ready_page_p95_ns={} deadline_ns={:?} deadline_median_ns={} deadline_p95_ns={} scheduler_cycle_ns={:?} scheduler_cycle_median_ns={} scheduler_cycle_p95_ns={} running_to_queued_retry_ns={:?} running_to_queued_retry_median_ns={} running_to_queued_retry_p95_ns={} concurrent_get_ns={:?} concurrent_get_median_ns={} concurrent_get_p95_ns={} concurrent_accept_ns={:?} concurrent_accept_median_ns={} concurrent_accept_p95_ns={}",
        future + ready,
        page_samples.iter().map(Duration::as_nanos).collect::<Vec<_>>(),
        percentile(&page_samples, 1, 2).as_nanos(),
        percentile(&page_samples, 95, 100).as_nanos(),
        deadline_samples.iter().map(Duration::as_nanos).collect::<Vec<_>>(),
        percentile(&deadline_samples, 1, 2).as_nanos(),
        percentile(&deadline_samples, 95, 100).as_nanos(),
        cycle_samples.iter().map(Duration::as_nanos).collect::<Vec<_>>(),
        percentile(&cycle_samples, 1, 2).as_nanos(),
        percentile(&cycle_samples, 95, 100).as_nanos(),
        retry_transition_samples
            .iter()
            .map(Duration::as_nanos)
            .collect::<Vec<_>>(),
        percentile(&retry_transition_samples, 1, 2).as_nanos(),
        percentile(&retry_transition_samples, 95, 100).as_nanos(),
        get_samples.iter().map(Duration::as_nanos).collect::<Vec<_>>(),
        percentile(&get_samples, 1, 2).as_nanos(),
        percentile(&get_samples, 95, 100).as_nanos(),
        accept_samples.iter().map(Duration::as_nanos).collect::<Vec<_>>(),
        percentile(&accept_samples, 1, 2).as_nanos(),
        percentile(&accept_samples, 95, 100).as_nanos(),
    );
}

fn main() {
    eprintln!(
        "memory_ready_scan mode=release target={}-{} available_cpus={} runtime_workers=4 page={} warmups={} samples={} payload_bytes=64 budget_bytes={} future_sizes={:?} ready_sizes={:?} seed=stable-sequential-ids deadline_pattern=alternating-two-values",
        std::env::consts::ARCH,
        std::env::consts::OS,
        std::thread::available_parallelism().map_or(1, usize::from),
        PAGE,
        WARMUPS,
        SAMPLES,
        BUDGET,
        SIZES,
        READY_SIZES
    );
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("runtime builds");
    for future in SIZES {
        for ready in READY_SIZES {
            rt.block_on(run(future, ready));
        }
    }
}
