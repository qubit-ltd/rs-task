// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Measures typed in-memory task acceptance with different retained histories.

use std::num::NonZeroUsize;
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
use qubit_task::model::typed::TaskId;
use qubit_task::model::typed::TransitionCommand;
use qubit_task::store::MemoryTaskStore;
use qubit_task::store::TaskStore;

const HISTORIES: [u64; 4] = [0, 256, 4_096, 32_768];
const BATCH_SIZE: u64 = 128;
const WARMUP_BATCHES: usize = 3;
const MEASURED_BATCHES: usize = 9;
const PAYLOAD_SIZE: usize = 64;
const PAYLOAD_BUDGET: usize = 64 * 1024 * 1024;

fn request() -> StoredTaskRequest {
    StoredTaskRequest {
        kind_id: "memory.accept.benchmark".to_owned(),
        category: None,
        payload: StoredPayload {
            type_id: ModelIdBuf::parse("qubit_task.bench.Payload").expect("valid model ID"),
            schema_version: 1,
            codec_id: "qubit.bytes.json".to_owned(),
            bytes: vec![0x5a; PAYLOAD_SIZE],
        },
        metadata: Metadata::new(),
        resource_limit: ResourceRequest::default(),
        correlation_key: None,
        idempotency_key: None,
    }
}

async fn accept_one(store: &MemoryTaskStore, id: TaskId, request: &StoredTaskRequest) {
    let accepted = store
        .accept_encoded(id, request.clone())
        .await
        .expect("unique benchmark request is accepted");
    assert!(accepted.created, "benchmark request must be unique");
}

async fn finish_batch(store: &MemoryTaskStore, ids: &[TaskId]) {
    for id in ids {
        let started = store
            .start_encoded(StartCommand {
                id: *id,
                expected_state_version: 0,
                started_at_ms: 1,
            })
            .await
            .expect("benchmark task starts");
        let finished = store
            .transition_encoded(TransitionCommand {
                id: *id,
                expected_state_version: started.state_version,
                expected_attempt: started.attempt,
                retry_not_before_ms: None,
                state: TaskState::Succeeded,
                cancel_requested: false,
                cancel_error: None,
                output: None,
                finished_at_ms: Some(2),
            })
            .await
            .expect("benchmark task becomes terminal");
        assert_eq!(finished.state, TaskState::Succeeded);
    }
}

async fn initialize_history(store: &MemoryTaskStore, count: u64, next_id: &mut u64, request: &StoredTaskRequest) {
    for _ in 0..count {
        let id = TaskId::from_id(Id::new(*next_id));
        *next_id += 1;
        accept_one(store, id, request).await;
        finish_batch(store, &[id]).await;
    }
}

async fn assert_history_size(store: &MemoryTaskStore, expected: u64, last_accepted_id: u64) {
    let mut count = 0_u64;
    for value in 1..=last_accepted_id {
        if let Some(task) = store
            .get_encoded_task(TaskId::from_id(Id::new(value)))
            .await
            .expect("task count lookup succeeds")
        {
            assert!(task.summary.state.is_terminal());
            count += 1;
        }
    }
    assert_eq!(count, expected, "terminal history returns to its target size");
}

async fn measure_group(history: u64) -> Vec<Duration> {
    let store = MemoryTaskStore::with_payload_budget(
        usize::try_from(history).expect("history fits usize"),
        NonZeroUsize::new(PAYLOAD_BUDGET).expect("budget is positive"),
    );
    let request = request();
    let mut next_id = 1_u64;
    initialize_history(&store, history, &mut next_id, &request).await;
    assert_history_size(&store, history, next_id - 1).await;

    let mut samples = Vec::with_capacity(MEASURED_BATCHES);
    for sample_index in 0..(WARMUP_BATCHES + MEASURED_BATCHES) {
        let mut ids = Vec::with_capacity(usize::try_from(BATCH_SIZE).expect("batch fits usize"));
        let start = Instant::now();
        for _ in 0..BATCH_SIZE {
            let id = TaskId::from_id(Id::new(next_id));
            next_id += 1;
            accept_one(&store, id, &request).await;
            ids.push(id);
        }
        let elapsed = start.elapsed();
        finish_batch(&store, &ids).await;
        assert_history_size(&store, history, next_id - 1).await;

        if history > 0 {
            for id in &ids {
                let task = store
                    .get_encoded_task(*id)
                    .await
                    .expect("task lookup succeeds")
                    .expect("new task is retained at the configured history size");
                assert_eq!(task.summary.state, TaskState::Succeeded);
            }
        } else {
            for id in &ids {
                assert!(store.get_encoded_task(*id).await.expect("lookup succeeds").is_none());
            }
        }

        if sample_index >= WARMUP_BATCHES {
            samples.push(elapsed / u32::try_from(BATCH_SIZE).expect("batch fits u32"));
        }
    }
    samples
}

fn percentile(sorted: &[Duration], numerator: usize, denominator: usize) -> Duration {
    let rank = sorted.len().saturating_mul(numerator).div_ceil(denominator).max(1);
    sorted[rank - 1]
}

fn main() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("Tokio runtime builds");
    eprintln!(
        "memory_accept mode=release target={}-{} workers={} batch={} warmup_batches={} measured_batches={} payload_bytes={} budget_bytes={}",
        std::env::consts::ARCH,
        std::env::consts::OS,
        std::thread::available_parallelism().map_or(1, usize::from),
        BATCH_SIZE,
        WARMUP_BATCHES,
        MEASURED_BATCHES,
        PAYLOAD_SIZE,
        PAYLOAD_BUDGET,
    );
    for history in HISTORIES {
        let mut samples = runtime.block_on(measure_group(history));
        samples.sort_unstable();
        let median = percentile(&samples, 1, 2);
        let p95 = percentile(&samples, 95, 100);
        println!(
            "history={history} samples={} min_ns_per_accept={} median_ns_per_accept={} p95_ns_per_accept={} max_ns_per_accept={}",
            samples.len(),
            samples.first().expect("at least one measured sample").as_nanos(),
            median.as_nanos(),
            p95.as_nanos(),
            samples.last().expect("at least one measured sample").as_nanos(),
        );
    }
}
