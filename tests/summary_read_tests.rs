// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Summary-only reads return lifecycle state without decoding stored payloads.

use std::alloc::GlobalAlloc;
use std::alloc::Layout;
use std::alloc::System;
use std::cell::Cell;
use std::num::NonZeroUsize;

use qubit_id::Id;
use qubit_metadata::Metadata;
use qubit_model_id::ModelIdBuf;
use qubit_task::model::TaskState;
use qubit_task::model::typed::StartCommand;
use qubit_task::model::typed::StoredPayload;
use qubit_task::model::typed::StoredTaskRequest;
use qubit_task::model::typed::TaskId;
use qubit_task::model::typed::TransitionCommand;
use qubit_task::store::MemoryTaskStore;
#[cfg(feature = "sqlite")]
use qubit_task::store::SqliteTaskStore;
use qubit_task::store::TaskStore;
#[cfg(feature = "sqlite")]
use rusqlite::Connection;

thread_local! {
    static TRACK_ALLOCATIONS: Cell<bool> = const { Cell::new(false) };
    static ALLOCATED_BYTES: Cell<usize> = const { Cell::new(0) };
}

struct ThreadCountingAllocator;

#[global_allocator]
static ALLOCATOR: ThreadCountingAllocator = ThreadCountingAllocator;

unsafe impl GlobalAlloc for ThreadCountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        record_allocation(layout.size());
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc_zeroed(layout) };
        record_allocation(layout.size());
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) };
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let result = unsafe { System.realloc(pointer, layout, new_size) };
        if !result.is_null() {
            record_allocation(new_size);
        }
        result
    }
}

fn record_allocation(bytes: usize) {
    let _ = TRACK_ALLOCATIONS.try_with(|tracking| {
        if tracking.get() {
            let _ = ALLOCATED_BYTES.try_with(|allocated| {
                allocated.set(allocated.get().saturating_add(bytes));
            });
        }
    });
}

struct AllocationTrackingGuard;

impl Drop for AllocationTrackingGuard {
    fn drop(&mut self) {
        let _ = TRACK_ALLOCATIONS.try_with(|tracking| tracking.set(false));
    }
}

async fn measure_summary_read_allocations(store: &MemoryTaskStore, id: TaskId) -> usize {
    ALLOCATED_BYTES.with(|allocated| allocated.set(0));
    TRACK_ALLOCATIONS.with(|tracking| tracking.set(true));
    let _guard = AllocationTrackingGuard;
    let _summary = store.get_summary(id).await.unwrap();
    drop(_guard);
    ALLOCATED_BYTES.with(Cell::get)
}

async fn measure_encoded_read_allocations(store: &MemoryTaskStore, id: TaskId) -> usize {
    ALLOCATED_BYTES.with(|allocated| allocated.set(0));
    TRACK_ALLOCATIONS.with(|tracking| tracking.set(true));
    let _guard = AllocationTrackingGuard;
    let _task = store.get_encoded_task(id).await.unwrap();
    drop(_guard);
    ALLOCATED_BYTES.with(Cell::get)
}

fn task_id(value: u64) -> TaskId {
    TaskId::from_id(Id::new(value))
}

fn request(payload_size: usize) -> StoredTaskRequest {
    StoredTaskRequest {
        kind_id: "summary.read.test".to_owned(),
        category: None,
        payload: StoredPayload {
            type_id: ModelIdBuf::parse("qubit_task.tests.Payload").expect("valid model ID"),
            schema_version: 1,
            codec_id: "qubit.bytes.test".to_owned(),
            bytes: vec![0x5a; payload_size],
        },
        metadata: Metadata::new(),
        resource_limit: Default::default(),
        correlation_key: None,
        idempotency_key: None,
    }
}

#[tokio::test(flavor = "current_thread")]
async fn test_memory_summary_reads_track_updates_without_cloning_payload() {
    let store = MemoryTaskStore::new(4);
    let id = task_id(301);
    let accepted = store.accept_encoded(id, request(8 * 1024 * 1024)).await.unwrap();
    let initial = store.get_summary(id).await.unwrap().unwrap();
    assert_eq!(initial, accepted.summary);

    let small_id = task_id(304);
    store.accept_encoded(small_id, request(1)).await.unwrap();
    // Warm both paths before measuring so one-time runtime setup is excluded.
    store.get_summary(small_id).await.unwrap();
    store.get_summary(id).await.unwrap();
    let small_read_bytes = measure_summary_read_allocations(&store, small_id).await;
    let large_read_bytes = measure_summary_read_allocations(&store, id).await;
    let encoded_read_bytes = measure_encoded_read_allocations(&store, id).await;
    assert!(
        small_read_bytes < 32 * 1024,
        "small summary read allocated {small_read_bytes} bytes"
    );
    assert!(
        large_read_bytes <= small_read_bytes + 32 * 1024,
        "8 MiB payload summary read allocated {large_read_bytes} bytes versus {small_read_bytes} for a 1-byte payload",
    );
    assert!(large_read_bytes < 8 * 1024 * 1024 / 16);
    assert!(
        encoded_read_bytes >= 8 * 1024 * 1024 / 2,
        "allocator control failed to observe payload clone: only {encoded_read_bytes} bytes allocated",
    );

    let running = store
        .start_encoded(StartCommand {
            id,
            expected_state_version: initial.state_version,
            started_at_ms: 10,
        })
        .await
        .unwrap();
    let updated = store
        .update_progress(qubit_task::model::typed::ProgressCommand::new(
            id,
            running.attempt,
            1,
            None,
            Vec::new(),
            11,
        ))
        .await
        .unwrap();
    assert_eq!(store.get_summary(id).await.unwrap(), Some(updated.clone()));
    assert_eq!(store.get_summary(task_id(302)).await.unwrap(), None);

    let terminal = store
        .transition_encoded(TransitionCommand {
            id,
            expected_state_version: updated.state_version,
            expected_attempt: updated.attempt,
            retry_not_before_ms: None,
            state: TaskState::Failed {
                category: "test".to_owned(),
                message: "failed".to_owned(),
            },
            cancel_requested: false,
            cancel_error: None,
            finished_at_ms: Some(12),
            output: None,
        })
        .await
        .unwrap();
    assert_eq!(store.get_summary(id).await.unwrap(), Some(terminal));
    store
        .prune_terminal_before(13, NonZeroUsize::new(1).unwrap())
        .await
        .unwrap();
    assert_eq!(store.get_summary(id).await.unwrap(), None);
    assert_eq!(store.get_state_checkpoint(id).await.unwrap(), None);
}

#[tokio::test]
async fn test_memory_summary_reads_return_none_after_terminal_eviction() {
    let store = MemoryTaskStore::new(0);
    let id = task_id(303);
    let accepted = store.accept_encoded(id, request(1)).await.unwrap();
    let running = store
        .start_encoded(StartCommand {
            id,
            expected_state_version: accepted.summary.state_version,
            started_at_ms: 20,
        })
        .await
        .unwrap();
    store
        .transition_encoded(TransitionCommand {
            id,
            expected_state_version: running.state_version,
            expected_attempt: running.attempt,
            retry_not_before_ms: None,
            state: TaskState::Succeeded,
            cancel_requested: false,
            cancel_error: None,
            finished_at_ms: Some(21),
            output: None,
        })
        .await
        .unwrap();

    assert_eq!(store.get_summary(id).await.unwrap(), None);
}

#[tokio::test]
#[cfg(feature = "sqlite")]
async fn test_sqlite_summary_reads_do_not_select_payload() {
    let path = std::env::temp_dir().join(format!(
        "qubit-task-summary-read-{}-{}.sqlite",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    ));
    let store = SqliteTaskStore::open(&path).unwrap();
    let id = task_id(401);
    let accepted = store.accept_encoded(id, request(1024)).await.unwrap();
    Connection::open(&path)
        .unwrap()
        .execute(
            "UPDATE tasks SET payload='not a blob' WHERE id=?1",
            [id.to_padded_decimal()],
        )
        .unwrap();

    assert_eq!(store.get_summary(id).await.unwrap(), Some(accepted.summary));
    assert!(store.get_encoded_task(id).await.is_err());
    assert_eq!(store.get_summary(task_id(402)).await.unwrap(), None);
    let running = store
        .start_encoded(StartCommand {
            id,
            expected_state_version: 0,
            started_at_ms: 20,
        })
        .await
        .unwrap();
    store
        .transition_encoded(TransitionCommand {
            id,
            expected_state_version: running.state_version,
            expected_attempt: running.attempt,
            retry_not_before_ms: None,
            state: TaskState::Succeeded,
            cancel_requested: false,
            cancel_error: None,
            finished_at_ms: Some(21),
            output: None,
        })
        .await
        .unwrap();
    store
        .prune_terminal_before(22, NonZeroUsize::new(1).unwrap())
        .await
        .unwrap();
    assert_eq!(store.get_summary(id).await.unwrap(), None);
    assert!(store.get_state_checkpoint(id).await.unwrap().is_some());
    drop(store);
    let _ = std::fs::remove_file(&path);
    let mut lock_path = path.as_os_str().to_owned();
    lock_path.push(".owner.lock");
    let _ = std::fs::remove_file(std::path::PathBuf::from(lock_path));
}
