/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Tests for [`TaskExecutionService`](qubit_task::service::TaskExecutionService).

use std::{
    io,
    sync::{Arc, mpsc},
    thread,
    time::Duration,
};

use qubit_executor::TaskExecutionError;
use qubit_executor::service::RejectedExecution;
use qubit_thread_pool::{ThreadPool, ThreadPoolBuildError};
use qubit_task::service::{TaskExecutionService, TaskExecutionServiceError, TaskStatus};

/// Creates a current-thread Tokio runtime for driving async termination APIs.
fn create_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime for task execution service tests")
}

/// Creates a service backed by a single-worker thread pool.
fn create_single_worker_service() -> TaskExecutionService {
    TaskExecutionService::builder()
        .thread_pool(
            ThreadPool::builder()
                .pool_size(1)
                .queue_capacity(2)
                .keep_alive(Duration::from_millis(50)),
        )
        .build()
        .expect("task execution service should be created")
}

/// Waits until a blocking task reports that it has started.
fn wait_started(receiver: mpsc::Receiver<()>) {
    receiver
        .recv_timeout(Duration::from_secs(1))
        .expect("task should start within timeout");
}

#[test]
fn test_task_execution_service_tracks_successful_task() {
    let service = TaskExecutionService::new().expect("service should be created");

    let handle = service
        .submit_callable(1, || Ok::<usize, io::Error>(42))
        .expect("service should accept task");

    assert_eq!(handle.get().expect("task should succeed"), 42);
    assert_eq!(service.status(1), Some(TaskStatus::Succeeded));
    assert_eq!(service.stats().succeeded, 1);
    service.shutdown();
    create_runtime().block_on(service.await_termination());
}

#[test]
fn test_task_status_is_active_only_for_in_flight_states() {
    assert!(TaskStatus::Submitted.is_active());
    assert!(TaskStatus::Running.is_active());
    assert!(!TaskStatus::Succeeded.is_active());
    assert!(!TaskStatus::Failed.is_active());
    assert!(!TaskStatus::Panicked.is_active());
    assert!(!TaskStatus::Cancelled.is_active());
}

#[test]
fn test_task_execution_service_cancel_unknown_and_terminal_tasks() {
    let service = TaskExecutionService::new().expect("service should be created");

    assert_eq!(service.status(404), None);
    assert!(!service.cancel(404));

    let handle = service
        .submit_callable(1, || Ok::<usize, io::Error>(42))
        .expect("service should accept task");
    assert_eq!(handle.get().expect("task should succeed"), 42);

    assert_eq!(service.status(1), Some(TaskStatus::Succeeded));
    assert!(!service.cancel(1));
    service.shutdown();
    create_runtime().block_on(service.await_termination());
}

#[test]
fn test_task_execution_service_builder_propagates_pool_build_error() {
    let result = TaskExecutionService::builder()
        .thread_pool(ThreadPool::builder().pool_size(0))
        .build();

    assert!(matches!(
        result,
        Err(ThreadPoolBuildError::ZeroMaximumPoolSize),
    ));
}

#[test]
fn test_task_execution_service_tracks_failure_and_panic() {
    let service = TaskExecutionService::new().expect("service should be created");

    let failed = service
        .submit_callable(1, || Err::<(), _>(io::Error::other("failed")))
        .expect("service should accept failing task");
    let panicked = service
        .submit(2, || -> Result<(), io::Error> { panic!("boom") })
        .expect("service should accept panicking task");

    assert!(matches!(failed.get(), Err(TaskExecutionError::Failed(_)),));
    assert!(matches!(panicked.get(), Err(TaskExecutionError::Panicked)));
    assert_eq!(service.status(1), Some(TaskStatus::Failed));
    assert_eq!(service.status(2), Some(TaskStatus::Panicked));
    let stats = service.stats();
    assert_eq!(stats.failed, 1);
    assert_eq!(stats.panicked, 1);
    service.shutdown();
    create_runtime().block_on(service.await_termination());
}

#[test]
fn test_task_execution_service_rejects_duplicate_task_id() {
    let service = create_single_worker_service();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();

    let first = service
        .submit(1, move || {
            started_tx
                .send(())
                .expect("test should receive task start signal");
            release_rx
                .recv()
                .map_err(|err| io::Error::other(err.to_string()))?;
            Ok::<(), io::Error>(())
        })
        .expect("first task should be accepted");
    wait_started(started_rx);

    let duplicate = service.submit(1, || Ok::<(), io::Error>(()));

    assert!(matches!(
        duplicate,
        Err(TaskExecutionServiceError::DuplicateTask(1)),
    ));
    release_tx
        .send(())
        .expect("blocking task should receive release signal");
    first.get().expect("first task should complete");
    service.shutdown();
    create_runtime().block_on(service.await_termination());
}

#[test]
fn test_task_execution_service_suspend_rejects_new_tasks() {
    let service = TaskExecutionService::new().expect("service should be created");

    assert!(!service.is_suspended());
    service.suspend();
    assert!(service.is_suspended());
    let rejected = service.submit(1, || Ok::<(), io::Error>(()));
    service.resume();
    assert!(!service.is_suspended());
    let accepted = service
        .submit(1, || Ok::<(), io::Error>(()))
        .expect("service should accept after resume");

    assert!(matches!(
        rejected,
        Err(TaskExecutionServiceError::Suspended),
    ));
    accepted.get().expect("accepted task should complete");
    service.shutdown();
    create_runtime().block_on(service.await_termination());
}

#[test]
fn test_task_execution_service_waits_for_snapshot_and_idle() {
    let service = Arc::new(create_single_worker_service());
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();

    let first = service
        .submit(1, move || {
            started_tx
                .send(())
                .expect("test should receive task start signal");
            release_rx
                .recv()
                .map_err(|err| io::Error::other(err.to_string()))?;
            Ok::<(), io::Error>(())
        })
        .expect("first task should be accepted");
    wait_started(started_rx);
    let second = service
        .submit_callable(2, || Ok::<usize, io::Error>(42))
        .expect("queued task should be accepted");

    let stats = service.stats();
    assert_eq!(stats.running, 1);
    assert_eq!(stats.submitted, 1);
    assert_eq!(service.thread_pool().stats().queued_tasks, 1);

    let (snapshot_done_tx, snapshot_done_rx) = mpsc::channel();
    let snapshot_service = Arc::clone(&service);
    let snapshot_waiter = thread::spawn(move || {
        snapshot_service.await_in_flight_tasks_completion();
        snapshot_done_tx
            .send(())
            .expect("test should receive snapshot completion");
    });
    let (idle_done_tx, idle_done_rx) = mpsc::channel();
    let idle_service = Arc::clone(&service);
    let idle_waiter = thread::spawn(move || {
        idle_service.await_idle();
        idle_done_tx
            .send(())
            .expect("test should receive idle completion");
    });

    assert!(
        snapshot_done_rx
            .recv_timeout(Duration::from_millis(30))
            .is_err()
    );
    assert!(
        idle_done_rx
            .recv_timeout(Duration::from_millis(30))
            .is_err()
    );
    release_tx
        .send(())
        .expect("blocking task should receive release signal");
    first.get().expect("first task should complete");
    assert_eq!(second.get().expect("queued task should run"), 42);
    snapshot_done_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("snapshot waiter should finish");
    idle_done_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("idle waiter should finish");
    snapshot_waiter
        .join()
        .expect("snapshot waiter should not panic");
    idle_waiter.join().expect("idle waiter should not panic");

    service.shutdown();
    create_runtime().block_on(service.await_termination());
}

#[test]
fn test_task_execution_service_shutdown_now_cancels_queued_task() {
    let service = create_single_worker_service();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();

    let first = service
        .submit(1, move || {
            started_tx
                .send(())
                .expect("test should receive task start signal");
            release_rx
                .recv()
                .map_err(|err| io::Error::other(err.to_string()))?;
            Ok::<(), io::Error>(())
        })
        .expect("first task should be accepted");
    wait_started(started_rx);
    let queued = service
        .submit_callable(2, || Ok::<usize, io::Error>(42))
        .expect("queued task should be accepted");

    let report = service.shutdown_now();

    assert_eq!(report.queued, 1);
    assert!(service.is_shutdown());
    assert_eq!(service.status(2), Some(TaskStatus::Cancelled));
    assert!(matches!(queued.get(), Err(TaskExecutionError::Cancelled)));
    assert!(!service.is_terminated());
    release_tx
        .send(())
        .expect("blocking task should receive release signal");
    first.get().expect("first task should complete");
    create_runtime().block_on(service.await_termination());
    assert!(service.is_terminated());
}

#[test]
fn test_task_execution_service_removes_record_when_pool_rejects() {
    let service = TaskExecutionService::builder()
        .thread_pool(ThreadPool::builder().pool_size(1).stack_size(usize::MAX))
        .build()
        .expect("service should be created with lazy worker spawning");

    let result = service.submit(1, || Ok::<(), io::Error>(()));

    assert!(matches!(
        result,
        Err(TaskExecutionServiceError::Rejected(
            RejectedExecution::WorkerSpawnFailed { .. },
        )),
    ));
    assert_eq!(service.status(1), None);
    service.shutdown();
    create_runtime().block_on(service.await_termination());
}

#[test]
fn test_task_execution_service_cancels_queued_task() {
    let service = create_single_worker_service();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();

    let first = service
        .submit(1, move || {
            started_tx
                .send(())
                .expect("test should receive task start signal");
            release_rx
                .recv()
                .map_err(|err| io::Error::other(err.to_string()))?;
            Ok::<(), io::Error>(())
        })
        .expect("first task should be accepted");
    wait_started(started_rx);
    let queued = service
        .submit_callable(2, || Ok::<usize, io::Error>(42))
        .expect("queued task should be accepted");

    assert!(service.cancel(2));
    assert_eq!(service.status(2), Some(TaskStatus::Cancelled));
    assert!(matches!(queued.get(), Err(TaskExecutionError::Cancelled)));
    release_tx
        .send(())
        .expect("blocking task should receive release signal");
    first.get().expect("first task should complete");
    service.await_idle();
    assert_eq!(service.stats().cancelled, 1);
    service.shutdown();
    create_runtime().block_on(service.await_termination());
}
