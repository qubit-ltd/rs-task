// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for [`TaskExecutionService`](qubit_task::service::TaskExecutionService).

use std::future::Future;
use std::future::IntoFuture;
use std::io;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;
use std::sync::Arc;
use std::sync::Barrier;
use std::sync::mpsc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::thread;
use std::time::Duration;

use qubit_executor::TaskExecutionError;
use qubit_executor::service::ExecutorServiceBuilderError;
use qubit_executor::service::SubmissionError;
use qubit_id::Id;
use qubit_task::service::TaskExecutionService;
use qubit_task::service::TaskExecutionServiceError;
use qubit_task::service::TaskStatus;
use qubit_thread_pool::ThreadPool;

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

/// Returns a successful unit task result.
fn successful_unit_task() -> Result<(), io::Error> {
    Ok(())
}

/// Returns a successful numeric task result.
fn successful_usize_task() -> Result<usize, io::Error> {
    Ok(42)
}

#[test]
fn test_task_execution_service_tracks_successful_task() {
    let service = TaskExecutionService::new().expect("service should be created");

    let handle = service
        .submit_callable(Id::new(1), successful_usize_task as fn() -> Result<usize, io::Error>)
        .expect("service should accept task");

    assert_eq!(handle.get().expect("task should succeed"), 42);
    assert_eq!(service.status(Id::new(1)), Some(TaskStatus::Succeeded));
    assert_eq!(service.stats().succeeded, 1);
    service.shutdown();
    service.wait_termination();
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

    assert_eq!(service.status(Id::new(404)), None);
    assert!(!service.cancel(Id::new(404)));

    let handle = service
        .submit_callable(Id::new(1), successful_usize_task as fn() -> Result<usize, io::Error>)
        .expect("service should accept task");
    assert_eq!(handle.get().expect("task should succeed"), 42);

    assert_eq!(service.status(Id::new(1)), Some(TaskStatus::Succeeded));
    assert!(!service.cancel(Id::new(1)));
    service.shutdown();
    service.wait_termination();
}

#[test]
fn test_task_execution_service_cancel_running_task_returns_false() {
    let service = create_single_worker_service();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();

    let handle = service
        .submit(Id::new(1), move || {
            started_tx.send(()).expect("test should receive task start signal");
            release_rx.recv().map_err(|err| io::Error::other(err.to_string()))?;
            Ok::<(), io::Error>(())
        })
        .expect("running task should be accepted");
    wait_started(started_rx);

    assert_eq!(service.status(Id::new(1)), Some(TaskStatus::Running));
    assert!(!service.cancel(Id::new(1)));
    assert_eq!(service.status(Id::new(1)), Some(TaskStatus::Running));
    release_tx.send(()).expect("running task should receive release signal");
    handle.get().expect("running task should complete");
    assert_eq!(service.status(Id::new(1)), Some(TaskStatus::Succeeded));
    service.shutdown();
    service.wait_termination();
}

#[test]
fn test_task_execution_service_builder_propagates_pool_build_error() {
    let result = TaskExecutionService::builder()
        .thread_pool(ThreadPool::builder().pool_size(0))
        .build();

    assert!(matches!(result, Err(ExecutorServiceBuilderError::ZeroMaximumPoolSize),));
}

#[test]
fn test_task_execution_service_tracks_failure_and_panic() {
    let service = TaskExecutionService::new().expect("service should be created");

    let failed = service
        .submit_callable(Id::new(1), || Err::<(), _>(io::Error::other("failed")))
        .expect("service should accept failing task");
    let panicked = service
        .submit(Id::new(2), || -> Result<(), io::Error> { panic!("boom") })
        .expect("service should accept panicking task");

    assert!(matches!(failed.get(), Err(TaskExecutionError::Failed(_)),));
    assert!(matches!(panicked.get(), Err(TaskExecutionError::Panicked)));
    assert_eq!(service.status(Id::new(1)), Some(TaskStatus::Failed));
    assert_eq!(service.status(Id::new(2)), Some(TaskStatus::Panicked));
    let stats = service.stats();
    assert_eq!(stats.failed, 1);
    assert_eq!(stats.panicked, 1);
    service.shutdown();
    service.wait_termination();
}

#[test]
fn test_task_execution_service_rejects_duplicate_task_id() {
    let service = create_single_worker_service();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();

    let first = service
        .submit(Id::new(1), move || {
            started_tx.send(()).expect("test should receive task start signal");
            release_rx.recv().map_err(|err| io::Error::other(err.to_string()))?;
            Ok::<(), io::Error>(())
        })
        .expect("first task should be accepted");
    wait_started(started_rx);

    let duplicate = service.submit(Id::new(1), successful_unit_task as fn() -> Result<(), io::Error>);

    assert!(matches!(duplicate, Err(TaskExecutionServiceError::DuplicateTask(actual)) if actual == Id::new(1)));
    release_tx
        .send(())
        .expect("blocking task should receive release signal");
    first.get().expect("first task should complete");
    service.shutdown();
    service.wait_termination();
}

#[test]
fn test_task_execution_service_reuses_terminal_task_id() {
    let service = create_single_worker_service();
    let first = service
        .submit_callable(Id::new(7), successful_usize_task as fn() -> Result<usize, io::Error>)
        .expect("first task should be accepted");
    assert_eq!(first.get().expect("first task should finish"), 42);

    let second = service
        .submit_callable(Id::new(7), successful_usize_task as fn() -> Result<usize, io::Error>)
        .expect("completed task ID should be reusable");
    assert_eq!(second.get().expect("second task should finish"), 42);
    service.shutdown();
    service.wait_termination();
}

#[test]
fn test_task_execution_service_zero_history_drops_terminal_status() {
    let service = TaskExecutionService::builder()
        .completed_history_capacity(0)
        .build()
        .expect("service should be created");
    let first = service
        .submit_callable(Id::new(7), successful_usize_task as fn() -> Result<usize, io::Error>)
        .expect("first task should be accepted");
    assert_eq!(first.get().expect("first task should finish"), 42);
    assert_eq!(service.status(Id::new(7)), None);
    assert_eq!(service.stats().total, 0);
    let second = service
        .submit(Id::new(7), successful_unit_task as fn() -> Result<(), io::Error>)
        .expect("terminal ID should be reusable without history");
    second.get().expect("second task should finish");
    service.shutdown();
    service.wait_termination();
}

#[test]
fn test_task_execution_service_suspend_rejects_new_tasks() {
    let service = TaskExecutionService::new().expect("service should be created");

    assert!(!service.is_suspended());
    service.suspend();
    assert!(service.is_suspended());
    let rejected = service.submit(Id::new(1), successful_unit_task as fn() -> Result<(), io::Error>);
    service.resume();
    assert!(!service.is_suspended());
    let accepted = service
        .submit(Id::new(1), successful_unit_task as fn() -> Result<(), io::Error>)
        .expect("service should accept after resume");

    assert!(matches!(rejected, Err(TaskExecutionServiceError::Suspended),));
    accepted.get().expect("accepted task should complete");
    service.shutdown();
    service.wait_termination();
}

#[test]
fn test_task_execution_service_waits_for_snapshot_and_idle() {
    let service = Arc::new(create_single_worker_service());
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();

    let first = service
        .submit(Id::new(1), move || {
            started_tx.send(()).expect("test should receive task start signal");
            release_rx.recv().map_err(|err| io::Error::other(err.to_string()))?;
            Ok::<(), io::Error>(())
        })
        .expect("first task should be accepted");
    wait_started(started_rx);
    let second = service
        .submit_callable(Id::new(2), successful_usize_task as fn() -> Result<usize, io::Error>)
        .expect("queued task should be accepted");

    let stats = service.stats();
    assert_eq!(stats.running, 1);
    assert_eq!(stats.submitted, 1);
    assert_eq!(service.thread_pool_stats().queued_tasks, 1);

    let (snapshot_done_tx, snapshot_done_rx) = mpsc::channel();
    let snapshot_service = Arc::clone(&service);
    let snapshot_waiter = thread::spawn(move || {
        snapshot_service.wait_for_current_tasks();
        snapshot_done_tx
            .send(())
            .expect("test should receive snapshot completion");
    });
    let (idle_done_tx, idle_done_rx) = mpsc::channel();
    let idle_service = Arc::clone(&service);
    let idle_waiter = thread::spawn(move || {
        idle_service.wait_for_idle();
        idle_done_tx.send(()).expect("test should receive idle completion");
    });

    assert!(snapshot_done_rx.recv_timeout(Duration::from_millis(30)).is_err());
    assert!(idle_done_rx.recv_timeout(Duration::from_millis(30)).is_err());
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
    snapshot_waiter.join().expect("snapshot waiter should not panic");
    idle_waiter.join().expect("idle waiter should not panic");

    service.shutdown();
    service.wait_termination();
}

#[test]
fn test_task_execution_service_wait_methods_return_when_no_tasks_are_active() {
    let service = TaskExecutionService::new().expect("service should be created");

    service.wait_for_current_tasks();
    service.wait_for_idle();

    let stats = service.stats();
    assert_eq!(stats.total, 0);
    assert_eq!(service.status(Id::new(1)), None);
    service.shutdown();
    service.wait_termination();
}

#[test]
fn test_task_execution_service_stop_cancels_queued_task() {
    let service = create_single_worker_service();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();

    let first = service
        .submit(Id::new(1), move || {
            started_tx.send(()).expect("test should receive task start signal");
            release_rx.recv().map_err(|err| io::Error::other(err.to_string()))?;
            Ok::<(), io::Error>(())
        })
        .expect("first task should be accepted");
    wait_started(started_rx);
    let queued = service
        .submit_callable(Id::new(2), successful_usize_task as fn() -> Result<usize, io::Error>)
        .expect("queued task should be accepted");

    let report = service.stop();

    assert_eq!(report.queued, 1);
    assert!(service.is_not_running());
    assert_eq!(service.status(Id::new(2)), Some(TaskStatus::Cancelled));
    assert!(matches!(queued.get(), Err(TaskExecutionError::Cancelled)));
    assert!(!service.is_terminated());
    release_tx
        .send(())
        .expect("blocking task should receive release signal");
    first.get().expect("first task should complete");
    service.wait_termination();
    assert!(service.is_terminated());
}

#[test]
fn test_task_execution_service_cancel_and_stop_race_keeps_terminal_status() {
    let service = Arc::new(create_single_worker_service());
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let running = service
        .submit(Id::new(1), move || {
            started_tx.send(()).expect("start signal should send");
            release_rx.recv().map_err(|error| io::Error::other(error.to_string()))?;
            Ok::<(), io::Error>(())
        })
        .expect("running task should be accepted");
    wait_started(started_rx);
    let queued = service
        .submit(Id::new(2), successful_unit_task as fn() -> Result<(), io::Error>)
        .expect("queued task should be accepted");

    let gate = Arc::new(Barrier::new(3));
    let cancel_service = Arc::clone(&service);
    let cancel_gate = Arc::clone(&gate);
    let cancel_thread = thread::spawn(move || {
        cancel_gate.wait();
        cancel_service.cancel(Id::new(2))
    });
    let stop_service = Arc::clone(&service);
    let stop_gate = Arc::clone(&gate);
    let stop_thread = thread::spawn(move || {
        stop_gate.wait();
        stop_service.stop()
    });
    gate.wait();
    let cancelled = cancel_thread.join().expect("cancel thread should not panic");
    let report = stop_thread.join().expect("stop thread should not panic");
    assert_eq!(usize::from(cancelled) + report.queued, 1);
    assert_eq!(service.thread_pool_stats().cancelled_tasks, 1);

    assert_eq!(service.status(Id::new(2)), Some(TaskStatus::Cancelled));
    assert!(matches!(queued.get(), Err(TaskExecutionError::Cancelled)));
    release_tx.send(()).expect("running task should be released");
    running.get().expect("running task should complete");
    service.wait_termination();
}

#[test]
fn test_task_execution_service_removes_record_when_pool_rejects() {
    let service = TaskExecutionService::builder()
        .thread_pool(ThreadPool::builder().pool_size(1).stack_size(usize::MAX))
        .build()
        .expect("service should be created with lazy worker spawning");

    let result = service.submit(Id::new(1), successful_unit_task as fn() -> Result<(), io::Error>);

    assert!(matches!(
        result,
        Err(TaskExecutionServiceError::Rejected(
            SubmissionError::WorkerSpawnFailed { .. },
        )),
    ));
    assert_eq!(service.status(Id::new(1)), None);
    service.shutdown();
    service.wait_termination();
}

#[test]
fn test_task_execution_service_preserves_terminal_status_when_reused_id_is_rejected() {
    let service = TaskExecutionService::new().expect("service should be created");
    let id = Id::new(1);
    service
        .submit(id, successful_unit_task as fn() -> Result<(), io::Error>)
        .expect("first task should be accepted")
        .get()
        .expect("first task should complete");

    service.shutdown();
    let rejected = service.submit(id, successful_unit_task as fn() -> Result<(), io::Error>);

    assert!(matches!(rejected, Err(TaskExecutionServiceError::Rejected(_))));
    assert_eq!(service.status(id), Some(TaskStatus::Succeeded));
    assert_eq!(service.stats().succeeded, 1);
    service.wait_termination();
}

#[test]
fn test_task_execution_service_cancels_queued_task() {
    let service = create_single_worker_service();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();

    let first = service
        .submit(Id::new(1), move || {
            started_tx.send(()).expect("test should receive task start signal");
            release_rx.recv().map_err(|err| io::Error::other(err.to_string()))?;
            Ok::<(), io::Error>(())
        })
        .expect("first task should be accepted");
    wait_started(started_rx);
    let queued = service
        .submit_callable(Id::new(2), successful_usize_task as fn() -> Result<usize, io::Error>)
        .expect("queued task should be accepted");

    assert!(service.cancel(Id::new(2)));
    assert_eq!(service.status(Id::new(2)), Some(TaskStatus::Cancelled));
    assert!(matches!(queued.get(), Err(TaskExecutionError::Cancelled)));
    release_tx
        .send(())
        .expect("blocking task should receive release signal");
    first.get().expect("first task should complete");
    service.wait_for_idle();
    assert_eq!(service.stats().cancelled, 1);
    service.shutdown();
    service.wait_termination();
}

/// Records when a queued callable releases its captured resource.
struct DropProbe(Arc<AtomicUsize>);

impl Drop for DropProbe {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn test_task_execution_service_cancel_releases_capture_and_queue_capacity() {
    let service = TaskExecutionService::builder()
        .thread_pool(ThreadPool::builder().pool_size(1).queue_capacity(1))
        .build()
        .expect("service should build");
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let running = service.submit(Id::new(1), move || {
        started_tx.send(()).expect("worker start should send");
        release_rx.recv().expect("worker should be released");
        Ok::<(), io::Error>(())
    }).expect("blocking task should be accepted");
    wait_started(started_rx);
    let drops = Arc::new(AtomicUsize::new(0));
    let probe = DropProbe(Arc::clone(&drops));
    let queued = service.submit(Id::new(2), move || {
        let _probe = &probe;
        Ok::<(), io::Error>(())
    }).expect("queued task should be accepted");
    assert!(matches!(service.submit(Id::new(3), successful_unit_task as fn() -> Result<(), io::Error>),
        Err(TaskExecutionServiceError::Rejected(SubmissionError::Saturated))));

    let cancelled = service.cancel(Id::new(2));
    let released_before_return = drops.load(Ordering::SeqCst);
    let queued_after_cancel = service.thread_pool_stats().queued_tasks;
    let replacement = service.submit(Id::new(3), successful_unit_task as fn() -> Result<(), io::Error>);
    // Release the worker before asserting so a regression cannot strand it.
    release_tx.send(()).expect("worker should be released");
    running.get().expect("blocking task should finish");
    assert!(cancelled);
    assert!(matches!(queued.get(), Err(TaskExecutionError::Cancelled)));
    assert_eq!(released_before_return, 1, "cancel must release the callable before returning");
    assert_eq!(queued_after_cancel, 0, "cancel must remove its queue entry");
    replacement.expect("cancel must free queue capacity").get().expect("replacement should run");
    assert!(!service.cancel(Id::new(2)));
    service.shutdown();
    service.wait_termination();
    let pool_stats = service.thread_pool_stats();
    assert_eq!(pool_stats.cancelled_tasks, 1);
    assert_eq!(pool_stats.completed_tasks, 2);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn test_task_execution_service_cancel_and_start_choose_one_terminal_result() {
    for _ in 0..32 {
        let service = Arc::new(create_single_worker_service());
        let gate = Arc::new(Barrier::new(2));
        let worker_gate = Arc::clone(&gate);
        let (started_tx, started_rx) = mpsc::channel();
        let running = service.submit(Id::new(1), move || {
            started_tx.send(()).expect("worker start should send");
            worker_gate.wait();
            Ok::<(), io::Error>(())
        }).expect("blocking task should be accepted");
        wait_started(started_rx);
        let calls = Arc::new(AtomicUsize::new(0));
        let task_calls = Arc::clone(&calls);
        let queued = service.submit(Id::new(2), move || {
            task_calls.fetch_add(1, Ordering::SeqCst);
            Ok::<(), io::Error>(())
        }).expect("queued task should be accepted");
        gate.wait();
        let cancelled = service.cancel(Id::new(2));
        running.get().expect("blocking task should finish");
        let result = queued.get();
        service.shutdown();
        service.wait_termination();
        if cancelled {
            assert!(matches!(result, Err(TaskExecutionError::Cancelled)));
            assert_eq!(service.status(Id::new(2)), Some(TaskStatus::Cancelled));
            assert_eq!(calls.load(Ordering::SeqCst), 0);
        } else {
            result.expect("worker that wins ownership should finish");
            assert_eq!(service.status(Id::new(2)), Some(TaskStatus::Succeeded));
            assert_eq!(calls.load(Ordering::SeqCst), 1);
        }
        assert!(!service.cancel(Id::new(2)));
        let stats = service.thread_pool_stats();
        assert_eq!(stats.cancelled_tasks, usize::from(cancelled));
        assert_eq!(stats.completed_tasks + stats.cancelled_tasks, 2);
        assert_eq!(stats.running_tasks + stats.queued_tasks, 0);
    }
}

/// Panics when cancellation publishes a result to an awaiting caller.
struct PanickingWake;

impl Wake for PanickingWake {
    fn wake(self: Arc<Self>) {
        panic!("test waker panic");
    }
}

#[test]
fn test_task_execution_service_cancel_publishing_panic_finishes_registry() {
    let service = create_single_worker_service();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let running = service.submit(Id::new(1), move || {
        started_tx.send(()).expect("worker start should send");
        release_rx.recv().expect("worker should be released");
        Ok::<(), io::Error>(())
    }).expect("blocking task should be accepted");
    wait_started(started_rx);
    let queued = service.submit(Id::new(2), successful_unit_task as fn() -> Result<(), io::Error>)
        .expect("queued task should be accepted");
    let mut future = std::pin::pin!(queued.into_future());
    let waker = Waker::from(Arc::new(PanickingWake));
    let mut context = Context::from_waker(&waker);
    assert!(future.as_mut().poll(&mut context).is_pending());

    let cancelled = catch_unwind(AssertUnwindSafe(|| service.cancel(Id::new(2))));
    let status = service.status(Id::new(2));
    release_tx.send(()).expect("worker should be released");
    running.get().expect("blocking task should finish");
    assert!(cancelled.expect("cancellation callback panic should be contained"));
    assert_eq!(status, Some(TaskStatus::Cancelled));
    assert!(matches!(future.as_mut().poll(&mut context), Poll::Ready(Err(TaskExecutionError::Cancelled))));
    service.wait_for_idle();
    service.shutdown();
    service.wait_termination();
    assert_eq!(service.thread_pool_stats().cancelled_tasks, 1);
}

/// Holds cancellation in captured-value destruction after terminal publication.
struct BlockingDrop {
    started: mpsc::Sender<()>,
    release: mpsc::Receiver<()>,
}

impl Drop for BlockingDrop {
    fn drop(&mut self) {
        self.started.send(()).expect("drop start should send");
        self.release.recv().expect("drop should be released");
    }
}

#[test]
fn test_task_execution_service_reused_id_survives_old_cancel_return() {
    let service = Arc::new(create_single_worker_service());
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let running = service.submit(Id::new(1), move || {
        started_tx.send(()).expect("worker start should send");
        release_rx.recv().expect("worker should be released");
        Ok::<(), io::Error>(())
    }).expect("blocking task should be accepted");
    wait_started(started_rx);
    let (drop_started_tx, drop_started_rx) = mpsc::channel();
    let (drop_release_tx, drop_release_rx) = mpsc::channel();
    let probe = BlockingDrop { started: drop_started_tx, release: drop_release_rx };
    let old = service.submit(Id::new(2), move || {
        let _probe = &probe;
        Ok::<(), io::Error>(())
    }).expect("old task should be accepted");
    let cancelling_service = Arc::clone(&service);
    let cancellation = thread::spawn(move || cancelling_service.cancel(Id::new(2)));
    let drop_started = drop_started_rx.recv_timeout(Duration::from_secs(1));
    // Always unblock both paths before reporting failure on the old implementation.
    if drop_started.is_err() {
        drop_release_tx.send(()).expect("drop should be released");
        release_tx.send(()).expect("worker should be released");
        running.get().expect("blocking task should finish");
        cancellation.join().expect("cancel thread should finish");
        panic!("cancel must destroy the old callable before returning");
    }
    assert!(old.is_done());
    assert!(matches!(old.get(), Err(TaskExecutionError::Cancelled)));
    assert_eq!(service.status(Id::new(2)), Some(TaskStatus::Cancelled));
    let replacement = service.submit_callable(Id::new(2), successful_usize_task as fn() -> Result<usize, io::Error>)
        .expect("terminal ID should be reusable during old cancellation cleanup");
    assert_eq!(service.status(Id::new(2)), Some(TaskStatus::Submitted));
    drop_release_tx.send(()).expect("drop should be released");
    assert!(cancellation.join().expect("cancel thread should finish"));
    assert_eq!(service.status(Id::new(2)), Some(TaskStatus::Submitted));
    release_tx.send(()).expect("worker should be released");
    running.get().expect("blocking task should finish");
    assert_eq!(replacement.get().expect("replacement should run"), 42);
    service.shutdown();
    service.wait_termination();
    assert_eq!(service.status(Id::new(2)), Some(TaskStatus::Succeeded));
    let stats = service.thread_pool_stats();
    assert_eq!(stats.cancelled_tasks, 1);
    assert_eq!(stats.completed_tasks, 2);
}
