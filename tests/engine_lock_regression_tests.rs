// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::process::Command;
use std::process::Stdio;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use qubit_task::engine::LocalTaskExecutionEngine;
use qubit_task::engine::TaskExecutionEngine;
use qubit_task::model::ResourceCapacity;
use qubit_task::model::ResourceRequest;
use qubit_task::model::TaskId;

const LOCK_REGRESSION_CHILD: &str = "RS_TASK_LOCK_REGRESSION_CHILD";

/// Runs the deadlock reproduction in a child process so a deadlocked mutex
/// cannot hang the whole test suite.
#[test]
fn test_local_engine_parallel_prepare_drop_has_no_lock_inversion() {
    if std::env::var_os(LOCK_REGRESSION_CHILD).is_some() {
        run_prepare_drop_stress();
        return;
    }

    let executable = std::env::current_exe().expect("test executable path is available");
    let mut child = Command::new(executable)
        .arg("--exact")
        .arg("test_local_engine_parallel_prepare_drop_has_no_lock_inversion")
        .arg("--nocapture")
        .env(LOCK_REGRESSION_CHILD, "1")
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("lock regression child starts");
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(status) = child.try_wait().expect("child status is readable") {
            assert!(status.success(), "parallel prepare/drop child failed: {status}");
            return;
        }
        if Instant::now() >= deadline {
            child.kill().expect("timed out child is stopped");
            let status = child.wait().expect("stopped child is reaped");
            panic!("parallel prepare/drop child deadlocked or exceeded 15 seconds: {status}");
        }
        thread::sleep(Duration::from_millis(20));
    }
}

/// Repeatedly reserves and drops CPU resources from competing OS threads.
fn run_prepare_drop_stress() {
    let engine = Arc::new(LocalTaskExecutionEngine::new(ResourceCapacity {
        cpu_slots: u32::MAX,
        ..ResourceCapacity::default()
    }));
    let workers = (0..8)
        .map(|_| {
            let engine = Arc::clone(&engine);
            thread::spawn(move || {
                let id = TaskId::generate();
                for _ in 0..10_000 {
                    let prepared = engine
                        .try_prepare(
                            id,
                            ResourceRequest {
                                cpu_slots: 1,
                                ..ResourceRequest::default()
                            },
                        )
                        .expect("reservation fits the unbounded test capacity");
                    thread::yield_now();
                    drop(prepared);
                }
            })
        })
        .collect::<Vec<_>>();
    for worker in workers {
        worker.join().expect("resource worker completes");
    }
    assert_eq!(engine.capacity().used_cpu_slots, 0);
}
