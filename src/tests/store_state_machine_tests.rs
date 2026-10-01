// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
#[path = "support/store_reference_model.rs"]
mod store_reference_model;

use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::time::SystemTime;

use proptest::collection;
use proptest::strategy::Strategy;
use proptest::test_runner::Config;
use proptest::test_runner::FileFailurePersistence;
use proptest::test_runner::RngSeed;
use proptest::test_runner::TestCaseError;
use proptest::test_runner::TestRunner;
use store_reference_model::Command;
use store_reference_model::ErrorKind;
use store_reference_model::ReferenceModel;
use store_reference_model::STATES;
use store_reference_model::error_kind;
use store_reference_model::logical_id;
use store_reference_model::request;
use store_reference_model::stable;

use crate::model::AcceptOutcome;
use crate::model::TaskId;
use crate::model::TaskQuery;
use crate::model::TaskStateKind;
use crate::model::TaskSummary;
use crate::store::LegacyTaskStore as TaskStore;
use crate::store::MemoryTaskStore;
#[cfg(feature = "sqlite")]
use crate::store::SqliteTaskStore;

/// Uses an explicit temporary workspace, never source-relative proptest
/// artifacts.
fn artifact_root() -> PathBuf {
    let path = std::env::var_os("RS_TASK_MODEL_ARTIFACT_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("qubit-task-state-model"));
    std::fs::create_dir_all(&path).expect("create model artifact workspace");
    path
}

/// Produces shrinkable logical commands; edge legality is resolved by the live
/// oracle.
fn commands(maximum: usize) -> impl Strategy<Value = Vec<Command>> {
    let operation = proptest::prop_oneof![
        5 => (0_u8..8, 0_u8..3).prop_map(|(key, variant)| Command::Accept { key, variant }),
        7 => (0_usize..16, 0_u8..2, 0_u8..2, proptest::bool::ANY, 0_u8..7)
            .prop_map(|(id_index, version_delta, attempt_delta, legal, state)| Command::Transition {
                id_index, version_delta, attempt_delta, legal, state }),
        3 => (0_usize..16).prop_map(|id_index| Command::RequestCancel { id_index }),
        3 => (0_u8..8, 0_u8..3, 0_usize..16, proptest::sample::select(vec![0, 1, 2, 8, 256, 257]))
            .prop_map(|(filter, correlation, after_index, limit)| Command::List { filter, correlation, after_index, limit }),
        2 => (0_usize..16, 1_usize..5).prop_map(|(cutoff_selector, limit)| Command::Prune { cutoff_selector, limit }),
    ];
    collection::vec(operation, 1..=maximum)
}

/// Compares summaries and classifications, propagating readable command context
/// to shrinking.
fn compare_transition(
    actual: Result<TaskSummary, crate::store::StoreError>,
    expected: Result<TaskSummary, ErrorKind>,
) -> Result<(), TestCaseError> {
    match (actual, expected) {
        (Ok(actual), Ok(expected)) if stable(actual.clone()) == expected => Ok(()),
        (Err(actual), Err(expected)) if error_kind(&actual) == Some(expected) => Ok(()),
        (actual, expected) => Err(TestCaseError::fail(format!(
            "transition actual={actual:?}, expected={expected:?}"
        ))),
    }
}

/// Drives one fresh store and compares every command and retained record to the
/// oracle.
async fn run_trace(store: &dyn TaskStore, commands: &[Command]) -> Result<(), TestCaseError> {
    let mut model = ReferenceModel::default();
    for (step, command) in commands.iter().enumerate() {
        let context = |message: String| TestCaseError::fail(format!("step {step}: {command:?}: {message}"));
        match *command {
            Command::Accept { key, variant } => {
                let id = logical_id(model.ids.len() + 1);
                model.ids.push(id);
                let request = request(key, variant);
                let expected = model
                    .idempotency
                    .get(&key)
                    .and_then(|id| model.records.get(id))
                    .cloned();
                let actual = store.accept(id, request.clone()).await;
                match (actual, expected) {
                    (Ok(AcceptOutcome::Accepted(record)), None) => {
                        if record.id != id || record.request != request {
                            return Err(context("accepted immutable fields differ".into()));
                        }
                        model.accepted(id, key, request, record.accepted_at_ms);
                        if stable(record.summary()) != model.records[&id].summary {
                            return Err(context("initial lifecycle differs".into()));
                        }
                    }
                    (Ok(AcceptOutcome::Existing(record)), Some(expected)) if expected.request == request => {
                        if record.request != request || stable(record.summary()) != expected.summary {
                            return Err(context("idempotent replay differs".into()));
                        }
                    }
                    (Err(error), Some(expected))
                        if expected.request != request
                            && error_kind(&error) == Some(ErrorKind::IdempotencyConflict) => {}
                    (actual, expected) => {
                        return Err(context(format!("accept actual={actual:?} expected={expected:?}")));
                    }
                }
            }
            Command::Transition {
                id_index,
                version_delta,
                attempt_delta,
                legal,
                state,
            } => {
                let id = model.id(id_index);
                let command = model.transition_command(id, version_delta, attempt_delta, legal, state);
                let expected = model.transition(&command);
                compare_transition(store.transition(command).await, expected)
                    .map_err(|error| context(error.to_string()))?;
            }
            Command::RequestCancel { id_index } => {
                let command = model.cancel_command(model.id(id_index));
                let expected = model.transition(&command);
                compare_transition(store.transition(command).await, expected)
                    .map_err(|error| context(error.to_string()))?;
            }
            Command::List {
                filter,
                correlation,
                after_index,
                limit,
            } => {
                let query = TaskQuery {
                    states: STATES.get(usize::from(filter)).copied().into_iter().collect(),
                    correlation_key: (correlation < 2).then(|| format!("group-{correlation}")),
                    after: model.cursors.get(after_index).copied(),
                    limit,
                };
                let expected = model.list(&query);
                match (store.list(query).await, expected) {
                    (Ok(actual), Ok((rows, next)))
                        if actual.records.iter().cloned().map(stable).collect::<Vec<_>>() == rows
                            && actual.next == next => {}
                    (Err(error), Err(expected)) if error_kind(&error) == Some(expected) => {}
                    (actual, expected) => {
                        return Err(context(format!("list actual={actual:?} expected={expected:?}")));
                    }
                }
            }
            Command::Prune { cutoff_selector, limit } => {
                let cutoff = match cutoff_selector {
                    0 => 0,
                    // SQLite stores timestamps as signed integers; use the common domain.
                    1 => i64::MAX as u64,
                    selector => model
                        .cursors
                        .get(selector - 2)
                        .map_or(0, |cursor| cursor.accepted_at_ms),
                };
                let expected = model.prune(cutoff, limit);
                let actual = store
                    .prune_terminal_before(
                        cutoff,
                        NonZeroUsize::new(limit).expect("generated positive prune limit"),
                    )
                    .await
                    .map_err(|error| context(error.to_string()))?;
                if actual != expected {
                    return Err(context(format!("prune actual={actual}, expected={expected}")));
                }
            }
        }
        let actual_counts = store.count_states().await.map_err(|error| context(error.to_string()))?;
        if actual_counts != model.counts() {
            return Err(context(format!(
                "counts actual={actual_counts:?}, expected={:?}",
                model.counts()
            )));
        }
        for id in &model.ids {
            let actual = store
                .get_summary(*id)
                .await
                .map_err(|error| context(error.to_string()))?
                .map(stable);
            let expected = model.records.get(id).map(|record| record.summary.clone());
            if actual != expected {
                return Err(context(format!(
                    "summary id={id}, actual={actual:?}, expected={expected:?}"
                )));
            }
        }
        for key in 0..8 {
            let actual = store
                .get_summary_by_idempotency_key(&format!("model-key-{key}"))
                .await
                .map_err(|error| context(error.to_string()))?
                .map(stable);
            let expected = model
                .idempotency
                .get(&key)
                .and_then(|id| model.records.get(id))
                .map(|record| record.summary.clone());
            if actual != expected {
                return Err(context(format!("idempotency key {key} differs")));
            }
        }
    }
    Ok(())
}

/// Runs bounded cases with a printed replay seed and stores minimized failures
/// externally.
fn run_properties(name: &str, cases: u32, maximum: usize, sqlite: bool) {
    let artifacts = artifact_root();
    let seed = std::env::var("PROPTEST_RNG_SEED")
        .ok()
        .and_then(|seed| seed.parse().ok())
        .unwrap_or_else(|| {
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("clock after epoch")
                .as_nanos() as u64
        });
    eprintln!("{name}: PROPTEST_RNG_SEED={seed}, cases={cases}, max_commands={maximum}");
    std::fs::write(artifacts.join(format!("{name}-seed.txt")), seed.to_string()).expect("persist suite seed");
    let persistence: &'static str = Box::leak(
        artifacts
            .join(format!("{name}-regressions.txt"))
            .to_string_lossy()
            .into_owned()
            .into_boxed_str(),
    );
    let mut runner = TestRunner::new(Config {
        cases,
        rng_seed: RngSeed::Fixed(seed),
        max_shrink_iters: 4096,
        failure_persistence: Some(Box::new(FileFailurePersistence::Direct(persistence))),
        ..Config::default()
    });
    let result = runner.run(&commands(maximum), |commands| {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("case runtime");
        let database_dir = artifacts.join(format!("case-{}", TaskId::generate()));
        let result = runtime.block_on(async {
            #[cfg(feature = "sqlite")]
            if sqlite {
                std::fs::create_dir(&database_dir).expect("isolated case directory");
                let store = SqliteTaskStore::open(database_dir.join("model.sqlite")).expect("case SQLite opens");
                let epoch = store.acquire_owner().await.expect("case acquires owner");
                let result = run_trace(&store, &commands).await;
                store.release_owner(epoch).await.expect("case releases owner");
                return result;
            }
            let _ = sqlite;
            // At most 64 accept commands: this capacity cannot evict unmodeled history.
            run_trace(&MemoryTaskStore::new(64), &commands).await
        });
        drop(runtime);
        if database_dir.exists() {
            std::fs::remove_dir_all(&database_dir).expect("remove only case-owned database directory");
        }
        result
    });
    if let Err(error) = result {
        let trace = format!("PROPTEST_RNG_SEED={seed}\n{error:#?}\n");
        std::fs::write(artifacts.join(format!("{name}-minimal-trace.txt")), &trace).expect("save minimized trace");
        panic!("{trace}");
    }
}

#[test]
/// Checks distinguishing oracle edges before comparing it to any backend.
fn test_reference_model_rejects_queued_success_and_terminal_updates() {
    assert!(!store_reference_model::legal_edge(
        TaskStateKind::Queued,
        TaskStateKind::Succeeded
    ));
    assert!(store_reference_model::legal_edge(
        TaskStateKind::Running,
        TaskStateKind::Running
    ));
    assert!(!store_reference_model::legal_edge(
        TaskStateKind::Succeeded,
        TaskStateKind::Running
    ));
}

#[test]
/// Exercises replay, illegal edges, both stale CAS fields, cancellation,
/// retries, cursor gaps and reuse of a pruned idempotency key in one
/// deterministic trace.
fn test_model_directed_lifecycle_and_pruning_trace() {
    let commands = vec![
        Command::Accept { key: 0, variant: 0 },
        Command::Accept { key: 0, variant: 0 },
        Command::Accept { key: 0, variant: 1 },
        Command::Transition {
            id_index: 0,
            version_delta: 0,
            attempt_delta: 0,
            legal: false,
            state: 0,
        },
        Command::Transition {
            id_index: 0,
            version_delta: 1,
            attempt_delta: 0,
            legal: true,
            state: 0,
        },
        Command::Transition {
            id_index: 0,
            version_delta: 0,
            attempt_delta: 1,
            legal: true,
            state: 0,
        },
        Command::Transition {
            id_index: 0,
            version_delta: 0,
            attempt_delta: 0,
            legal: true,
            state: 0,
        },
        Command::RequestCancel { id_index: 0 },
        Command::Transition {
            id_index: 0,
            version_delta: 0,
            attempt_delta: 0,
            legal: true,
            state: 0,
        },
        Command::List {
            filter: 7,
            correlation: 2,
            after_index: 9,
            limit: 0,
        },
        Command::Transition {
            id_index: 0,
            version_delta: 0,
            attempt_delta: 0,
            legal: true,
            state: 1,
        },
        Command::Transition {
            id_index: 0,
            version_delta: 0,
            attempt_delta: 0,
            legal: true,
            state: 3,
        },
        Command::RequestCancel { id_index: 0 },
        Command::Accept { key: 1, variant: 0 },
        Command::RequestCancel { id_index: 3 },
        Command::List {
            filter: 7,
            correlation: 2,
            after_index: 9,
            limit: 1,
        },
        Command::Prune {
            cutoff_selector: 1,
            limit: 1,
        },
        Command::List {
            filter: 7,
            correlation: 2,
            after_index: 0,
            limit: 256,
        },
        Command::Accept { key: 0, variant: 2 },
        Command::Transition {
            id_index: 5,
            version_delta: 0,
            attempt_delta: 0,
            legal: true,
            state: 0,
        },
        Command::List {
            filter: 7,
            correlation: 2,
            after_index: 9,
            limit: 257,
        },
    ];
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("case runtime");
    runtime
        .block_on(run_trace(&MemoryTaskStore::new(64), &commands))
        .expect("directed trace matches");
}

#[test]
/// Compares 128 independent memory stores with at most 64 commands each.
fn test_memory_state_machine_matches_independent_model() {
    run_properties("memory", 128, 64, false);
}

#[cfg(feature = "sqlite")]
#[test]
/// Compares 32 independently owned SQLite databases with at most 32 commands
/// each.
fn test_sqlite_state_machine_matches_independent_model() {
    run_properties("sqlite", 32, 32, true);
}
