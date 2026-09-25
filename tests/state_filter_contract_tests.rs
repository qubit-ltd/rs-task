// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
#[cfg(feature = "sqlite")]
mod sqlite_tests {
    use std::sync::Arc;

    use qubit_task::model::AcceptOutcome;
    use qubit_task::model::TaskId;
    use qubit_task::model::TaskQuery;
    use qubit_task::model::TaskRecord;
    use qubit_task::model::TaskRequest;
    use qubit_task::model::TaskState;
    use qubit_task::model::TaskStateKind;
    use qubit_task::model::TransitionCommand;
    use qubit_task::store::MemoryTaskStore;
    use qubit_task::store::SqliteTaskStore;
    use qubit_task::store::TaskStore;

    /// Inserts a record and moves it to the requested state through store
    /// transitions.
    async fn insert_state(store: &dyn TaskStore, state: TaskState) -> TaskRecord {
        let id = TaskId::generate();
        let accepted = store
            .accept(id, TaskRequest::new("filter-test", "1", Vec::new()))
            .await
            .expect("task accepted");
        let AcceptOutcome::Accepted(mut record) = accepted else {
            panic!("task ID is newly generated");
        };
        if matches!(
            state,
            TaskState::Failed { .. } | TaskState::Panicked { .. } | TaskState::Succeeded
        ) {
            record = store
                .transition(TransitionCommand {
                    id,
                    expected_version: record.state_version,
                    expected_attempt: record.attempt,
                    state: TaskState::Running,
                    output: None,
                    assigned_resources: Vec::new(),
                    cancel_requested: false,
                })
                .await
                .expect("task starts");
        }
        store
            .transition(TransitionCommand {
                id,
                expected_version: record.state_version,
                expected_attempt: record.attempt,
                state,
                output: None,
                assigned_resources: Vec::new(),
                cancel_requested: false,
            })
            .await
            .expect("task reaches requested state")
    }

    /// Removes only database files created by the SQLite test.
    fn remove_database(path: &std::path::Path) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(path.with_extension("owner.lock"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }

    #[tokio::test]
    async fn test_memory_and_sqlite_filter_failed_states_by_kind() {
        let path = std::env::temp_dir().join(format!("qubit-task-state-filter-{}.sqlite", TaskId::generate()));
        let memory: Arc<dyn TaskStore> = Arc::new(MemoryTaskStore::new(16));
        let sqlite: Arc<dyn TaskStore> = Arc::new(SqliteTaskStore::open(&path).expect("SQLite store opens"));
        let memory_a = insert_state(
            memory.as_ref(),
            TaskState::Failed {
                category: "decode".into(),
                message: "invalid header".into(),
            },
        )
        .await;
        let memory_b = insert_state(
            memory.as_ref(),
            TaskState::Failed {
                category: "network".into(),
                message: "peer closed".into(),
            },
        )
        .await;
        let sqlite_a = insert_state(
            sqlite.as_ref(),
            TaskState::Failed {
                category: "decode".into(),
                message: "invalid header".into(),
            },
        )
        .await;
        let sqlite_b = insert_state(
            sqlite.as_ref(),
            TaskState::Failed {
                category: "network".into(),
                message: "peer closed".into(),
            },
        )
        .await;

        let query = TaskQuery {
            states: vec![TaskStateKind::Failed],
            limit: 1,
            ..TaskQuery::default()
        };
        let memory_first = memory.list(query.clone()).await.expect("memory query succeeds");
        let sqlite_first = sqlite.list(query.clone()).await.expect("SQLite query succeeds");
        assert!(memory_first.next.is_some());
        assert!(sqlite_first.next.is_some());
        let memory_second = memory
            .list(TaskQuery {
                after: memory_first.next,
                ..query.clone()
            })
            .await
            .expect("memory second page succeeds");
        let sqlite_second = sqlite
            .list(TaskQuery {
                after: sqlite_first.next,
                ..query
            })
            .await
            .expect("SQLite second page succeeds");
        let memory_ids = memory_first
            .records
            .iter()
            .chain(&memory_second.records)
            .map(|record| record.id)
            .collect::<Vec<_>>();
        let sqlite_ids = sqlite_first
            .records
            .iter()
            .chain(&sqlite_second.records)
            .map(|record| record.id)
            .collect::<Vec<_>>();

        assert_eq!(memory_ids.len(), 2);
        assert_eq!(sqlite_ids.len(), 2);
        assert!(memory_ids.contains(&memory_a.id) && memory_ids.contains(&memory_b.id));
        assert!(sqlite_ids.contains(&sqlite_a.id) && sqlite_ids.contains(&sqlite_b.id));

        let memory_blocked = insert_state(
            memory.as_ref(),
            TaskState::Blocked {
                reason: "missing handler".into(),
            },
        )
        .await;
        let memory_blocked_second = insert_state(
            memory.as_ref(),
            TaskState::Blocked {
                reason: "operator approval required".into(),
            },
        )
        .await;
        let sqlite_blocked = insert_state(
            sqlite.as_ref(),
            TaskState::Blocked {
                reason: "resources unavailable".into(),
            },
        )
        .await;
        let sqlite_blocked_second = insert_state(
            sqlite.as_ref(),
            TaskState::Blocked {
                reason: "resource request is unsatisfiable".into(),
            },
        )
        .await;
        let blocked_query = TaskQuery {
            states: vec![TaskStateKind::Blocked],
            limit: 16,
            ..TaskQuery::default()
        };
        let memory_result = memory
            .list(blocked_query.clone())
            .await
            .expect("memory blocked query succeeds");
        let sqlite_result = sqlite.list(blocked_query).await.expect("SQLite blocked query succeeds");
        assert_eq!(memory_result.records.len(), 2);
        assert_eq!(sqlite_result.records.len(), 2);
        let memory_blocked_ids = memory_result.records.iter().map(|record| record.id).collect::<Vec<_>>();
        let sqlite_blocked_ids = sqlite_result.records.iter().map(|record| record.id).collect::<Vec<_>>();
        assert!(memory_blocked_ids.contains(&memory_blocked.id));
        assert!(memory_blocked_ids.contains(&memory_blocked_second.id));
        assert!(sqlite_blocked_ids.contains(&sqlite_blocked.id));
        assert!(sqlite_blocked_ids.contains(&sqlite_blocked_second.id));

        let mixed_query = TaskQuery {
            states: vec![TaskStateKind::Failed, TaskStateKind::Blocked],
            limit: 16,
            ..TaskQuery::default()
        };
        assert_eq!(
            memory
                .list(mixed_query.clone())
                .await
                .expect("memory mixed query succeeds")
                .records
                .len(),
            4
        );
        assert_eq!(
            sqlite
                .list(mixed_query)
                .await
                .expect("SQLite mixed query succeeds")
                .records
                .len(),
            4
        );
        let unfiltered_query = TaskQuery {
            limit: 16,
            ..TaskQuery::default()
        };
        assert_eq!(
            memory
                .list(unfiltered_query.clone())
                .await
                .expect("memory unfiltered query succeeds")
                .records
                .len(),
            4
        );
        assert_eq!(
            sqlite
                .list(unfiltered_query)
                .await
                .expect("SQLite unfiltered query succeeds")
                .records
                .len(),
            4
        );

        drop(memory);
        drop(sqlite);
        remove_database(&path);
    }
}
