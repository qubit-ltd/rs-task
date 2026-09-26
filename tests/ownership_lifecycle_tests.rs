// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::model::TaskRequest;
use qubit_task::service::TaskServiceError;

#[tokio::test]
async fn test_dropping_one_clone_keeps_service_open() {
    let service = TaskExecutionServiceBuilder::in_memory()
        .build()
        .await
        .expect("in-memory service builds");
    let retained = service.clone();

    drop(service);

    let handle = retained
        .submit_local(
            |_| qubit_task::service::LocalTaskOutcome::<(), std::io::Error>::Succeeded {
                value: (),
                summary: qubit_task::model::TaskOutput::default(),
            },
        )
        .await
        .expect("retained clone can still accept work");
    handle.result().await.expect("task finalizes").expect("task succeeds");
    retained.shutdown().await.expect("retained clone closes service");
}

#[tokio::test]
async fn test_cancel_after_shutdown_is_rejected() {
    let service = TaskExecutionServiceBuilder::in_memory()
        .build()
        .await
        .expect("in-memory service builds");
    let accepted = service
        .submit(test_keyed(TaskRequest::new("missing", "1", Vec::new())))
        .await
        .expect("task is accepted");
    assert!(matches!(
        service.wait(accepted.id).await,
        Err(TaskServiceError::Blocked)
    ));
    service.shutdown().await.expect("service shuts down");
    assert!(matches!(
        service
            .get(accepted.id)
            .await
            .expect("record remains readable")
            .map(|record| record.state),
        Some(qubit_task::model::TaskState::Blocked { .. })
    ));

    assert!(matches!(
        service.cancel(accepted.id).await,
        Err(TaskServiceError::ShuttingDown)
    ));
}

#[cfg(feature = "sqlite")]
mod sqlite_tests {
    use qubit_task::TaskExecutionServiceBuilder;
    use qubit_task::model::AcceptOutcome;
    use qubit_task::model::OwnerEpoch;
    use qubit_task::model::TaskId;
    use qubit_task::model::TaskRequest;
    use qubit_task::model::TaskState;
    use qubit_task::model::TransitionCommand;
    use qubit_task::store::SqliteTaskStore;
    use qubit_task::store::StoreError;
    use qubit_task::store::TaskStore;

    /// Returns a unique path for one isolated SQLite store test.
    fn test_database_path() -> std::path::PathBuf {
        std::env::temp_dir().join(format!("qubit-task-owner-{}.sqlite", TaskId::generate()))
    }

    /// Removes only disposable database files created by this test.
    fn remove_database(path: &std::path::Path) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(path.with_extension("owner.lock"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }

    #[tokio::test]
    async fn test_released_owner_is_fenced_and_epoch_is_checked() {
        let path = test_database_path();
        let store = SqliteTaskStore::open(&path).expect("store opens");
        let epoch = store.acquire_owner().await.expect("owner acquired");
        let id = TaskId::generate();
        let accepted = store
            .accept(id, TaskRequest::new("task", "1", Vec::new()))
            .await
            .expect("task accepted");
        let record = match accepted {
            AcceptOutcome::Accepted(record) => record,
            AcceptOutcome::Existing(_) => panic!("task ID is newly generated"),
        };

        assert!(matches!(
            store.release_owner(OwnerEpoch(epoch.0 + 1)).await,
            Err(StoreError::Failure(_))
        ));
        store.release_owner(epoch).await.expect("owner released");
        assert!(matches!(
            store
                .accept(TaskId::generate(), TaskRequest::new("task", "1", Vec::new()))
                .await,
            Err(StoreError::Failure(_))
        ));
        assert!(matches!(
            store
                .transition(TransitionCommand {
                    id,
                    expected_version: record.state_version,
                    expected_attempt: record.attempt,
                    state: TaskState::Cancelled,
                    output: None,
                    assigned_resources: Vec::new(),
                    retry_not_before_ms: None,
                    cancel_requested: false,
                })
                .await,
            Err(StoreError::Failure(_))
        ));
        drop(store);

        let replacement = SqliteTaskStore::open(&path).expect("replacement owner opens");
        let replacement_epoch = replacement.acquire_owner().await.expect("replacement owner acquired");
        assert!(replacement_epoch.0 > epoch.0);
        let unchanged = replacement
            .get(id)
            .await
            .expect("record can be read")
            .expect("record remains");
        assert_eq!(unchanged.state, TaskState::Queued);
        replacement
            .release_owner(replacement_epoch)
            .await
            .expect("replacement owner released");
        drop(replacement);
        remove_database(&path);
    }

    #[tokio::test]
    async fn test_drop_last_service_handle_releases_sqlite_owner() {
        let path = test_database_path();
        let service = TaskExecutionServiceBuilder::recoverable_sqlite(&path)
            .expect("recoverable service config")
            .build()
            .await
            .expect("recoverable service builds");
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        drop(service);

        let replacement = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                if let Ok(store) = SqliteTaskStore::open(&path) {
                    break store;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("last service handle drop releases SQLite ownership");
        drop(replacement);
        remove_database(&path);
    }
}

#[allow(dead_code)]
fn test_keyed(mut request: qubit_task::model::TaskRequest) -> qubit_task::model::TaskRequest {
    static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(1);
    if request.idempotency_key.is_none() {
        request.idempotency_key = Some(format!(
            "test-request-{}",
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
    }
    request
}
