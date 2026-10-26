// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Sequential durable lifecycle notification publication.

mod internal;
#[path = "task_event_publisher/task_event_publisher.rs"]
mod publisher_impl;

pub(in crate::service::task_event_publisher) use internal::QuarantineCommand;
pub(super) use publisher_impl::TaskEventPublisher;

#[cfg(all(test, feature = "sqlite"))]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use qubit_event_bus::AsyncEventBusRegistry;
    use qubit_event_bus::EventBusConfig;
    use uuid::Uuid;

    use super::TaskEventPublisher;
    use crate::store::SqliteTaskStore;
    use crate::store::TaskStore;

    async fn create_publisher() -> (TaskEventPublisher, std::path::PathBuf) {
        let database = std::env::temp_dir().join(format!("task-publisher-{}.sqlite", Uuid::new_v4()));
        let store = Arc::new(SqliteTaskStore::open(&database).expect("open temporary task store"));
        store.acquire_owner().await.expect("acquire store owner");
        store.enable_event_outbox().await.expect("enable durable outbox");
        let registry = AsyncEventBusRegistry::with_local().expect("local bus registry");
        let bus = Arc::new(
            registry
                .create(&EventBusConfig::default())
                .await
                .expect("local event bus"),
        );
        TaskEventPublisher::new(store, bus, Duration::from_secs(1))
            .map(|publisher| (publisher, database))
            .expect("publisher configuration is valid")
    }

    #[tokio::test]
    async fn test_close_without_started_worker_is_idempotent() {
        let (publisher, database) = create_publisher().await;

        publisher.close().await.expect("no-worker close succeeds");
        publisher.close().await.expect("repeated close succeeds");
        drop(publisher);
        std::fs::remove_file(database).expect("remove temporary task database");
    }

    #[tokio::test]
    async fn test_start_is_idempotent_and_close_drains_empty_outbox() {
        let (publisher, database) = create_publisher().await;

        publisher.start().await;
        publisher.start().await;
        publisher.notify();
        assert_eq!(publisher.stats().queued, 1);
        assert_eq!(publisher.stats().published, 0);
        assert_eq!(publisher.stats().failed, 0);
        assert_eq!(publisher.stats().dropped, 0);

        publisher.close().await.expect("worker exits after empty outbox");
        publisher.close().await.expect("joined worker is not joined twice");
        drop(publisher);
        std::fs::remove_file(database).expect("remove temporary task database");
    }

    #[tokio::test]
    async fn test_close_reports_outbox_read_failure_at_deadline() {
        let (publisher, database) = create_publisher().await;
        rusqlite::Connection::open(&database)
            .expect("open database for fault injection")
            .execute_batch("DROP TABLE task_event_outbox")
            .expect("remove the outbox table");

        publisher.start().await;
        let error = publisher
            .close()
            .await
            .expect_err("outbox read keeps failing until deadline");
        let diagnostic = error.to_string();
        assert!(diagnostic.contains("pending count unavailable"), "{diagnostic}");
        assert!(
            diagnostic.contains("no publication was active at timeout"),
            "{diagnostic}"
        );
        assert!(diagnostic.contains("task_event_outbox"), "{diagnostic}");
        drop(publisher);
        std::fs::remove_file(database).expect("remove temporary task database");
    }
}
