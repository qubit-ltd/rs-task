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
    use qubit_event_bus::model::SubscribeRequest;
    use qubit_event_bus::model::SubscriberId;
    use qubit_event_bus::model::Topic;
    use qubit_metadata::Metadata;
    use uuid::Uuid;

    use super::TaskEventPublisher;
    use crate::model::ResourceRequest;
    use crate::model::StoredPayload;
    use crate::model::StoredTaskRequest;
    use crate::model::typed::TaskId;
    use crate::store::SqliteTaskStore;
    use crate::store::TaskStore;

    async fn create_publisher() -> (TaskEventPublisher, std::path::PathBuf, Arc<SqliteTaskStore>) {
        let database =
            std::env::temp_dir().join(format!("task-publisher-{}.sqlite", Uuid::new_v4()));
        let store = Arc::new(SqliteTaskStore::open(&database).expect("open temporary task store"));
        store.acquire_owner().await.expect("acquire store owner");
        store
            .enable_event_outbox()
            .await
            .expect("enable durable outbox");
        let registry = AsyncEventBusRegistry::with_local().expect("local bus registry");
        let bus = Arc::new(
            registry
                .create(&EventBusConfig::default())
                .await
                .expect("local event bus"),
        );
        let subscription = bus
            .subscribe(
                SubscribeRequest::builder()
                    .subscriber_id(SubscriberId::new("publisher-unit-test").expect("subscriber ID"))
                    .topic(Topic::<crate::event::TaskEvent>::new("task.lifecycle").expect("topic"))
                    .build()
                    .expect("subscription request"),
            )
            .await
            .expect("subscribe for local publish admission");
        tokio::spawn(async move {
            subscription
                .run(|_| async { Ok::<(), qubit_event_bus::DeliveryError>(()) })
                .await
                .expect("local event subscription runs");
        });
        TaskEventPublisher::new(store.clone(), bus, Duration::from_secs(1))
            .map(|publisher| (publisher, database, store))
            .expect("publisher configuration is valid")
    }

    #[tokio::test]
    async fn test_close_without_started_worker_is_idempotent() {
        let (publisher, database, _) = create_publisher().await;

        publisher.close().await.expect("no-worker close succeeds");
        publisher.close().await.expect("repeated close succeeds");
        drop(publisher);
        std::fs::remove_file(database).expect("remove temporary task database");
    }

    #[tokio::test]
    async fn test_start_is_idempotent_and_close_drains_empty_outbox() {
        let (publisher, database, _) = create_publisher().await;

        publisher.start().await;
        publisher.start().await;
        publisher.notify();
        assert_eq!(publisher.stats().queued, 1);
        publisher.wake();
        assert_eq!(publisher.stats().queued, 1);
        assert_eq!(publisher.stats().published, 0);
        assert_eq!(publisher.stats().failed, 0);
        assert_eq!(publisher.stats().dropped, 0);

        publisher
            .close()
            .await
            .expect("worker exits after empty outbox");
        publisher
            .close()
            .await
            .expect("joined worker is not joined twice");
        drop(publisher);
        std::fs::remove_file(database).expect("remove temporary task database");
    }

    #[tokio::test]
    async fn test_wake_processes_pending_outbox_without_incrementing_queued() {
        let (publisher, database, store) = create_publisher().await;
        publisher.start().await;
        store
            .accept_encoded(
                TaskId::from_id(qubit_id::Id::new(77)),
                StoredTaskRequest {
                    kind_id: "publisher.wake-test".into(),
                    category: None,
                    payload: StoredPayload {
                        type_id: "publisher.WakePayload".try_into().expect("model ID"),
                        schema_version: 1,
                        codec_id: "json".into(),
                        bytes: vec![1],
                    },
                    metadata: Metadata::new(),
                    resource_limit: ResourceRequest::default(),
                    correlation_key: None,
                    idempotency_key: None,
                },
            )
            .await
            .expect("persist pending lifecycle event");
        publisher.wake();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if publisher.stats().published == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "wake did not publish pending outbox row: {:?}, {:?}",
                publisher.stats(),
                publisher.failure()
            )
        });
        assert_eq!(publisher.stats().queued, 0);
        assert!(
            store
                .list_event_outbox(128)
                .await
                .expect("outbox read")
                .is_empty()
        );
        publisher.close().await.expect("worker closes");
        drop(publisher);
        std::fs::remove_file(database).expect("remove temporary task database");
    }

    #[tokio::test]
    async fn test_close_reports_outbox_read_failure_at_deadline() {
        let (publisher, database, _) = create_publisher().await;
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
        assert!(
            diagnostic.contains("pending count unavailable"),
            "{diagnostic}"
        );
        assert!(
            diagnostic.contains("no publication was active at timeout"),
            "{diagnostic}"
        );
        assert!(diagnostic.contains("task_event_outbox"), "{diagnostic}");
        drop(publisher);
        std::fs::remove_file(database).expect("remove temporary task database");
    }
}
