// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use futures::FutureExt;
use tokio::sync;
use tokio::task::JoinHandle;

use super::AttemptInFlightGuard;
use super::ServiceCore;
use super::finish_attempt;
use super::panic_message;
use super::record_store_fault;
use crate::engine::ExecutionOutcome;
use crate::model::TaskRecord;

/// Spawns finalization with ownership established before the first poll.
///
/// The scheduler must increment the in-flight count exactly once before this
/// call. The returned worker retains the running permit through finalization;
/// dropping its unpolled future also releases the count and permit. Store
/// panics are latched as service faults and wake public task waiters.
///
/// `running` is the committed attempt snapshot and `receiver` carries the
/// engine outcome. The service runtime spawns the returned worker immediately.
pub(super) fn spawn_attempt_finalizer(
    core: Arc<ServiceCore>,
    running: TaskRecord,
    receiver: sync::oneshot::Receiver<ExecutionOutcome>,
    permit: sync::OwnedSemaphorePermit,
) -> JoinHandle<()> {
    let runtime_handle = core.runtime_handle.clone();
    let guard = AttemptInFlightGuard {
        core_ref: Arc::downgrade(&core),
    };
    runtime_handle.spawn(async move {
        let _guard = guard;
        let id = running.id;
        let attempt = running.attempt;
        let weak = Arc::downgrade(&core);
        let result = AssertUnwindSafe(async {
            finish_attempt(weak, running, receiver, permit).await;
        })
        .catch_unwind()
        .await;
        if let Err(payload) = result {
            record_store_fault(
                &core,
                format!(
                    "attempt finalizer panicked for {id}, attempt {attempt}: {}",
                    panic_message(payload)
                ),
            );
        }
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    use tokio::runtime::Builder;
    use tokio::sync;

    use super::super::retry_queue_reservation::RetryQueueReservation;
    use super::spawn_attempt_finalizer;
    use crate::TaskExecutionServiceBuilder;
    use crate::model::TaskId;
    use crate::model::TaskRequest;

    #[test]
    fn test_attempt_finalizer_supervisor_unpolled_drop_restores_resources() {
        let runtime = Builder::new_current_thread().enable_all().build().expect("runtime");
        let service = runtime.block_on(async {
            let service = TaskExecutionServiceBuilder::in_memory()
                .runtime_handle(tokio::runtime::Handle::current())
                .build()
                .await
                .expect("service");
            service.shutdown().await.expect("scheduler stopped");
            service
        });
        let core = Arc::clone(&service.core);
        let id = TaskId::generate();
        let _accepted = runtime
            .block_on(core.store.accept(id, TaskRequest::new("unpolled", "1", vec![])))
            .expect("accepted");
        let record = runtime.block_on(core.store.get(id)).expect("read").expect("record");
        let available = core.running_slots.available_permits();
        let permit = Arc::clone(&core.running_slots)
            .try_acquire_owned()
            .expect("running slot");
        core.attempts_in_flight.fetch_add(1, Ordering::AcqRel);
        let (_sender, receiver) = sync::oneshot::channel();
        let finalizer = spawn_attempt_finalizer(Arc::clone(&core), record, receiver, permit);
        // This runtime has not polled the newly spawned task even once.
        finalizer.abort();
        assert!(runtime.block_on(finalizer).expect_err("aborted").is_cancelled());
        assert_eq!(core.attempts_in_flight.load(Ordering::Acquire), 0);
        assert_eq!(core.running_slots.available_permits(), available);

        let reservation = RetryQueueReservation::try_new(&core).expect("retry slot");
        assert_eq!(core.queue_count.load(Ordering::Acquire), 1);
        let unpolled = async move {
            let _reservation = reservation;
        };
        drop(unpolled);
        assert_eq!(core.queue_count.load(Ordering::Acquire), 0);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _reservation = RetryQueueReservation::try_new(&core).expect("retry slot");
            panic!("uncommitted retry write panics");
        }));
        assert!(result.is_err());
        assert_eq!(core.queue_count.load(Ordering::Acquire), 0);
        RetryQueueReservation::try_new(&core)
            .expect("retry slot")
            .commit_to_queue();
        assert_eq!(core.queue_count.load(Ordering::Acquire), 1);
    }
}
