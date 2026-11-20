// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Scope guard for a registered task attempt cancellation state.

use std::sync::Arc;

use crate::handler::typed::attempt_cancellation::AttemptCancellation;
use crate::model::typed::TaskId;

use super::TypedServiceCore;

/// Removes only the exact state registered by this execution attempt.
pub(in crate::service::typed_task_execution_service) struct AttemptCancellationGuard {
    /// Service state whose attempt registration this guard owns.
    core: Arc<TypedServiceCore>,
    /// Task identity and attempt number used as the cancellation-map key.
    key: (TaskId, u32),
    /// Registered state used to avoid removing a replacement entry.
    state: Arc<AttemptCancellation>,
}

impl AttemptCancellationGuard {
    /// Creates a guard for a cancellation-map entry already inserted by the runner.
    ///
    /// The guard removes the entry on every exit path, including early returns
    /// and unwinding, and checks pointer identity before removal.
    pub(in crate::service::typed_task_execution_service) fn new(
        core: Arc<TypedServiceCore>,
        key: (TaskId, u32),
        state: Arc<AttemptCancellation>,
    ) -> Self {
        Self { core, key, state }
    }
}

impl Drop for AttemptCancellationGuard {
    /// Removes the owned registration without disturbing a newer entry.
    fn drop(&mut self) {
        let mut entries = self.core.cancellations.lock();
        if entries
            .get(&self.key)
            .is_some_and(|current| Arc::ptr_eq(current, &self.state))
        {
            entries.remove(&self.key);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;

    use qubit_codec::ValueBytesCodecRegistry;
    use qubit_id::Id;
    use qubit_id::IdGenerationError;
    use qubit_id::IdGenerator;

    use super::*;
    use crate::handler::typed::attempt_cancellation::AttemptCancellation;
    use crate::model::typed::TaskId;
    use crate::service::TaskExecutionServiceBuilder;
    use crate::store::MemoryTaskStore;

    struct TestIds(AtomicU64);

    impl IdGenerator<Id, IdGenerationError> for TestIds {
        fn generate(&self) -> Result<Id, IdGenerationError> {
            Ok(Id::new(
                self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            ))
        }
    }

    async fn service_core() -> Arc<TypedServiceCore> {
        let service = TaskExecutionServiceBuilder::new(
            Arc::new(MemoryTaskStore::new(4)),
            Arc::new(ValueBytesCodecRegistry::empty()),
            Arc::new(TestIds(AtomicU64::new(1))),
        )
        .build()
        .await
        .expect("service starts");
        Arc::clone(&service.core)
    }

    #[tokio::test]
    async fn test_registration_is_removed_when_readback_returns_error() {
        let core = service_core().await;
        let key = (TaskId::from_id(Id::new(10)), 1);
        let state = Arc::new(AttemptCancellation::new());
        let result: Result<(), &'static str> = {
            core.cancellations.lock().insert(key, Arc::clone(&state));
            let _guard = AttemptCancellationGuard::new(Arc::clone(&core), key, Arc::clone(&state));
            Err("injected summary readback failure")
        };
        assert_eq!(result, Err("injected summary readback failure"));
        assert!(!core.cancellations.lock().contains_key(&key));
    }

    #[tokio::test]
    async fn test_registration_is_removed_when_readback_unwinds() {
        let core = service_core().await;
        let key = (TaskId::from_id(Id::new(11)), 1);
        let state = Arc::new(AttemptCancellation::new());
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            core.cancellations.lock().insert(key, Arc::clone(&state));
            let _guard = AttemptCancellationGuard::new(Arc::clone(&core), key, Arc::clone(&state));
            panic!("injected summary readback panic");
        }));
        assert!(result.is_err());
        assert!(!core.cancellations.lock().contains_key(&key));
    }
}
