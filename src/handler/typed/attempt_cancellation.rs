// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared cancellation state for one typed task attempt.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

/// Cancellation state shared by the service and all contexts for one attempt.
pub(crate) struct AttemptCancellation {
    /// Legacy atomic flag shared with existing handler cancellation APIs.
    flag: Arc<AtomicBool>,
    /// Monotonic notification state observed by operation-feature waiters.
    #[cfg(feature = "operation")]
    requested: tokio::sync::watch::Sender<bool>,
}

impl AttemptCancellation {
    /// Creates cancellation state, with an operation notification channel only
    /// when the optional operation feature is enabled.
    pub(crate) fn new() -> Self {
        Self {
            flag: Arc::new(AtomicBool::new(false)),
            #[cfg(feature = "operation")]
            requested: tokio::sync::watch::channel(false).0,
        }
    }

    /// Returns the legacy raw cancellation flag.
    pub(crate) fn flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.flag)
    }

    /// Sets the legacy flag and publishes the cooperative cancellation event.
    pub(crate) fn request(&self) {
        self.flag.store(true, Ordering::Release);
        #[cfg(feature = "operation")]
        self.requested.send_replace(true);
    }

    /// Waits until the service publishes cooperative cancellation.
    #[cfg(feature = "operation")]
    pub(crate) async fn wait_requested(&self) {
        let mut receiver = self.requested.subscribe();
        let _observed = receiver.wait_for(|requested| *requested).await;
    }
}

impl Default for AttemptCancellation {
    /// Creates an unset cancellation state for one task attempt.
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(all(test, feature = "operation"))]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use super::AttemptCancellation;

    #[tokio::test]
    async fn test_request_before_subscription_is_observed() {
        let state = AttemptCancellation::new();
        state.request();
        tokio::time::timeout(Duration::from_secs(1), state.wait_requested())
            .await
            .expect("late subscriber observes the latched request");
    }

    #[tokio::test]
    async fn test_all_subscribers_observe_request() {
        let state = Arc::new(AttemptCancellation::new());
        let first = {
            let state = Arc::clone(&state);
            tokio::spawn(async move { state.wait_requested().await })
        };
        let second = {
            let state = Arc::clone(&state);
            tokio::spawn(async move { state.wait_requested().await })
        };
        state.request();
        tokio::time::timeout(Duration::from_secs(1), async {
            first.await.expect("first waiter completes");
            second.await.expect("second waiter completes");
        })
        .await
        .expect("both waiters observe the request");
    }

    #[tokio::test]
    async fn test_dropped_waiter_can_wait_again() {
        let state = AttemptCancellation::new();
        let mut first = Box::pin(state.wait_requested());
        assert!(futures::poll!(&mut first).is_pending());
        drop(first);
        state.request();
        tokio::time::timeout(Duration::from_secs(1), state.wait_requested())
            .await
            .expect("new waiter observes the latched request");
    }

    #[tokio::test]
    async fn test_raw_flag_write_does_not_notify() {
        let state = AttemptCancellation::new();
        state.flag().store(true, Ordering::Release);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), state.wait_requested())
                .await
                .is_err()
        );
    }
}
