// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use tokio::sync::watch;

mod internal;
mod task_cancellation;

pub use task_cancellation::TaskCancellation;

use self::internal::CancellationState;

/// Service-side sender for one attempt's cancellation state.
pub(crate) struct AttemptCancellation {
    /// Publishes cancellation updates to all handler-side receivers.
    state: watch::Sender<CancellationState>,
}

impl AttemptCancellation {
    /// Creates a sender with no cancellation request published.
    pub(crate) fn new() -> Self {
        Self {
            state: watch::channel(CancellationState::Clear).0,
        }
    }

    /// Publishes a cancellation request, preserving an existing request.
    ///
    /// Cooperative requests also wake subscribers waiting for notification.
    pub(crate) fn request(&self, cooperative: bool) {
        self.state.send_modify(|state| {
            if cooperative {
                *state = CancellationState::CooperativeRequested;
            } else if *state == CancellationState::Clear {
                *state = CancellationState::Requested;
            }
        });
    }

    /// Creates a read-only receiver for the current attempt state.
    pub(crate) fn handle(&self) -> TaskCancellation {
        TaskCancellation {
            state: self.state.subscribe(),
        }
    }
}

impl Default for AttemptCancellation {
    /// Creates an attempt cancellation sender with no request.
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::AttemptCancellation;

    #[tokio::test]
    async fn test_cooperative_wait_is_latched_and_repeatable() {
        let sender = AttemptCancellation::new();
        let handle = sender.handle();
        assert!(!handle.is_requested());
        sender.request(true);
        tokio::time::timeout(Duration::from_secs(1), handle.wait_requested())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), handle.wait_requested())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_persisted_non_cooperative_request_is_queryable_but_not_waitable() {
        let sender = AttemptCancellation::new();
        let handle = sender.handle();
        sender.request(false);
        assert!(handle.is_requested());
        assert!(
            tokio::time::timeout(Duration::from_millis(10), handle.wait_requested())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_closed_sender_does_not_signal_cancellation() {
        let sender = AttemptCancellation::new();
        let handle = sender.handle();
        drop(sender);

        assert!(
            tokio::time::timeout(Duration::from_millis(10), handle.wait_requested())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_late_and_shared_subscribers_observe_cooperative_request() {
        let sender = std::sync::Arc::new(AttemptCancellation::new());
        let first = sender.handle();
        let second = sender.handle();
        let first_wait = tokio::spawn(async move { first.wait_requested().await });
        let second_wait = tokio::spawn(async move { second.wait_requested().await });
        sender.request(true);
        tokio::time::timeout(Duration::from_secs(1), async {
            first_wait.await.unwrap();
            second_wait.await.unwrap();
        })
        .await
        .unwrap();
        let late = sender.handle();
        tokio::time::timeout(Duration::from_secs(1), late.wait_requested())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_dropping_a_waiter_does_not_consume_the_request() {
        let sender = AttemptCancellation::new();
        let handle = sender.handle();
        let mut waiter = Box::pin(handle.wait_requested());
        assert!(futures::poll!(&mut waiter).is_pending());
        drop(waiter);
        sender.request(true);
        tokio::time::timeout(Duration::from_secs(1), handle.wait_requested())
            .await
            .unwrap();
    }
}
