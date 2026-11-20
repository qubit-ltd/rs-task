// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
// =============================================================================
use tokio::sync::watch;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CancellationState {
    Clear,
    Requested,
    CooperativeRequested,
}

/// Service-side sender for one attempt's cancellation state.
pub(crate) struct AttemptCancellation {
    state: watch::Sender<CancellationState>,
}

impl AttemptCancellation {
    pub(crate) fn new() -> Self {
        Self {
            state: watch::channel(CancellationState::Clear).0,
        }
    }

    pub(crate) fn request(&self, cooperative: bool) {
        self.state.send_modify(|state| {
            if cooperative {
                *state = CancellationState::CooperativeRequested;
            } else if *state == CancellationState::Clear {
                *state = CancellationState::Requested;
            }
        });
    }

    pub(crate) fn handle(&self) -> TaskCancellation {
        TaskCancellation {
            state: self.state.subscribe(),
        }
    }
}

impl Default for AttemptCancellation {
    fn default() -> Self {
        Self::new()
    }
}

/// Read-only cancellation state shared with a task handler.
#[derive(Clone)]
pub struct TaskCancellation {
    state: watch::Receiver<CancellationState>,
}

impl TaskCancellation {
    /// Returns whether cancellation has been durably requested for this attempt.
    #[must_use]
    pub fn is_requested(&self) -> bool {
        *self.state.borrow() != CancellationState::Clear
    }

    /// Waits until the service publishes a cooperative cancellation request.
    pub async fn wait_requested(&self) {
        let mut state = self.state.clone();
        loop {
            if *state.borrow() == CancellationState::CooperativeRequested {
                return;
            }
            if state.changed().await.is_err() {
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::AttemptCancellation;

    #[tokio::test]
    async fn cooperative_wait_is_latched_and_repeatable() {
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
    async fn persisted_non_cooperative_request_is_queryable_but_not_waitable() {
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
    async fn late_and_shared_subscribers_observe_cooperative_request() {
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
    async fn dropping_a_waiter_does_not_consume_the_request() {
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
