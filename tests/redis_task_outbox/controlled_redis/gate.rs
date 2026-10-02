// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! One-shot synchronization point for an applied Redis reply.

use std::future::Future;
use std::pin::Pin;
use std::sync::Condvar;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;

#[derive(Default)]
struct GateState {
    armed: bool,
    command: Option<Vec<u8>>,
    new_entries_only: bool,
    reached: bool,
    released: bool,
    discard_reply: bool,
    waker: Option<Waker>,
}

/// One-shot response gate for an already applied Redis command.
#[derive(Default)]
pub struct ReplyGate {
    state: Mutex<GateState>,
    changed: Condvar,
}

impl ReplyGate {
    /// Arms the next new-entry XREADGROUP reply and resets prior gate state.
    ///
    /// State locking may block briefly and panics if the gate mutex is
    /// poisoned.
    pub fn arm(&self) {
        self.arm_for_new_entries();
    }

    /// Arms the next applied reply for the ASCII command name `command`.
    ///
    /// The command is copied and normalized to uppercase; prior gate state is
    /// reset. State locking may block briefly and panics if the gate mutex
    /// is poisoned.
    pub fn arm_for(&self, command: &'static str) {
        let mut state = self.state.lock().expect("reply gate lock is healthy");
        state.armed = true;
        state.command = Some(command.as_bytes().to_ascii_uppercase());
        state.new_entries_only = false;
        state.reached = false;
        state.released = false;
        state.discard_reply = false;
        state.waker = None;
    }

    /// Selects new-entry XREADGROUP replies while leaving recovery reads
    /// ungated.
    ///
    /// Resets prior state under the mutex; a poisoned state mutex causes a
    /// panic.
    fn arm_for_new_entries(&self) {
        let mut state = self.state.lock().expect("reply gate lock is healthy");
        state.armed = true;
        state.command = Some(b"XREADGROUP".to_vec());
        state.new_entries_only = true;
        state.reached = false;
        state.released = false;
        state.discard_reply = false;
        state.waker = None;
    }

    /// Blocks until the applied reply reaches the gate or `timeout` expires.
    ///
    /// Returns true if the gate was reached, and false if it remains unreached.
    /// Panics if the state mutex or condition-variable wait is poisoned.
    pub fn wait_until_reached(&self, timeout: Duration) -> bool {
        let state = self.state.lock().expect("reply gate lock is healthy");
        let (state, _) = self
            .changed
            .wait_timeout_while(state, timeout, |state| !state.reached)
            .expect("reply gate lock is healthy");
        state.reached
    }

    /// Returns a future completing once Redis has produced the gated reply.
    ///
    /// Polling registers the current waker instead of waiting for the reply
    /// condition. Mutex locking may block briefly and panics if shared
    /// state is poisoned.
    pub fn wait_applied(&self) -> WaitApplied<'_> {
        WaitApplied { gate: self }
    }

    /// Releases the held response to the client and wakes blocking proxy
    /// workers.
    ///
    /// State locking may block briefly and panics if the gate mutex is
    /// poisoned.
    pub fn release(&self) {
        let mut state = self.state.lock().expect("reply gate lock is healthy");
        state.discard_reply = false;
        state.released = true;
        self.changed.notify_all();
    }

    /// Unblocks the proxy while discarding the held reply and closing that
    /// connection.
    ///
    /// State locking may block briefly and panics if the gate mutex is
    /// poisoned.
    pub fn release_without_reply(&self) {
        let mut state = self.state.lock().expect("reply gate lock is healthy");
        state.discard_reply = true;
        state.released = true;
        self.changed.notify_all();
    }

    /// Holds a matching applied reply until released and reports whether to
    /// discard it.
    ///
    /// `command` is the uppercase command name; `request` identifies new-entry
    /// reads. Returns false for an unmatched or delivered reply, and true
    /// for a discarded reply. Blocks on the condition variable and panics
    /// if the gate mutex is poisoned.
    pub(super) fn hold_if_armed(&self, command: &[u8], request: &[u8]) -> bool {
        let mut state = self.state.lock().expect("reply gate lock is healthy");
        if !state.armed || state.command.as_deref() != Some(command) {
            return false;
        }
        if state.new_entries_only && !request.contains(&b'>') {
            return false;
        }
        state.armed = false;
        state.reached = true;
        let waker = state.waker.take();
        self.changed.notify_all();
        drop(state);
        if let Some(waker) = waker {
            waker.wake();
        }
        let state = self.state.lock().expect("reply gate lock is healthy");
        let _state = self
            .changed
            .wait_while(state, |state| !state.released)
            .expect("reply gate lock is healthy");
        _state.discard_reply
    }
}

/// Future returned by [`ReplyGate::wait_applied`].
pub struct WaitApplied<'a> {
    gate: &'a ReplyGate,
}

impl Future for WaitApplied<'_> {
    type Output = ();

    /// Checks the applied gate and records `context`'s waker while it is
    /// pending.
    ///
    /// Returns Ready once reached, otherwise Pending without waiting for the
    /// reply. Mutex locking may block briefly.
    /// Panics if the shared gate state mutex is poisoned.
    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.gate.state.lock().expect("reply gate lock is healthy");
        if state.reached {
            Poll::Ready(())
        } else {
            state.waker = Some(context.waker().clone());
            Poll::Pending
        }
    }
}
