// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Focused cancellation acceptance regressions using the typed service fixture.
mod service {
    mod typed_task_execution_tests;
}

mod support;

#[cfg(feature = "event-bus")]
pub use support::task_event_codec;
