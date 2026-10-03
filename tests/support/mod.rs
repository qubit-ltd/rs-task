// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
// Shared across separate integration-test crates; some test targets only use a
// subset.
#[allow(dead_code)]
pub mod store_contract;

#[cfg(feature = "event-bus")]
#[path = "../fixtures/doc-examples/src/task_event_codec.rs"]
#[allow(dead_code)]
pub mod task_event_codec;
