// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
// Tracks volatile records and the retention accounting updated on eviction.
mod memory_state;

pub(in crate::store::memory) use memory_state::MemoryState;
