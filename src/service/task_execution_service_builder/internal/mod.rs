// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Private implementation details for the in-memory service builder.

// Provides the monotonic ID source used by the in-memory builder constructor.
mod in_memory_id_generator;

pub(super) use in_memory_id_generator::InMemoryIdGenerator;
