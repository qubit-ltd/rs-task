// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Private built-in provider implementations.

mod fair_fifo_provider;
mod local_engine_provider;
mod memory_store_provider;
#[cfg(feature = "sqlite")]
mod sqlite_store_provider;

pub(super) use fair_fifo_provider::FairFifoProvider;
pub(super) use local_engine_provider::LocalEngineProvider;
pub(super) use memory_store_provider::MemoryStoreProvider;
#[cfg(feature = "sqlite")]
pub(super) use sqlite_store_provider::SqliteStoreProvider;
