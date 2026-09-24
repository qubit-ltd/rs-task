// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Resource-aware asynchronous task execution with pluggable storage and
//! scheduling.

pub mod engine;
pub mod handler;
pub mod model;
pub mod scheduling;
pub mod service;
pub mod spi;
pub mod store;

#[cfg(feature = "event-bus")]
pub mod event;

pub use engine::TaskExecutionEngine;
pub use handler::TaskHandler;
pub use model::TaskId;
pub use model::TaskRecord;
pub use model::TaskRequest;
pub use service::TaskExecutionService;
pub use service::TaskExecutionServiceBuilder;
pub use store::TaskStore;
