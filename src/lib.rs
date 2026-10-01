// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Resource-aware asynchronous task execution with pluggable storage and
//! scheduling.

#[cfg(all(feature = "conformance", test))]
mod conformance;

pub(crate) mod engine;
pub mod handler;
pub mod model;
#[cfg(test)]
mod scheduling;
pub mod service;
#[cfg(test)]
mod spi;
pub mod store;

#[cfg(feature = "event-bus")]
pub mod event;

#[cfg(not(test))]
pub use handler::CancellationMode;
#[cfg(not(test))]
pub use handler::ExternalCancellationHook;
#[cfg(not(test))]
pub use handler::HandlerDispatchError;
#[cfg(not(test))]
pub use handler::HandlerRegistrationError;
#[cfg(not(test))]
pub use handler::TaskContext;
#[cfg(not(test))]
pub use handler::TaskHandler;
#[cfg(not(test))]
pub use handler::TaskHandlerDescriptor;
#[cfg(not(test))]
pub use handler::TaskHandlerRegistry;
#[cfg(not(test))]
pub use model::EncodedPayload;
pub use model::MAX_TASK_OUTPUT_SUMMARY_BYTES;
#[cfg(not(test))]
pub use model::Payload;
#[cfg(not(test))]
pub use model::ResourceRequest;
#[cfg(not(test))]
pub use model::TaskId;
pub use model::TaskOutput;
#[cfg(not(test))]
pub use model::TaskPage;
#[cfg(not(test))]
pub use model::TaskQuery;
#[cfg(not(test))]
pub use model::TaskRequest;
#[cfg(not(test))]
pub use model::TaskSummary;
#[cfg(not(test))]
pub use service::TaskExecutionService;
#[cfg(not(test))]
pub use service::TaskExecutionServiceBuilder;
pub use store::TaskStore;

#[cfg(test)]
mod tests;

#[cfg(all(test, feature = "sqlite"))]
pub(crate) use tests::common::sqlite_paths;
