// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Resource-aware asynchronous task execution with pluggable storage and
//! scheduling.

pub(crate) mod engine;
pub mod handler;
pub mod model;
pub mod service;
pub mod store;

#[cfg(feature = "event-bus")]
pub mod event;

pub use handler::CancellationMode;
pub use handler::ExternalCancellationHook;
pub use handler::HandlerDispatchError;
pub use handler::HandlerRegistrationError;
pub use handler::TaskContext;
pub use handler::TaskHandler;
pub use handler::TaskHandlerDescriptor;
pub use handler::TaskHandlerRegistry;
pub use model::MAX_TASK_OUTPUT_SUMMARY_BYTES;
pub use model::TaskOutput;
pub use model::typed::EncodedPayload;
pub use model::typed::Payload;
pub use model::typed::ResourceRequest;
pub use model::typed::TaskId;
pub use model::typed::TaskPage;
pub use model::typed::TaskQuery;
pub use model::typed::TaskRequest;
pub use model::typed::TaskSummary;
pub use service::RetryPolicy;
pub use service::RetryPolicyError;
pub use service::TaskExecutionService;
pub use service::TaskExecutionServiceBuilder;
pub use store::TaskStore;
