// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public request, state, resource, and result types for task execution.

mod owner_epoch;
mod resource_capacity;
mod store_capabilities;
mod task_limits;
mod task_output;
mod task_run_error;
mod task_state;
mod task_state_kind;
pub mod typed;
pub use owner_epoch::OwnerEpoch;
pub use resource_capacity::ResourceCapacity;
pub use store_capabilities::StoreCapabilities;
pub use task_limits::MAX_TASK_PAYLOAD_BYTES;
pub use task_output::MAX_TASK_OUTPUT_SUMMARY_BYTES;
pub use task_output::TaskOutput;
pub use task_run_error::MAX_TASK_DIAGNOSTIC_CATEGORY_BYTES;
pub use task_run_error::MAX_TASK_DIAGNOSTIC_MESSAGE_BYTES;
pub use task_run_error::TaskRunError;
pub use task_state::TaskState;
pub use task_state_kind::TaskStateKind;
pub use typed::AcceptOutcome;
pub use typed::EncodedPayload;
pub use typed::MAX_TASK_METADATA_BYTES;
pub use typed::MAX_TASK_METADATA_ENTRIES;
pub use typed::MAX_TASK_PROGRESS_METRICS;
pub use typed::MAX_TASK_PROGRESS_SNAPSHOT_BYTES;
pub use typed::MAX_TASK_QUERY_LIMIT;
pub use typed::Payload;
pub use typed::PayloadEncodeError;
pub use typed::ProgressCommand;
pub use typed::ProgressMetricSnapshot;
pub use typed::ProgressSnapshotError;
pub use typed::ProgressStageSnapshot;
pub use typed::ResourceRequest;
pub use typed::StartCommand;
pub use typed::StoredPayload;
pub use typed::StoredTask;
pub use typed::StoredTaskRequest;
pub use typed::TaskCursor;
pub use typed::TaskId;
pub use typed::TaskPage;
pub use typed::TaskProgressSnapshot;
pub use typed::TaskQuery;
pub use typed::TaskRequest;
pub use typed::TaskRequestEncodeError;
pub use typed::TaskSummary;
pub use typed::TransitionCommand;
