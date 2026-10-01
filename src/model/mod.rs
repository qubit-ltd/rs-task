// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public request, state, resource, and result types for task execution.

#[cfg(test)]
mod accept_outcome;
mod owner_epoch;
#[cfg(test)]
mod recovery_page;
#[cfg(test)]
mod request_validation_error;
#[cfg(test)]
mod request_validation_field;
#[cfg(test)]
mod request_validation_rule;
mod resource_capacity;
#[cfg(test)]
mod resource_request;
#[cfg(test)]
mod resource_snapshot;
mod store_capabilities;
#[cfg(test)]
mod task_cursor;
#[cfg(test)]
mod task_id;
mod task_limits;
mod task_output;
#[cfg(test)]
mod task_page;
#[cfg(test)]
mod task_query;
#[cfg(test)]
mod task_record;
#[cfg(test)]
mod task_request;
#[cfg(test)]
mod task_request_info;
mod task_run_error;
mod task_state;
mod task_state_kind;
#[cfg(test)]
mod task_stats;
#[cfg(test)]
mod task_summary;
#[cfg(test)]
mod transition_command;
#[path = "next/mod.rs"]
pub mod typed;
#[cfg(test)]
pub(crate) use accept_outcome::AcceptOutcome;
pub use owner_epoch::OwnerEpoch;
#[cfg(test)]
pub(crate) use recovery_page::RecoveryPage;
#[cfg(test)]
pub use request_validation_error::RequestValidationError;
#[cfg(test)]
pub use request_validation_field::RequestValidationField;
#[cfg(test)]
pub use request_validation_rule::RequestValidationRule;
pub use resource_capacity::ResourceCapacity;
#[cfg(test)]
pub use resource_request::MAX_RESOURCE_DESCRIPTION_BYTES;
#[cfg(test)]
pub use resource_request::MAX_RESOURCE_NAME_BYTES;
#[cfg(test)]
pub use resource_request::MAX_RESOURCE_NAME_ENTRIES;
#[cfg(test)]
pub(crate) use resource_request::ResourceRequest;
#[cfg(test)]
pub use resource_snapshot::ResourceSnapshot;
pub use store_capabilities::StoreCapabilities;
#[cfg(test)]
pub(crate) use task_cursor::TaskCursor;
#[cfg(test)]
pub(crate) use task_id::TaskId;
pub use task_limits::MAX_TASK_PAYLOAD_BYTES;
pub use task_output::MAX_TASK_OUTPUT_SUMMARY_BYTES;
pub use task_output::TaskOutput;
#[cfg(test)]
pub(crate) use task_page::TaskPage;
#[cfg(test)]
pub(crate) use task_query::TaskQuery;
#[cfg(test)]
pub(crate) use task_query::checked_page_size;
#[cfg(test)]
pub(crate) use task_record::TaskRecord;
#[cfg(test)]
pub use task_request::MAX_CORRELATION_KEY_BYTES;
#[cfg(test)]
pub(crate) use task_request::TaskRequest;
#[cfg(test)]
pub(crate) use task_request_info::TaskRequestInfo;
pub use task_run_error::MAX_TASK_DIAGNOSTIC_CATEGORY_BYTES;
pub use task_run_error::MAX_TASK_DIAGNOSTIC_MESSAGE_BYTES;
pub use task_run_error::TaskRunError;
pub use task_state::TaskState;
pub use task_state_kind::TaskStateKind;
#[cfg(test)]
pub use task_stats::TaskStats;
#[cfg(test)]
pub(crate) use task_summary::TaskSummary;
#[cfg(test)]
pub(crate) use transition_command::TransitionCommand;
pub(crate) use typed as next;
#[cfg(not(test))]
pub use typed::AcceptOutcome;
#[cfg(not(test))]
pub use typed::EncodedPayload;
#[cfg(not(test))]
pub use typed::MAX_TASK_METADATA_BYTES;
#[cfg(not(test))]
pub use typed::MAX_TASK_METADATA_ENTRIES;
#[cfg(not(test))]
pub use typed::MAX_TASK_PROGRESS_METRICS;
#[cfg(not(test))]
pub use typed::MAX_TASK_PROGRESS_SNAPSHOT_BYTES;
#[cfg(not(test))]
pub use typed::Payload;
#[cfg(not(test))]
pub use typed::PayloadEncodeError;
#[cfg(not(test))]
pub use typed::ProgressCommand;
#[cfg(not(test))]
pub use typed::ProgressMetricSnapshot;
#[cfg(not(test))]
pub use typed::ProgressSnapshotError;
#[cfg(not(test))]
pub use typed::ProgressStageSnapshot;
#[cfg(not(test))]
pub use typed::ResourceRequest;
#[cfg(not(test))]
pub use typed::StartCommand;
#[cfg(not(test))]
pub use typed::StoredPayload;
#[cfg(not(test))]
pub use typed::StoredTask;
#[cfg(not(test))]
pub use typed::StoredTaskRequest;
#[cfg(not(test))]
pub use typed::TaskCursor;
#[cfg(not(test))]
pub use typed::TaskId;
#[cfg(not(test))]
pub use typed::TaskPage;
#[cfg(not(test))]
pub use typed::TaskProgressSnapshot;
#[cfg(not(test))]
pub use typed::TaskQuery;
#[cfg(not(test))]
pub use typed::TaskRequest;
#[cfg(not(test))]
pub use typed::TaskRequestEncodeError;
#[cfg(not(test))]
pub use typed::TaskSummary;
#[cfg(not(test))]
pub use typed::TransitionCommand;
