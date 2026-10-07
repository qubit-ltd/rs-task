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
#[cfg(test)]
mod validation_coverage_tests;
mod task_run_error;
mod task_state;
mod task_state_kind;
#[cfg(test)]
mod task_summary;
#[cfg(test)]
mod transition_command;
#[path = "next/mod.rs"]
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
pub(crate) use typed as next;
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

/// Models used only by the legacy store unit-test adapter.
#[cfg(test)]
pub(crate) mod legacy {
    pub(crate) use super::accept_outcome::AcceptOutcome;
    pub(crate) use super::recovery_page::RecoveryPage;
    pub(crate) use super::request_validation_error::RequestValidationError;
    pub(crate) use super::request_validation_field::RequestValidationField;
    pub(crate) use super::request_validation_rule::RequestValidationRule;
    pub(crate) use super::resource_request::ResourceRequest;
    pub(crate) use super::task_cursor::TaskCursor;
    pub(crate) use super::task_id::TaskId;
    pub(crate) use super::task_page::TaskPage;
    pub(crate) use super::task_query::TaskQuery;
    pub(crate) use super::task_query::checked_page_size;
    pub(crate) use super::task_record::TaskRecord;
    pub(crate) use super::task_request::TaskRequest;
    pub(crate) use super::task_request_info::TaskRequestInfo;
    pub(crate) use super::task_summary::TaskSummary;
    pub(crate) use super::transition_command::TransitionCommand;
}
