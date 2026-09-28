// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public request, state, resource, and result types for task execution.

mod accept_outcome;
mod owner_epoch;
mod recovery_page;
mod resource_capacity;
mod resource_request;
mod resource_snapshot;
mod store_capabilities;
mod task_cursor;
mod task_id;
mod task_output;
mod task_page;
mod task_query;
mod task_record;
mod task_request;
mod task_request_info;
mod task_run_error;
mod task_state;
mod task_state_counts;
mod task_state_kind;
mod task_stats;
mod task_summary;
mod transition_command;

pub use accept_outcome::AcceptOutcome;
pub use owner_epoch::OwnerEpoch;
pub use recovery_page::RecoveryPage;
pub use resource_capacity::ResourceCapacity;
pub use resource_request::MAX_RESOURCE_DESCRIPTION_BYTES;
pub use resource_request::MAX_RESOURCE_NAME_BYTES;
pub use resource_request::MAX_RESOURCE_NAME_ENTRIES;
pub use resource_request::ResourceRequest;
pub use resource_snapshot::ResourceSnapshot;
pub use store_capabilities::StoreCapabilities;
pub use task_cursor::TaskCursor;
pub use task_id::TaskId;
pub use task_output::MAX_TASK_OUTPUT_SUMMARY_BYTES;
pub use task_output::TaskOutput;
pub use task_page::TaskPage;
pub use task_query::MAX_TASK_QUERY_LIMIT;
pub use task_query::TaskQuery;
pub(crate) use task_query::checked_page_size;
pub use task_record::TaskRecord;
pub use task_request::MAX_CORRELATION_KEY_BYTES;
pub use task_request::MAX_HANDLER_VERSION_BYTES;
pub use task_request::MAX_IDEMPOTENCY_KEY_BYTES;
pub use task_request::MAX_TASK_METADATA_BYTES;
pub use task_request::MAX_TASK_METADATA_ENTRIES;
pub use task_request::MAX_TASK_METADATA_KEY_BYTES;
pub use task_request::MAX_TASK_METADATA_VALUE_BYTES;
pub use task_request::MAX_TASK_PAYLOAD_BYTES;
pub use task_request::MAX_TASK_TYPE_BYTES;
pub use task_request::TaskRequest;
pub use task_request_info::TaskRequestInfo;
pub use task_run_error::MAX_TASK_DIAGNOSTIC_CATEGORY_BYTES;
pub use task_run_error::MAX_TASK_DIAGNOSTIC_MESSAGE_BYTES;
pub use task_run_error::TaskRunError;
pub use task_state::TaskState;
pub use task_state_counts::TaskStateCounts;
pub use task_state_kind::TaskStateKind;
pub use task_stats::TaskStats;
pub use task_summary::TaskSummary;
pub use transition_command::TransitionCommand;
