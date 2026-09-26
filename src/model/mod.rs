// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public request, state, resource, and result types for task execution.

mod resource;
mod task_id;
mod task_record;
mod task_request;

pub use resource::ResourceCapacity;
pub use resource::ResourceRequest;
pub use resource::ResourceSnapshot;
pub use task_id::TaskId;
pub use task_record::AcceptOutcome;
pub use task_record::MAX_TASK_QUERY_LIMIT;
pub use task_record::OwnerEpoch;
pub use task_record::StoreCapabilities;
pub use task_record::StoredTask;
pub use task_record::StoredTaskPage;
pub use task_record::TaskCursor;
pub use task_record::TaskPage;
pub use task_record::TaskQuery;
pub use task_record::TaskRecord;
pub use task_record::TaskState;
pub use task_record::TaskStateCounts;
pub use task_record::TaskStateKind;
pub use task_record::TaskStats;
pub use task_record::TransitionCommand;
pub(crate) use task_record::checked_page_size;
pub use task_request::MAX_CORRELATION_KEY_BYTES;
pub use task_request::MAX_HANDLER_VERSION_BYTES;
pub use task_request::MAX_IDEMPOTENCY_KEY_BYTES;
pub use task_request::MAX_TASK_DIAGNOSTIC_CATEGORY_BYTES;
pub use task_request::MAX_TASK_DIAGNOSTIC_MESSAGE_BYTES;
pub use task_request::MAX_TASK_METADATA_BYTES;
pub use task_request::MAX_TASK_METADATA_ENTRIES;
pub use task_request::MAX_TASK_METADATA_KEY_BYTES;
pub use task_request::MAX_TASK_METADATA_VALUE_BYTES;
pub use task_request::MAX_TASK_OUTPUT_SUMMARY_BYTES;
pub use task_request::MAX_TASK_PAYLOAD_BYTES;
pub use task_request::MAX_TASK_TYPE_BYTES;
pub use task_request::TaskOutput;
pub use task_request::TaskRequest;
pub use task_request::TaskRunError;
