// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! JSON task-event codec compiled for both user guides.

// guide-codec-start
use std::sync::Arc;

use qubit_event_bus::CodecError;
use qubit_event_bus::codec::EventCodec;
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::SchemaId;
use qubit_event_bus::spi::EncodedPayload;
use qubit_task::event::TaskEvent;

/// Minimal JSON codec used by this provider-discovery fixture.
pub struct TaskEventJsonCodec {
    content_type: ContentType,
    schema_id: SchemaId,
}

impl TaskEventJsonCodec {
    /// Builds the codec with valid content-type and schema identifiers.
    pub fn new() -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self {
            content_type: ContentType::new("application/json")?,
            schema_id: SchemaId::new("task-event-v1")?,
        })
    }
}

impl EventCodec<TaskEvent> for TaskEventJsonCodec {
    fn content_type(&self) -> &ContentType {
        &self.content_type
    }

    fn schema_id(&self) -> Option<&SchemaId> {
        Some(&self.schema_id)
    }

    /// Accepts the v1 JSON contract and its historical schema-less encoding.
    /// Other MIME texts and schema identifiers are configuration mismatches.
    fn validate_metadata(&self, payload: &EncodedPayload) -> Result<(), CodecError> {
        if payload.content_type() == &self.content_type
            && (payload.schema_id().is_none() || payload.schema_id() == Some(&self.schema_id))
        {
            return Ok(());
        }
        Err(CodecError::MetadataMismatch {
            expected_content_type: self.content_type.clone(),
            actual_content_type: payload.content_type().clone(),
            expected_schema_id: Some(self.schema_id.clone()),
            actual_schema_id: payload.schema_id().cloned(),
        })
    }

    fn encode(&self, value: &TaskEvent) -> Result<Arc<[u8]>, CodecError> {
        serde_json::to_vec(value)
            .map(Arc::from)
            .map_err(|source| CodecError::Encode { source: Box::new(source) })
    }

    fn decode(&self, payload: &EncodedPayload) -> Result<TaskEvent, CodecError> {
        serde_json::from_slice(payload.bytes()).map_err(|source| CodecError::Decode { source: Box::new(source) })
    }
}
// guide-codec-end
