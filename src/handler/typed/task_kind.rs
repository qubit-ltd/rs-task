// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
// =============================================================================
use qubit_codec::ValueCodecId;
use qubit_model_id::HasModelId;

use super::TaskHandlerDescriptor;
use crate::model::typed::TaskRequest;

/// The current payload schema and codec for one task kind.
#[derive(Debug, Clone)]
pub struct TaskKind<T> {
    kind_id: String,
    schema: u32,
    codec: ValueCodecId,
    marker: std::marker::PhantomData<fn() -> T>,
}

impl<T: HasModelId> TaskKind<T> {
    /// Creates a task kind using the payload type's model ID.
    #[must_use]
    pub fn new(kind: impl Into<String>, schema: u32, codec: ValueCodecId) -> Self {
        Self {
            kind_id: kind.into(),
            schema,
            codec,
            marker: std::marker::PhantomData,
        }
    }

    /// Returns a descriptor accepting this kind's current schema.
    #[must_use]
    pub fn descriptor(&self) -> TaskHandlerDescriptor {
        TaskHandlerDescriptor::for_payload::<T>(self.kind_id.clone(), self.schema)
    }

    /// Creates a typed request for this task kind.
    #[must_use]
    pub fn request(&self, data: T) -> TaskRequest<T> {
        TaskRequest::new(self.kind_id.clone(), self.schema, self.codec, data)
    }
}
