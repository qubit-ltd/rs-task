// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use qubit_codec::ValueCodecId;
use qubit_model_id::HasModelId;

use super::TaskHandlerDescriptor;
use crate::model::typed::TaskRequest;

/// The current payload schema and codec for one task kind.
///
/// This value creates descriptors and typed requests using one stable kind,
/// schema version, and codec identity.
///
/// # Type Parameters
///
/// * `T` is the payload type and supplies its model identity through
///   [`HasModelId`].
///
/// # Examples
///
/// ```
/// use qubit_codec::ValueCodecId;
/// use qubit_model_id::HasModelId;
/// use qubit_model_id::ModelId;
/// use qubit_task::TaskKind;
///
/// struct Payload;
/// impl HasModelId for Payload {
///     const MODEL_ID: ModelId = ModelId::new("example.Payload");
/// }
///
/// let kind = TaskKind::<Payload>::new("email.send", 1, ValueCodecId::new("example.codec"));
/// assert_eq!(kind.descriptor().kind_id(), "email.send");
/// ```
#[derive(Debug, Clone)]
pub struct TaskKind<T> {
    /// Stable handler routing identity.
    kind_id: String,
    /// Payload schema version encoded by requests created from this kind.
    schema: u32,
    /// Codec identity used to encode requests created from this kind.
    codec: ValueCodecId,
    /// Associates this value with its payload type without storing a payload.
    marker: std::marker::PhantomData<fn() -> T>,
}

impl<T: HasModelId> TaskKind<T> {
    /// Creates a task kind using the payload type's model ID.
    ///
    /// # Parameters
    ///
    /// * `kind` is the stable handler routing identity.
    /// * `schema` is the payload schema version.
    /// * `codec` identifies the payload codec.
    ///
    /// # Returns
    ///
    /// A typed kind definition using the payload type's model ID.
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
    ///
    /// # Returns
    ///
    /// A handler descriptor for this kind, payload type, and schema version.
    #[must_use]
    pub fn descriptor(&self) -> TaskHandlerDescriptor {
        TaskHandlerDescriptor::for_payload::<T>(self.kind_id.clone(), self.schema)
    }

    /// Creates a typed request for this task kind.
    ///
    /// # Parameters
    ///
    /// * `data` is the application payload value to submit.
    ///
    /// # Returns
    ///
    /// A request configured with this kind's identity, schema, and codec.
    #[must_use]
    pub fn request(&self, data: T) -> TaskRequest<T> {
        TaskRequest::new(self.kind_id.clone(), self.schema, self.codec, data)
    }
}
