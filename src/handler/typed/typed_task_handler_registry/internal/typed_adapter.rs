// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::any::type_name;
use std::marker::PhantomData;
use std::sync::Arc;

use qubit_codec::ValueBytesCodecRegistry;

use super::ErasedHandler;
use crate::handler::typed::HandlerDispatchError;
use crate::handler::typed::PreparedTask;
use crate::handler::typed::TaskHandlerDescriptor;
use crate::handler::typed::TypedTaskHandler;
use crate::model::typed::StoredPayload;

/// Adapts a typed handler to the registry's type-erased dispatch interface.
pub(in crate::handler::typed::typed_task_handler_registry) struct TypedAdapter<T, H> {
    /// Descriptor used to validate the payload before decoding.
    pub(in crate::handler::typed::typed_task_handler_registry) descriptor: TaskHandlerDescriptor,
    /// Handler retained for execution after successful preparation.
    pub(in crate::handler::typed::typed_task_handler_registry) handler: Arc<H>,
    /// Records the payload type expected by `H` without storing a value of `T`.
    pub(in crate::handler::typed::typed_task_handler_registry) marker: PhantomData<fn() -> T>,
}

impl<T, H> ErasedHandler for TypedAdapter<T, H>
where
    T: Send + Sync + 'static,
    H: TypedTaskHandler<T>,
{
    /// Validates and decodes an owned payload before creating its handler task.
    ///
    /// The payload is consumed during preparation, while the codec registry is
    /// borrowed only for lookup and decoding. The returned task owns the
    /// decoded value and a shared handler reference, so it can run after this
    /// method returns.
    ///
    /// # Parameters
    ///
    /// * `payload` is the stored value whose model, schema, codec, and decoded
    ///   Rust type must match this adapter.
    /// * `codecs` provides the registered decoder for the payload's codec ID.
    ///
    /// # Returns
    ///
    /// Returns a prepared task that invokes the typed handler with the decoded
    /// payload.
    ///
    /// # Errors
    ///
    /// Returns an error if the payload model or schema is incompatible, its
    /// codec is missing or fails, or the decoded value has a different Rust
    /// type.
    fn prepare(
        &self,
        payload: StoredPayload,
        codecs: &ValueBytesCodecRegistry,
    ) -> Result<PreparedTask, HandlerDispatchError> {
        if payload.type_id != self.descriptor.payload_type_id {
            return Err(HandlerDispatchError::PayloadTypeMismatch {
                expected: self.descriptor.payload_type_id.to_string(),
                actual: payload.type_id.to_string(),
            });
        }
        if !self
            .descriptor
            .accepted_schema_versions
            .contains(&payload.schema_version)
        {
            return Err(HandlerDispatchError::UnsupportedSchemaVersion(payload.schema_version));
        }
        let codec = codecs
            .get(&payload.codec_id)
            .ok_or_else(|| HandlerDispatchError::MissingCodec(payload.codec_id.clone()))?;
        let decoded = codec.descriptor().decode(&payload.bytes)?;
        let actual_type = decoded.as_ref().type_id();
        let input = decoded
            .downcast::<T>()
            .map_err(|_| HandlerDispatchError::DecodedValueTypeMismatch {
                expected: type_name::<T>(),
                actual: actual_type,
            })?;
        let input = *input;
        let handler = Arc::clone(&self.handler);
        Ok(PreparedTask::new(Box::new(move |context| {
            Box::pin(async move { handler.run(input, context).await })
        })))
    }
}
