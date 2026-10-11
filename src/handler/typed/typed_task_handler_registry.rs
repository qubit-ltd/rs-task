// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

use qubit_codec::ValueBytesCodecRegistry;

use super::cancellation_mode::CancellationMode;
use super::external_cancellation_hook::ExternalCancellationHook;
use super::handler_dispatch_error::HandlerDispatchError;
use super::handler_registration_error::HandlerRegistrationError;
use super::prepared_task::PreparedTask;
use super::task_handler_descriptor::TaskHandlerDescriptor;
use super::typed_task_handler::TypedTaskHandler;
use crate::model::TaskRunError;
use crate::model::typed::StoredPayload;
use crate::model::typed::TaskId;
use crate::store::TaskFuture;

/// Type-erased boundary used to prepare a task after registry lookup.
trait ErasedHandler: Send + Sync {
    /// Validates and decodes the stored payload, then creates a runnable task.
    fn prepare(
        &self,
        payload: StoredPayload,
        codecs: &ValueBytesCodecRegistry,
    ) -> Result<PreparedTask, HandlerDispatchError>;
}

/// Adapts a typed handler to the registry's type-erased dispatch interface.
struct TypedAdapter<T, H> {
    /// Descriptor used to validate the payload before decoding.
    descriptor: TaskHandlerDescriptor,
    /// Handler retained for execution after successful preparation.
    handler: Arc<H>,
    /// Records the payload type expected by `H` without storing a value of `T`.
    marker: PhantomData<fn() -> T>,
}

impl<T, H> ErasedHandler for TypedAdapter<T, H>
where
    T: Send + Sync + 'static,
    H: TypedTaskHandler<T>,
{
    /// Rejects mismatched metadata and creates a future that runs the typed
    /// handler.
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
                expected: std::any::type_name::<T>(),
                actual: actual_type,
            })?;
        let input = *input;
        let handler = Arc::clone(&self.handler);
        Ok(PreparedTask::new(Box::new(move |context| {
            Box::pin(async move { handler.run(input, context).await })
        })))
    }
}

/// Stored metadata and handler state for one registered task kind.
struct Registration {
    /// Source label reported if a duplicate kind is registered.
    source: String,
    /// Descriptor exposed to callers and used during dispatch validation.
    descriptor: TaskHandlerDescriptor,
    /// Type-erased handler implementation used to prepare payloads.
    handler: Arc<dyn ErasedHandler>,
    /// Optional hook for kinds configured for external cancellation.
    external_cancellation_hook: Option<ExternalCancellationHook>,
}

/// Registry routing one handler per stable kind identifier.
#[derive(Default)]
pub struct TypedTaskHandlerRegistry {
    /// Registered handler state keyed by each descriptor's stable kind ID.
    handlers: HashMap<String, Registration>,
}

impl TypedTaskHandlerRegistry {
    /// Creates an empty typed handler registry.
    #[must_use]
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers one typed handler from a direct application registration.
    ///
    /// # Errors
    ///
    /// Returns an error if the descriptor is invalid or its kind identifier is
    /// already registered.
    pub fn register<T, H>(
        &mut self,
        descriptor: TaskHandlerDescriptor,
        handler: Arc<H>,
    ) -> Result<(), HandlerRegistrationError>
    where
        T: Send + Sync + 'static,
        H: TypedTaskHandler<T>,
    {
        self.register_with_options::<T, H>(descriptor, handler, "direct registration", None)
    }

    /// Registers one typed handler and retains its source for conflict errors.
    ///
    /// # Errors
    ///
    /// Returns an error if the descriptor is invalid or its kind identifier is
    /// already registered.
    pub fn register_with_source<T, H>(
        &mut self,
        descriptor: TaskHandlerDescriptor,
        handler: Arc<H>,
        source: impl Into<String>,
    ) -> Result<(), HandlerRegistrationError>
    where
        T: Send + Sync + 'static,
        H: TypedTaskHandler<T>,
    {
        self.register_with_options::<T, H>(descriptor, handler, source, None)
    }

    /// Registers a typed handler with an external cancellation hook when its
    /// descriptor declares [`CancellationMode::ExternalHook`].
    ///
    /// # Errors
    ///
    /// Returns an error if the descriptor is invalid, the hook does not match
    /// the cancellation mode, or the kind identifier is already registered.
    pub fn register_with_cancellation_hook<T, H>(
        &mut self,
        descriptor: TaskHandlerDescriptor,
        handler: Arc<H>,
        source: impl Into<String>,
        hook: ExternalCancellationHook,
    ) -> Result<(), HandlerRegistrationError>
    where
        T: Send + Sync + 'static,
        H: TypedTaskHandler<T>,
    {
        self.register_with_options::<T, H>(descriptor, handler, source, Some(hook))
    }

    /// Validates registration options and inserts the handler if its kind is
    /// free.
    fn register_with_options<T, H>(
        &mut self,
        descriptor: TaskHandlerDescriptor,
        handler: Arc<H>,
        source: impl Into<String>,
        external_cancellation_hook: Option<ExternalCancellationHook>,
    ) -> Result<(), HandlerRegistrationError>
    where
        T: Send + Sync + 'static,
        H: TypedTaskHandler<T>,
    {
        descriptor.validate()?;
        match (descriptor.cancellation_mode, external_cancellation_hook.is_some()) {
            (CancellationMode::ExternalHook, false) => {
                return Err(HandlerRegistrationError::MissingExternalHook(descriptor.kind_id));
            }
            (CancellationMode::ExternalHook, true) => {}
            (_, true) => {
                return Err(HandlerRegistrationError::UnexpectedExternalHook(descriptor.kind_id));
            }
            (_, false) => {}
        }
        let source = source.into();
        if let Some(existing) = self.handlers.get(&descriptor.kind_id) {
            return Err(HandlerRegistrationError::DuplicateKindId {
                kind_id: descriptor.kind_id,
                first_source: existing.source.clone(),
                second_source: source,
            });
        }
        let kind_id = descriptor.kind_id.clone();
        let erased: Arc<dyn ErasedHandler> = Arc::new(TypedAdapter::<T, H> {
            descriptor: descriptor.clone(),
            handler,
            marker: PhantomData,
        });
        self.handlers.insert(
            kind_id,
            Registration {
                source,
                descriptor,
                handler: erased,
                external_cancellation_hook,
            },
        );
        Ok(())
    }

    /// Validates and decodes a stored payload before any handler code runs.
    ///
    /// # Errors
    ///
    /// Returns an error if no handler is registered or if the payload metadata,
    /// schema, codec, or decoded value is incompatible with that handler.
    pub fn prepare(
        &self,
        kind_id: &str,
        payload: StoredPayload,
        codecs: &ValueBytesCodecRegistry,
    ) -> Result<PreparedTask, HandlerDispatchError> {
        self.handlers
            .get(kind_id)
            .ok_or_else(|| HandlerDispatchError::MissingHandler(kind_id.to_owned()))?
            .handler
            .prepare(payload, codecs)
    }

    /// Returns the registered descriptor for a handler kind.
    #[must_use]
    #[inline]
    pub fn descriptor(&self, kind_id: &str) -> Option<&TaskHandlerDescriptor> {
        self.handlers.get(kind_id).map(|registration| &registration.descriptor)
    }

    /// Returns whether the kind has an external cancellation hook.
    #[must_use]
    #[inline]
    pub fn has_external_cancellation_hook(&self, kind_id: &str) -> bool {
        self.handlers
            .get(kind_id)
            .is_some_and(|registration| registration.external_cancellation_hook.is_some())
    }

    /// Calls an external cancellation hook, if registered for this kind.
    #[must_use]
    pub fn cancel_externally(
        &self,
        kind_id: &str,
        task_id: TaskId,
        attempt: u32,
    ) -> Option<TaskFuture<'static, Result<(), TaskRunError>>> {
        self.handlers
            .get(kind_id)
            .and_then(|registration| registration.external_cancellation_hook.as_ref())
            .map(|hook| hook(task_id, attempt))
    }
}
