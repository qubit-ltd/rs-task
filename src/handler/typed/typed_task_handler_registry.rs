// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;

use qubit_codec::ValueBytesCodecRegistry;
use qubit_model_id::HasModelId;
use qubit_model_id::ModelIdBuf;

use self::internal::ErasedHandler;
use self::internal::Registration;
use self::internal::TypedAdapter;
use super::CancellationMode;
use super::HandlerDispatchError;
use super::HandlerOptions;
use super::HandlerRegistrationError;
use super::PreparedTask;
use super::TaskHandlerDescriptor;
use super::TypedTaskContext;
use super::TypedTaskHandler;
use crate::model::TaskAttempt;
use crate::model::TaskRunError;
use crate::model::typed::StoredPayload;
use crate::model::typed::TaskId;
use crate::store::TaskFuture;

// Stores and retrieves one implementation for each stable handler kind.
mod internal;

/// Registry routing one handler per stable kind identifier.
///
/// Registration validates the descriptor and registration options
/// before retaining the handler. During dispatch, the registry checks payload
/// metadata and decodes the value before invoking application code.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
///
/// use qubit_model_id::HasModelId;
/// use qubit_model_id::ModelId;
/// use qubit_task::HandlerOptions;
/// use qubit_task::TaskHandler;
/// use qubit_task::TaskHandlerRegistry;
/// use qubit_task::TaskKind;
/// use qubit_task::handler::TaskRunOutcome;
/// use qubit_task::handler::TaskRunResult;
/// use qubit_task::store::TaskFuture;
/// use qubit_task::TaskContext;
/// use qubit_task::TaskOutput;
///
/// struct Payload;
/// impl HasModelId for Payload {
///     const MODEL_ID: ModelId = ModelId::new("example.Payload");
/// }
/// struct Handler;
///
/// impl TaskHandler<Payload> for Handler {
///     fn run<'a>(&'a self, _input: Payload, _context: TaskContext) -> TaskFuture<'a, TaskRunResult> {
///         Box::pin(async { Ok(TaskRunOutcome::Succeeded(TaskOutput::default())) })
///     }
/// }
///
/// let descriptor = TaskKind::<Payload>::new("email.send", 1, qubit_codec::ValueCodecId::new("example.codec"))
///     .descriptor();
/// let mut registry = TaskHandlerRegistry::new();
/// registry
///     .register_with_options::<Payload, _>(descriptor, Arc::new(Handler),
///         HandlerOptions::new().with_cooperative_cancellation())
///     .expect("valid handler registration");
/// ```
#[derive(Default)]
pub struct TypedTaskHandlerRegistry {
    /// Registered handler state keyed by each descriptor's stable kind ID.
    handlers: HashMap<String, Registration>,
}

impl TypedTaskHandlerRegistry {
    /// Creates an empty typed handler registry.
    ///
    /// # Returns
    ///
    /// An empty registry ready to accept handler registrations.
    #[must_use]
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the registered descriptor for a handler kind.
    ///
    /// # Parameters
    ///
    /// * `kind_id` is the stable routing key to look up.
    ///
    /// # Returns
    ///
    /// `Some` borrows the registered descriptor; `None` means no handler uses
    /// that kind ID.
    #[inline]
    pub fn descriptor(&self, kind_id: &str) -> Option<&TaskHandlerDescriptor> {
        self.handlers.get(kind_id).map(|registration| &registration.descriptor)
    }

    /// Removes a registered kind and all of its associated handler metadata.
    pub fn unregister(&mut self, kind_id: &str) -> bool {
        self.handlers.remove(kind_id).is_some()
    }

    /// Returns the cancellation mode for a registered kind.
    pub fn cancellation_mode(&self, kind_id: &str) -> Option<CancellationMode> {
        self.handlers
            .get(kind_id)
            .map(|registration| registration.cancellation_mode)
    }

    /// Returns whether the kind has an external cancellation hook.
    ///
    /// # Parameters
    ///
    /// * `kind_id` is the stable routing key to inspect.
    ///
    /// # Returns
    ///
    /// `true` when that kind has a hook, or `false` when it has no handler or
    /// no hook.
    #[must_use]
    #[inline]
    pub fn has_external_cancellation_hook(&self, kind_id: &str) -> bool {
        self.handlers
            .get(kind_id)
            .is_some_and(|registration| registration.external_cancellation_hook.is_some())
    }

    /// Registers one typed handler from a direct application registration.
    ///
    /// # Type Parameters
    ///
    /// * `T` is the decoded payload type passed to the handler.
    /// * `H` is the application handler implementation.
    ///
    /// # Parameters
    ///
    /// * `descriptor` declares the stable kind, payload schema, and
    ///   cancellation mode.
    /// * `handler` is retained and shared for dispatches of this kind.
    ///
    /// # Returns
    ///
    /// `Ok(())` when registration succeeds.
    ///
    /// # Errors
    ///
    /// Returns an error if the descriptor is invalid, external cancellation
    /// requires a hook, or its kind identifier is already registered.
    #[inline]
    pub fn register<T, H>(
        &mut self,
        descriptor: TaskHandlerDescriptor,
        handler: Arc<H>,
    ) -> Result<(), HandlerRegistrationError>
    where
        T: HasModelId + Send + Sync + 'static,
        H: TypedTaskHandler<T>,
    {
        self.register_with_options::<T, H>(descriptor, handler, HandlerOptions::new())
    }

    /// Registers an async function handler without a handler struct.
    pub fn register_handler_fn<T, F, Fut>(
        &mut self,
        descriptor: TaskHandlerDescriptor,
        handler: F,
    ) -> Result<(), HandlerRegistrationError>
    where
        T: HasModelId + Send + Sync + 'static,
        F: Fn(T, TypedTaskContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = crate::handler::TaskRunResult> + Send + 'static,
    {
        self.register_handler_fn_with_options(descriptor, handler, HandlerOptions::new())
    }

    /// Registers an async function handler with cancellation options.
    pub fn register_handler_fn_with_options<T, F, Fut>(
        &mut self,
        descriptor: TaskHandlerDescriptor,
        handler: F,
        options: HandlerOptions,
    ) -> Result<(), HandlerRegistrationError>
    where
        T: HasModelId + Send + Sync + 'static,
        F: Fn(T, TypedTaskContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = crate::handler::TaskRunResult> + Send + 'static,
    {
        struct FunctionHandler<F, Fut> {
            function: F,
            marker: PhantomData<fn() -> Fut>,
        }
        impl<T, F, Fut> TypedTaskHandler<T> for FunctionHandler<F, Fut>
        where
            T: Send + Sync + 'static,
            F: Fn(T, TypedTaskContext) -> Fut + Send + Sync + 'static,
            Fut: Future<Output = crate::handler::TaskRunResult> + Send + 'static,
        {
            fn run<'a>(&'a self, input: T, context: TypedTaskContext) -> TaskFuture<'a, crate::handler::TaskRunResult> {
                Box::pin((self.function)(input, context))
            }
        }
        self.register_with_options::<T, _>(
            descriptor,
            Arc::new(FunctionHandler {
                function: handler,
                marker: PhantomData,
            }),
            options,
        )
    }

    /// Registers one typed handler and retains its source for conflict errors.
    ///
    /// # Type Parameters
    ///
    /// * `T` is the decoded payload type passed to the handler.
    /// * `H` is the application handler implementation.
    ///
    /// # Parameters
    ///
    /// * `descriptor` declares the stable kind, payload schema, and
    ///   cancellation mode.
    /// * `handler` is retained and shared for dispatches of this kind.
    /// * `source` labels this registration in duplicate-kind errors.
    ///
    /// # Returns
    ///
    /// `Ok(())` when registration succeeds.
    ///
    /// Registers a typed handler with its cancellation and source options.
    ///
    /// The payload type's model ID must match the descriptor before the
    /// handler is inserted.
    #[inline]
    /// Registers a typed handler with its cancellation and diagnostic options.
    pub fn register_with_options<T, H>(
        &mut self,
        descriptor: TaskHandlerDescriptor,
        handler: Arc<H>,
        options: HandlerOptions,
    ) -> Result<(), HandlerRegistrationError>
    where
        T: HasModelId + Send + Sync + 'static,
        H: TypedTaskHandler<T>,
    {
        self.insert::<T, H>(descriptor, handler, options)
    }

    /// Validates registration options and inserts the handler if its kind is
    /// free.
    ///
    /// # Type Parameters
    ///
    /// * `T` is the decoded payload type supplied to the handler.
    /// * `H` is the concrete handler implementation retained for dispatch.
    ///
    /// # Parameters
    ///
    /// * `descriptor` declares the handler kind, payload schema, and
    ///   cancellation mode.
    /// * `handler` is retained and adapted for erased registry dispatch.
    /// * `source` identifies this registration in duplicate-kind errors.
    /// * `external_cancellation_hook` provides external cancellation behavior
    ///   when required by the descriptor's cancellation mode.
    ///
    /// # Returns
    ///
    /// `Ok(())` when the registration is validated and inserted.
    ///
    /// # Errors
    ///
    /// Returns an error when the descriptor is invalid, the cancellation hook
    /// does not match the descriptor's mode, or the kind identifier is already
    /// registered.
    fn insert<T, H>(
        &mut self,
        descriptor: TaskHandlerDescriptor,
        handler: Arc<H>,
        options: HandlerOptions,
    ) -> Result<(), HandlerRegistrationError>
    where
        T: HasModelId + Send + Sync + 'static,
        H: TypedTaskHandler<T>,
    {
        descriptor.validate()?;
        if descriptor.payload_type_id() != &ModelIdBuf::from(T::MODEL_ID) {
            return Err(HandlerRegistrationError::PayloadModelMismatch {
                expected: descriptor.payload_type_id().to_string(),
                actual: ModelIdBuf::from(T::MODEL_ID).to_string(),
            });
        }
        match (options.cancellation_mode, options.external_cancellation_hook.is_some()) {
            (CancellationMode::ExternalHook, false) => {
                return Err(HandlerRegistrationError::MissingExternalHook(
                    descriptor.kind_id().to_owned(),
                ));
            }
            (CancellationMode::ExternalHook, true) => {}
            (_, true) => {
                return Err(HandlerRegistrationError::UnexpectedExternalHook(
                    descriptor.kind_id().to_owned(),
                ));
            }
            (_, false) => {}
        }
        let source = options.source;
        if let Some(existing) = self.handlers.get(descriptor.kind_id()) {
            return Err(HandlerRegistrationError::DuplicateKindId {
                kind_id: descriptor.kind_id().to_owned(),
                first_source: existing.source.clone(),
                second_source: source,
            });
        }
        let kind_id = descriptor.kind_id().to_owned();
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
                external_cancellation_hook: options.external_cancellation_hook,
                cancellation_mode: options.cancellation_mode,
            },
        );
        Ok(())
    }

    /// Validates and decodes a stored payload before any handler code runs.
    ///
    /// # Parameters
    ///
    /// * `kind_id` selects the registered handler.
    /// * `payload` is consumed after validating and decoding its stored value.
    /// * `codecs` supplies the decoder registered for the payload codec ID.
    ///
    /// # Returns
    ///
    /// A prepared, single-use task when validation succeeds.
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

    /// Calls an external cancellation hook, if registered for this kind.
    ///
    /// # Parameters
    ///
    /// * `kind_id` selects the registered handler.
    /// * `task_id` identifies the task whose external work should stop.
    /// * `attempt` identifies the active attempt.
    ///
    /// # Returns
    ///
    /// `Some` contains the hook's future; `None` means no hook is registered
    /// for the kind.
    pub fn cancel_externally(
        &self,
        kind_id: &str,
        task_id: TaskId,
        attempt: u32,
    ) -> Option<TaskFuture<'static, Result<(), TaskRunError>>> {
        self.handlers
            .get(kind_id)
            .and_then(|registration| registration.external_cancellation_hook.as_ref())
            .map(|hook| hook(TaskAttempt::new(task_id, attempt)))
    }
}
