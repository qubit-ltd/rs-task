// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use qubit_codec::ValueBytesCodecRegistry;

use crate::handler::typed::HandlerDispatchError;
use crate::handler::typed::PreparedTask;
use crate::model::typed::StoredPayload;

/// Type-erased boundary used to prepare a task after registry lookup.
pub(in crate::handler::typed::typed_task_handler_registry) trait ErasedHandler:
    Send + Sync
{
    /// Validates and decodes the stored payload, then creates a runnable task.
    ///
    /// The payload is consumed during preparation. The codec registry is
    /// borrowed only for decoder lookup and decoding; the prepared task owns
    /// the decoded value needed for later execution.
    ///
    /// # Parameters
    ///
    /// * `payload` is the stored payload to validate and decode.
    /// * `codecs` provides the decoder registered for the payload's codec ID.
    ///
    /// # Returns
    ///
    /// Returns a task that can execute the handler with the decoded payload.
    ///
    /// # Errors
    ///
    /// Returns `HandlerDispatchError` if the payload model or schema is not
    /// accepted, its codec is unavailable or fails to decode, or the decoded
    /// value has a different Rust type than the handler expects.
    fn prepare(
        &self,
        payload: StoredPayload,
        codecs: &ValueBytesCodecRegistry,
    ) -> Result<PreparedTask, HandlerDispatchError>;
}
