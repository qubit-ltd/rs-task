// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::error::Error;
use std::sync::Arc;

use qubit_codec::ValueBytesCodecDescriptor;
use qubit_codec::ValueBytesCodecRegistration;
use qubit_codec::ValueBytesCodecRegistry;
use qubit_codec::ValueCodecId;
use qubit_codec::ValueCodecRegistration;
use qubit_codec::ValueCodecRegistrationSource;
use qubit_codec::ValueDecoder;
use qubit_codec::ValueEncoder;
use qubit_model_id::HasModelId;
use qubit_model_id::ModelId;
use qubit_model_id::ModelIdBuf;
use qubit_task::CancellationMode;
use qubit_task::TaskContext;
use qubit_task::TaskHandler;
use qubit_task::TaskHandlerDescriptor;
use qubit_task::handler::TaskRunOutcome;
use qubit_task::handler::TaskRunResult;
use qubit_task::model::TaskOutput;
use qubit_task::model::TaskRequest;
use qubit_task::store::TaskFuture;
use serde_json::Error as JsonError;
use serde_json::Value;
use serde_json::from_slice;
use serde_json::to_vec;

/// JSON-backed payload used by the external provider fixture.
///
/// # Examples
///
/// ```
/// use qubit_task_fixture_provider::FixturePayload;
/// use qubit_task_fixture_provider::request;
/// use serde_json::Value;
///
/// let payload = FixturePayload(Value::Null);
/// let request = request("fixture", payload.0, "request-1".to_owned());
/// assert_eq!(request.kind_id, "fixture");
/// ```
pub struct FixturePayload(
    /// JSON value encoded and decoded by the fixture's codec.
    pub Value,
);

impl HasModelId for FixturePayload {
    const MODEL_ID: ModelId = ModelId::new("fixture.TaskPayload");
}

/// Minimal handler implemented in a crate separate from the service consumer.
///
/// # Examples
///
/// The service dispatcher decodes the request payload and supplies it with a
/// `TaskContext` when it invokes the handler.
///
/// ```
/// use qubit_task::TaskContext;
/// use qubit_task::TaskHandler;
/// use qubit_task::handler::TaskRunOutcome;
/// use qubit_task_fixture_provider::FixtureHandler;
/// use qubit_task_fixture_provider::FixturePayload;
/// use serde_json::Value;
///
/// async fn handle(payload: FixturePayload, context: TaskContext) {
///     let outcome = FixtureHandler.run(payload, context).await;
///     assert!(matches!(outcome, Ok(TaskRunOutcome::Succeeded(_))));
/// }
/// ```
pub struct FixtureHandler;

impl TaskHandler<FixturePayload> for FixtureHandler {
    /// Completes the fixture request successfully without performing work.
    fn run<'a>(
        &'a self,
        _payload: FixturePayload,
        _context: TaskContext,
    ) -> TaskFuture<'a, TaskRunResult> {
        Box::pin(async { Ok(TaskRunOutcome::Succeeded(TaskOutput::default())) })
    }
}

/// Stateless JSON codec used to register the fixture payload type.
#[derive(Default)]
pub struct JsonValueCodec;

impl ValueEncoder<FixturePayload> for JsonValueCodec {
    type Output = Vec<u8>;
    type Error = JsonError;

    /// Encodes the payload's JSON value as UTF-8 JSON bytes.
    fn encode(&mut self, value: &FixturePayload) -> Result<Vec<u8>, Self::Error> {
        to_vec(&value.0)
    }
}

impl ValueDecoder<[u8]> for JsonValueCodec {
    type Output = FixturePayload;
    type Error = JsonError;

    /// Decodes UTF-8 JSON bytes into the fixture payload type.
    fn decode(&mut self, bytes: &[u8]) -> Result<Self::Output, Self::Error> {
        from_slice(bytes).map(FixturePayload)
    }
}

/// Type-erased byte codec descriptor for [`FixturePayload`].
pub static JSON_DESCRIPTOR: ValueBytesCodecDescriptor =
    ValueBytesCodecDescriptor::of::<JsonValueCodec, FixturePayload>();

/// Static registration identifying the fixture's JSON payload codec.
pub static JSON_CODEC: ValueBytesCodecRegistration = ValueCodecRegistration::new(
    ValueCodecId::new("fixture.task.json"),
    &JSON_DESCRIPTOR,
    ValueCodecRegistrationSource::new("qubit-task", "fixture-provider", "fixture", 1),
);

/// Builds a registry containing the fixture's JSON codec.
///
/// # Returns
///
/// A shared registry that can encode and decode [`FixturePayload`].
///
/// # Errors
///
/// Returns an error if the static codec registration cannot form a valid registry.
pub fn codec_registry() -> Result<Arc<ValueBytesCodecRegistry>, Box<dyn Error>> {
    Ok(Arc::new(
        ValueBytesCodecRegistry::from_registrations([&JSON_CODEC])?,
    ))
}

/// Describes the fixture handler for the supplied routing identifier.
///
/// # Parameters
///
/// * `kind_id` - Routing identifier assigned to the handler.
///
/// # Returns
///
/// A descriptor for the fixture payload and its supported schema version.
///
/// # Errors
///
/// Returns an error if the fixture payload type identifier is invalid.
pub fn descriptor(kind_id: &str) -> Result<TaskHandlerDescriptor, Box<dyn Error>> {
    Ok(TaskHandlerDescriptor {
        kind_id: kind_id.into(),
        payload_type_id: ModelIdBuf::try_from("fixture.TaskPayload")?,
        accepted_schema_versions: vec![1],
        cancellation_mode: CancellationMode::Unsupported,
    })
}

/// Creates a version-one request with the fixture codec and idempotency key.
///
/// # Parameters
///
/// * `kind_id` - Routing identifier for the request.
/// * `payload` - JSON value to wrap as a [`FixturePayload`].
/// * `idempotency_key` - Key used to identify duplicate submissions.
///
/// # Returns
///
/// A request configured with schema version one and the fixture JSON codec.
#[must_use]
pub fn request(
    kind_id: &str,
    payload: Value,
    idempotency_key: String,
) -> TaskRequest<FixturePayload> {
    let mut request = TaskRequest::new(
        kind_id,
        1,
        ValueCodecId::new("fixture.task.json"),
        FixturePayload(payload),
    );
    request.idempotency_key = Some(idempotency_key);
    request
}
