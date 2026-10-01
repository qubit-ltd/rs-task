//! Contract tests for the typed handler registry.
//!
//! The integration-test crate root will include this module when the new
//! handler API is wired into `handler::mod`.

use std::sync::Arc;

use qubit_codec::ValueBytesCodecRegistry;
use qubit_model_metadata::metadata::ModelIdBuf;
use qubit_task::CancellationMode;
use qubit_task::HandlerDispatchError;
use qubit_task::HandlerRegistrationError;
use qubit_task::TaskContext;
use qubit_task::TaskHandler;
use qubit_task::TaskHandlerDescriptor;
use qubit_task::TaskHandlerRegistry;
use qubit_task::model::StoredPayload;
use qubit_task::store::TaskFuture;

#[derive(Debug, PartialEq, Eq)]
struct Input(u32);

struct Handler;

impl TaskHandler<Input> for Handler {
    fn run<'a>(&'a self, input: Input, _context: TaskContext) -> TaskFuture<'a, qubit_task::handler::TaskRunResult> {
        Box::pin(async move {
            assert_eq!(input, Input(42));
            Ok(qubit_task::handler::TaskRunOutcome::Succeeded(
                qubit_task::model::TaskOutput::default(),
            ))
        })
    }
}

fn descriptor(versions: Vec<u32>) -> TaskHandlerDescriptor {
    TaskHandlerDescriptor {
        kind_id: "example.image.resize".into(),
        payload_type_id: ModelIdBuf::try_from("example.ResizeRequest").unwrap(),
        accepted_schema_versions: versions,
        cancellation_mode: CancellationMode::Cooperative,
    }
}

fn payload(type_id: &str, schema_version: u32, codec_id: &str) -> StoredPayload {
    StoredPayload {
        type_id: ModelIdBuf::try_from(type_id).unwrap(),
        schema_version,
        codec_id: codec_id.to_owned(),
        bytes: if codec_id == "qubit_task.tests.handler_u64" {
            42_u64.to_le_bytes().to_vec()
        } else if codec_id == "qubit_task.tests.handler_input" {
            42_u32.to_le_bytes().to_vec()
        } else {
            vec![]
        },
    }
}

#[derive(Default)]
struct U64Codec;

impl qubit_codec::ValueEncoder<u64> for U64Codec {
    type Output = Vec<u8>;
    type Error = std::convert::Infallible;

    fn encode(&mut self, input: &u64) -> Result<Self::Output, Self::Error> {
        Ok(input.to_le_bytes().to_vec())
    }
}

impl qubit_codec::ValueDecoder<[u8]> for U64Codec {
    type Output = u64;
    type Error = std::io::Error;

    fn decode(&mut self, input: &[u8]) -> Result<Self::Output, Self::Error> {
        let bytes: [u8; 8] = input
            .try_into()
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "expected 8 bytes"))?;
        Ok(u64::from_le_bytes(bytes))
    }
}

qubit_codec::register_value_bytes_codec!(id = "qubit_task.tests.handler_u64", codec = U64Codec, value = u64);

#[derive(Default)]
struct InputCodec;

impl qubit_codec::ValueEncoder<Input> for InputCodec {
    type Output = Vec<u8>;
    type Error = std::convert::Infallible;

    fn encode(&mut self, input: &Input) -> Result<Self::Output, Self::Error> {
        Ok(input.0.to_le_bytes().to_vec())
    }
}

impl qubit_codec::ValueDecoder<[u8]> for InputCodec {
    type Output = Input;
    type Error = std::io::Error;

    fn decode(&mut self, input: &[u8]) -> Result<Self::Output, Self::Error> {
        let bytes: [u8; 4] = input
            .try_into()
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "expected 4 bytes"))?;
        Ok(Input(u32::from_le_bytes(bytes)))
    }
}

qubit_codec::register_value_bytes_codec!(id = "qubit_task.tests.handler_input", codec = InputCodec, value = Input);

#[test]
fn registration_rejects_a_duplicate_kind_id_even_if_versions_differ() {
    let mut registry = TaskHandlerRegistry::new();
    registry.register(descriptor(vec![1]), Arc::new(Handler)).unwrap();

    let error = registry.register(descriptor(vec![2]), Arc::new(Handler)).unwrap_err();

    assert!(matches!(error, HandlerRegistrationError::DuplicateKindId { .. }));
}

#[test]
fn descriptor_requires_one_payload_type_and_unique_explicit_versions() {
    let mut registry = TaskHandlerRegistry::new();

    let error = registry.register(descriptor(vec![]), Arc::new(Handler)).unwrap_err();
    assert!(matches!(error, HandlerRegistrationError::NoAcceptedSchemaVersions));

    let error = registry
        .register(descriptor(vec![1, 1]), Arc::new(Handler))
        .unwrap_err();
    assert!(matches!(error, HandlerRegistrationError::DuplicateSchemaVersion(1)));
}

#[test]
fn registry_rejects_empty_kind_ids_and_unexpected_external_hooks() {
    let mut registry = TaskHandlerRegistry::new();
    let mut empty_kind = descriptor(vec![1]);
    empty_kind.kind_id = " \t".into();
    assert!(matches!(
        registry.register(empty_kind, Arc::new(Handler)),
        Err(HandlerRegistrationError::EmptyKindId)
    ));

    let error = registry
        .register_with_cancellation_hook(
            descriptor(vec![1]),
            Arc::new(Handler),
            "unexpected hook source",
            Arc::new(|_, _| Box::pin(async { Ok(()) })),
        )
        .unwrap_err();
    assert!(matches!(error, HandlerRegistrationError::UnexpectedExternalHook(_)));
}

#[test]
fn registry_reports_missing_handler_and_preserves_registration_source() {
    let registry = TaskHandlerRegistry::new();
    assert!(matches!(
        registry.prepare("example.unknown", payload("example.ResizeRequest", 1, "example.u32"), &ValueBytesCodecRegistry::empty()),
        Err(HandlerDispatchError::MissingHandler(kind_id)) if kind_id == "example.unknown"
    ));
    assert_eq!(registry.descriptor("example.unknown"), None);
    assert!(!registry.has_external_cancellation_hook("example.unknown"));
    assert!(
        registry
            .cancel_externally(
                "example.unknown",
                qubit_task::model::TaskId::from_id(qubit_id::Id::new(42)),
                1,
            )
            .is_none()
    );

    let mut registry = TaskHandlerRegistry::new();
    registry
        .register_with_source(descriptor(vec![1]), Arc::new(Handler), "first provider")
        .unwrap();
    let error = registry
        .register_with_source(descriptor(vec![2]), Arc::new(Handler), "second provider")
        .unwrap_err();
    assert!(matches!(
        error,
        HandlerRegistrationError::DuplicateKindId {
            first_source,
            second_source,
            ..
        } if first_source == "first provider" && second_source == "second provider"
    ));
}

#[test]
fn handler_accepts_only_its_declared_payload_model() {
    let mut registry = TaskHandlerRegistry::new();
    registry.register(descriptor(vec![1, 2]), Arc::new(Handler)).unwrap();
    let codecs = ValueBytesCodecRegistry::try_global().unwrap();

    let error = registry
        .prepare(
            "example.image.resize",
            payload("example.OtherRequest", 1, "example.u32"),
            codecs,
        )
        .unwrap_err();

    assert!(matches!(error, HandlerDispatchError::PayloadTypeMismatch { .. }));
}

#[test]
fn handler_accepts_each_declared_schema_version_but_rejects_others() {
    let mut registry = TaskHandlerRegistry::new();
    registry.register(descriptor(vec![1, 3]), Arc::new(Handler)).unwrap();
    let codecs = ValueBytesCodecRegistry::try_global().unwrap();

    for version in [1, 3] {
        let _prepared = registry
            .prepare(
                "example.image.resize",
                payload("example.ResizeRequest", version, "qubit_task.tests.handler_input"),
                codecs,
            )
            .unwrap();
    }

    let error = registry
        .prepare(
            "example.image.resize",
            payload("example.ResizeRequest", 2, "example.u32"),
            codecs,
        )
        .unwrap_err();
    assert!(matches!(error, HandlerDispatchError::UnsupportedSchemaVersion(2)));
}

#[test]
fn missing_codec_is_rejected_before_handler_execution() {
    let mut registry = TaskHandlerRegistry::new();
    registry.register(descriptor(vec![1]), Arc::new(Handler)).unwrap();

    let error = registry
        .prepare(
            "example.image.resize",
            payload("example.ResizeRequest", 1, "example.missing"),
            &ValueBytesCodecRegistry::empty(),
        )
        .unwrap_err();

    assert!(matches!(error, HandlerDispatchError::MissingCodec(_)));
}

#[test]
fn payload_codec_with_another_rust_value_type_is_rejected_before_run() {
    let mut registry = TaskHandlerRegistry::new();
    registry.register(descriptor(vec![1]), Arc::new(Handler)).unwrap();
    let codecs = ValueBytesCodecRegistry::try_global().unwrap();

    let error = registry
        .prepare(
            "example.image.resize",
            payload("example.ResizeRequest", 1, "qubit_task.tests.handler_u64"),
            codecs,
        )
        .unwrap_err();

    assert!(matches!(error, HandlerDispatchError::DecodedValueTypeMismatch { .. }));
}

#[test]
fn successfully_decoded_payload_is_prepared_for_execution() {
    let mut registry = TaskHandlerRegistry::new();
    registry.register(descriptor(vec![1]), Arc::new(Handler)).unwrap();
    let codecs = ValueBytesCodecRegistry::try_global().unwrap();
    let _prepared = registry
        .prepare(
            "example.image.resize",
            StoredPayload {
                type_id: ModelIdBuf::try_from("example.ResizeRequest").unwrap(),
                schema_version: 1,
                codec_id: "qubit_task.tests.handler_input".into(),
                bytes: 42_u32.to_le_bytes().to_vec(),
            },
            codecs,
        )
        .unwrap();
}

#[test]
fn malformed_bytes_are_classified_as_a_codec_decode_error() {
    let mut registry = TaskHandlerRegistry::new();
    registry.register(descriptor(vec![1]), Arc::new(Handler)).unwrap();
    let codecs = ValueBytesCodecRegistry::try_global().unwrap();

    let error = registry
        .prepare(
            "example.image.resize",
            StoredPayload {
                type_id: ModelIdBuf::try_from("example.ResizeRequest").unwrap(),
                schema_version: 1,
                codec_id: "qubit_task.tests.handler_input".into(),
                bytes: vec![1],
            },
            codecs,
        )
        .unwrap_err();

    assert!(matches!(
        error,
        HandlerDispatchError::Codec(qubit_codec::ValueCodecExecutionError::DecodeFailed { .. })
    ));
}

#[tokio::test]
async fn external_cancellation_hook_is_required_and_invoked_with_attempt_identity() {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    let mut descriptor = descriptor(vec![1]);
    descriptor.cancellation_mode = CancellationMode::ExternalHook;
    let mut registry = TaskHandlerRegistry::new();
    let error = registry.register(descriptor.clone(), Arc::new(Handler)).unwrap_err();
    assert!(matches!(error, HandlerRegistrationError::MissingExternalHook(_)));

    let observed = Arc::new(AtomicUsize::new(0));
    let hook_observed = Arc::clone(&observed);
    registry
        .register_with_cancellation_hook(
            descriptor,
            Arc::new(Handler),
            "test provider",
            Arc::new(move |task_id, attempt| {
                let hook_observed = Arc::clone(&hook_observed);
                Box::pin(async move {
                    assert_eq!(task_id, qubit_task::model::TaskId::from_id(qubit_id::Id::new(42)));
                    assert_eq!(attempt, 7);
                    hook_observed.fetch_add(1, Ordering::AcqRel);
                    Ok(())
                })
            }),
        )
        .unwrap();

    assert!(registry.has_external_cancellation_hook("example.image.resize"));
    registry
        .cancel_externally(
            "example.image.resize",
            qubit_task::model::TaskId::from_id(qubit_id::Id::new(42)),
            7,
        )
        .unwrap()
        .await
        .unwrap();
    assert_eq!(observed.load(Ordering::Acquire), 1);
}
