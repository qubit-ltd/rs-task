use qubit_codec::ValueBytesCodecRegistry;
use thiserror::Error;

use crate::handler::typed::TypedTaskHandlerRegistry;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum TaskRouteError {
    #[error("no handler is registered for task kind '{kind_id}'")]
    MissingHandler { kind_id: String },
    #[error("handler expects payload model '{expected}', got '{actual}'")]
    PayloadTypeMismatch { expected: String, actual: String },
    #[error("handler does not accept payload schema version {version}")]
    UnsupportedSchemaVersion { version: u32 },
    #[error("payload codec '{codec_id}' is not registered")]
    MissingCodec { codec_id: String },
}

pub(crate) fn validate_task_route(
    handlers: &TypedTaskHandlerRegistry,
    codecs: &ValueBytesCodecRegistry,
    kind_id: &str,
    type_id: &str,
    schema_version: u32,
    codec_id: &str,
) -> Result<(), TaskRouteError> {
    let descriptor = handlers
        .descriptor(kind_id)
        .ok_or_else(|| TaskRouteError::MissingHandler {
            kind_id: kind_id.to_owned(),
        })?;
    if type_id != descriptor.payload_type_id().as_str() {
        return Err(TaskRouteError::PayloadTypeMismatch {
            expected: descriptor.payload_type_id().to_string(),
            actual: type_id.to_owned(),
        });
    }
    if schema_version == 0 || !descriptor.accepted_schema_versions().contains(&schema_version) {
        return Err(TaskRouteError::UnsupportedSchemaVersion {
            version: schema_version,
        });
    }
    if codecs.get(codec_id).is_none() {
        return Err(TaskRouteError::MissingCodec {
            codec_id: codec_id.to_owned(),
        });
    }
    Ok(())
}
