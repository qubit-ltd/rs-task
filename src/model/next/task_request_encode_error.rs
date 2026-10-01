use super::MAX_TASK_METADATA_BYTES;
use super::MAX_TASK_METADATA_ENTRIES;

/// Errors raised while converting a request into stored form.
#[derive(Debug, thiserror::Error)]
pub enum TaskRequestEncodeError {
    /// The handler routing identity is empty or contains only whitespace.
    #[error("task kind_id must not be empty")]
    EmptyKindId,
    /// Metadata contains more entries than the request-level limit.
    #[error("task metadata has {0} entries; the maximum is {MAX_TASK_METADATA_ENTRIES}")]
    TooManyMetadataEntries(usize),
    /// Metadata's bounded JSON representation exceeds the request-level limit.
    #[error("task metadata has {0} encoded bytes; the maximum is {MAX_TASK_METADATA_BYTES}")]
    MetadataTooLarge(usize),
    /// Encoded payload exceeds the service's reconstructable payload limit.
    #[error("encoded task payload has {0} bytes; the maximum is {max}", max = crate::model::MAX_TASK_PAYLOAD_BYTES)]
    PayloadTooLarge(usize),
    /// `rs-metadata` rejected serialization under its own JSON budget.
    #[error("task metadata serialization failed: {0}")]
    Metadata(#[source] qubit_metadata::MetadataWireEncodeError),
    /// The payload codec could not be resolved or failed to encode the value.
    #[error(transparent)]
    Payload(#[from] super::PayloadEncodeError),
    /// Scheduling resource names or labels exceed their wire limits.
    #[error("task resource request is invalid: {0}")]
    Resource(String),
}
