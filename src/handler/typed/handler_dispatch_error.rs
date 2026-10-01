use std::any::TypeId;

/// Error raised before business handler code runs.
#[derive(Debug, thiserror::Error)]
pub enum HandlerDispatchError {
    /// No typed handler is registered under the requested kind.
    #[error("no handler is registered for kind_id `{0}`")]
    MissingHandler(String),
    /// The stored payload model does not match the handler's declared model.
    #[error("payload type_id `{actual}` does not match handler type_id `{expected}`")]
    PayloadTypeMismatch {
        /// Declared handler payload model.
        expected: String,
        /// Stored payload model.
        actual: String,
    },
    /// The stored schema version is not accepted by the handler.
    #[error("payload schema version {0} is not accepted by this handler")]
    UnsupportedSchemaVersion(u32),
    /// No bytes codec with this stable ID is available.
    #[error("no bytes codec is registered for `{0}`")]
    MissingCodec(String),
    /// The codec decoded a value whose Rust type differs from the handler's T.
    #[error("codec decoded Rust type {actual:?}; handler expects {expected}")]
    DecodedValueTypeMismatch {
        /// Expected Rust value type name.
        expected: &'static str,
        /// Actual process-local Rust type identity.
        actual: TypeId,
    },
    /// The selected codec failed while decoding the payload bytes.
    #[error("payload bytes codec failed: {0}")]
    Codec(#[from] qubit_codec::ValueCodecExecutionError),
}
