use qubit_metadata::Metadata;

use super::ResourceRequest;
use super::StoredPayload;

/// Type-erased request data persisted by task stores.
#[derive(Debug, Clone, PartialEq)]
pub struct StoredTaskRequest {
    /// Handler routing identity.
    pub kind_id: String,
    /// User-visible query category.
    pub category: Option<String>,
    /// Encoded payload and stable schema/codec identities.
    pub payload: StoredPayload,
    /// Structured request metadata.
    pub metadata: Metadata,
    /// Scheduling quota requested by this task.
    pub resource_limit: ResourceRequest,
    /// Optional caller-defined correlation key.
    pub correlation_key: Option<String>,
    /// Optional idempotency key.
    pub idempotency_key: Option<String>,
}
