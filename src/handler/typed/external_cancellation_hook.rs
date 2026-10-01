use crate::model::TaskRunError;
use crate::model::next::TaskId;
use crate::store::TaskFuture;

/// Optional external cancellation action registered for a handler kind.
pub type ExternalCancellationHook =
    std::sync::Arc<dyn Fn(TaskId, u32) -> TaskFuture<'static, Result<(), TaskRunError>> + Send + Sync>;
