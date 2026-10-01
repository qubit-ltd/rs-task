use crate::handler::TaskRunResult;
use crate::handler::typed::TypedTaskContext;
use crate::store::TaskFuture;

/// Async business implementation for one concrete payload type.
pub trait TypedTaskHandler<T>: Send + Sync + 'static {
    /// Executes one attempt with an already decoded input value.
    fn run<'a>(&'a self, input: T, context: TypedTaskContext) -> TaskFuture<'a, TaskRunResult>;
}
