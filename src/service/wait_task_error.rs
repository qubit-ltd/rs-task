use crate::{TaskId, store::StoreError};

#[derive(Debug, thiserror::Error)]
pub enum WaitTaskError {
    #[error("task {0} is unavailable")]
    Unavailable(TaskId),
    #[error("timed out waiting for task {0}")]
    Timeout(TaskId),
    #[error("failed to read task state: {0}")]
    Read(#[source] StoreError),
}
