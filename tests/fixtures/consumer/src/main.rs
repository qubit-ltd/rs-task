use std::sync::Arc;

use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::model::TaskRequest;
use qubit_task::model::TaskState;
use qubit_task_fixture_provider::FixtureHandler;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let service = TaskExecutionServiceBuilder::in_memory()
        .register_handler(Arc::new(FixtureHandler))?
        .build()
        .await?;
    let request = TaskRequest::new("fixture", "1", b"input".to_vec()).with_idempotency_key("fixture-run-1");
    let accepted = service.submit(request).await?;
    let completed = service.wait(accepted.id).await?;
    assert!(matches!(completed.state, TaskState::Succeeded));
    service.shutdown().await?;
    Ok(())
}
