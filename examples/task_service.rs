use qubit_id::Id;
use qubit_task::service::TaskExecutionService;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let service = TaskExecutionService::new()?;
    let handle = service.submit_callable(Id::new(42), || Ok::<_, io::Error>(21))?;
    assert_eq!(handle.get()?, 21);
    service.wait_for_idle();
    service.shutdown();
    service.wait_termination();
    Ok(())
}
use std::io;
