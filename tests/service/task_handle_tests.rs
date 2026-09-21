use qubit_executor::TryGet;
use qubit_task::service::Id;
use qubit_task::service::TaskExecutionService;

#[test]
fn task_handle_preserves_qubit_id_and_exposes_result_operations() {
    let service = TaskExecutionService::new().expect("service should build");
    let id = Id::new(42);
    let handle = service
        .submit_callable(id, || Ok::<_, ()>(7))
        .expect("task should be accepted");

    assert_eq!(handle.task_id(), id);
    let _done_before_result = handle.is_done();
    match handle.try_get() {
        TryGet::Ready(result) => assert_eq!(result.expect("task should succeed"), 7),
        TryGet::Pending(handle) => assert_eq!(handle.get().expect("task should succeed"), 7),
    }
    service.shutdown();
    service.wait_termination();
}
