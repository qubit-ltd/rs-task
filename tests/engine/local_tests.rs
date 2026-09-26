use std::sync::Arc;

use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::handler::TaskContext;
use qubit_task::handler::TaskHandler;
use qubit_task::handler::TaskHandlerDescriptor;
use qubit_task::handler::TaskRunOutcome;
use qubit_task::handler::TaskRunResult;
use qubit_task::model::TaskOutput;
use qubit_task::model::TaskRequest;
use qubit_task::model::TaskRunError;
use qubit_task::model::TaskState;
use qubit_task::store::TaskFuture;

#[derive(Clone, Copy)]
enum HandlerBehavior {
    ConstructPanic,
    PollPanic,
    PanicCategory,
}

struct ProvenanceHandler(HandlerBehavior);

impl TaskHandler for ProvenanceHandler {
    fn descriptor(&self) -> TaskHandlerDescriptor {
        TaskHandlerDescriptor {
            task_type: "panic-provenance".into(),
            version: "1".into(),
        }
    }

    fn run<'a>(&'a self, _payload: &'a [u8], _context: TaskContext) -> TaskFuture<'a, TaskRunResult> {
        match self.0 {
            HandlerBehavior::ConstructPanic => panic!("panic while creating handler future"),
            HandlerBehavior::PollPanic => Box::pin(async { panic!("panic while polling handler future") }),
            HandlerBehavior::PanicCategory => Box::pin(async {
                Err(TaskRunError {
                    category: "panic".into(),
                    message: "application-selected category".into(),
                    retryable: false,
                })
            }),
        }
    }
}

async fn run_handler(behavior: HandlerBehavior) -> (TaskState, u32) {
    let service = TaskExecutionServiceBuilder::in_memory()
        .register_handler(Arc::new(ProvenanceHandler(behavior)))
        .expect("handler registration succeeds")
        .build()
        .await
        .expect("service builds");
    let accepted = service
        .submit(test_keyed(TaskRequest::new("panic-provenance", "1", Vec::new())))
        .await
        .expect("task is accepted");
    let result = tokio::time::timeout(std::time::Duration::from_secs(3), service.wait(accepted.id))
        .await
        .expect("task reaches a final state");
    let record = match result {
        Ok(record) => record,
        Err(qubit_task::service::TaskServiceError::Blocked) => service
            .get(accepted.id)
            .await
            .expect("task query succeeds")
            .expect("task record remains retained"),
        Err(error) => panic!("task should reach a stored lifecycle state: {error}"),
    };
    service.shutdown().await.expect("service shuts down");
    (record.state, record.attempt)
}

#[tokio::test]
async fn test_handler_future_construction_panic_is_not_retried() {
    let (state, attempt) = run_handler(HandlerBehavior::ConstructPanic).await;
    assert!(matches!(state, TaskState::Panicked { .. }));
    assert_eq!(attempt, 1);
}

#[tokio::test]
async fn test_handler_future_poll_panic_is_not_retried() {
    let (state, attempt) = run_handler(HandlerBehavior::PollPanic).await;
    assert!(matches!(state, TaskState::Panicked { .. }));
    assert_eq!(attempt, 1);
}

#[tokio::test]
async fn test_application_error_category_panic_remains_failed() {
    let (state, attempt) = run_handler(HandlerBehavior::PanicCategory).await;
    assert!(matches!(state, TaskState::Failed { .. }));
    assert_eq!(attempt, 1);
}

#[allow(dead_code)]
fn successful_result() -> TaskRunResult {
    Ok(TaskRunOutcome::Succeeded(TaskOutput::default()))
}

#[allow(dead_code)]
fn test_keyed(mut request: qubit_task::model::TaskRequest) -> qubit_task::model::TaskRequest {
    static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(1);
    if request.idempotency_key.is_none() {
        request.idempotency_key = Some(format!(
            "test-request-{}",
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
    }
    request
}
