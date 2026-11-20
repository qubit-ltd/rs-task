use std::sync::Arc;

use qubit_codec::ValueBytesCodecRegistry;
use qubit_codec::ValueCodecId;
use qubit_model_id::HasModelId;
use qubit_model_id::ModelId;
use qubit_task::handler::TaskHandler;
use qubit_task::handler::TaskHandlerDescriptor;
use qubit_task::handler::TaskKind;
use qubit_task::handler::TaskRunOutcome;
use qubit_task::handler::TaskRunResult;
use qubit_task::model::typed::ResourceRequest;
use qubit_task::service::TaskExecutionServiceBuilder;
use qubit_task::store::TaskFuture;

struct Echo(String);

impl HasModelId for Echo {
    const MODEL_ID: ModelId = ModelId::new("example.Echo");
}

struct Other;

impl HasModelId for Other {
    const MODEL_ID: ModelId = ModelId::new("example.Other");
}

struct Handler;

impl TaskHandler<Echo> for Handler {
    fn run<'a>(
        &'a self,
        value: Echo,
        _context: qubit_task::handler::TaskContext,
    ) -> TaskFuture<'a, TaskRunResult> {
        let _ = value.0;
        Box::pin(async move { Ok(TaskRunOutcome::success()) })
    }
}

fn descriptor(kind: &str) -> TaskHandlerDescriptor {
    TaskHandlerDescriptor::for_payload::<Echo>(kind, 1)
}

#[test]
fn kind_builds_request_and_descriptor() {
    let kind = TaskKind::<Echo>::new("echo", 2, ValueCodecId::new("example.codec"));
    let request = kind
        .request(Echo("hello".into()))
        .with_category("test")
        .with_correlation_key("corr")
        .with_idempotency_key("idem")
        .with_metadata(qubit_metadata::Metadata::new())
        .with_resources(ResourceRequest::cpu(2));
    assert_eq!(request.kind_id, "echo");
    assert_eq!(request.payload.schema_version, 2);
    assert_eq!(request.category.as_deref(), Some("test"));
    assert_eq!(request.correlation_key.as_deref(), Some("corr"));
    assert_eq!(request.idempotency_key.as_deref(), Some("idem"));
    assert_eq!(request.resource_limit.cpu_slots, 2);
}

#[test]
fn descriptor_validates_schema_and_payload_identity_at_registration() {
    let mut registry = qubit_task::handler::TaskHandlerRegistry::new();
    let empty = descriptor("empty").with_accepted_schema_versions([]);
    assert!(
        registry
            .register::<Echo, _>(empty, Arc::new(Handler))
            .is_err()
    );

    let repeated = descriptor("repeated").with_accepted_schema_versions([1, 1]);
    assert!(
        registry
            .register::<Echo, _>(repeated, Arc::new(Handler))
            .is_err()
    );
    let repeated = descriptor("zero").with_accepted_schema_versions([0]);
    assert!(
        registry
            .register::<Echo, _>(repeated, Arc::new(Handler))
            .is_err()
    );

    assert!(
        registry
            .register::<Echo, _>(
                TaskHandlerDescriptor::for_payload::<Other>("mismatch", 1),
                Arc::new(Handler)
            )
            .is_err()
    );
    assert!(TaskHandlerDescriptor::try_new("invalid-model", "not a model", 1).is_err());
}

#[tokio::test]
async fn registration_options_select_cancellation_and_hook_receives_attempt() {
    use qubit_id::Id;
    use qubit_task::handler::CancellationMode;
    use qubit_task::handler::HandlerOptions;
    use qubit_task::model::typed::TaskId;

    let mut registry = qubit_task::handler::TaskHandlerRegistry::new();
    registry
        .register_with_options::<Echo, _>(
            descriptor("cooperative"),
            Arc::new(Handler),
            HandlerOptions::new().with_cooperative_cancellation(),
        )
        .unwrap();
    assert_eq!(
        registry.cancellation_mode("cooperative"),
        Some(CancellationMode::Cooperative)
    );
    assert_eq!(registry.cancellation_mode("unregistered"), None);

    let expected_id = TaskId::from_id(Id::new(42));
    registry
        .register_with_options::<Echo, _>(
            descriptor("external"),
            Arc::new(Handler),
            HandlerOptions::new().with_external_cancellation(move |attempt| async move {
                assert_eq!(attempt.task_id(), expected_id);
                assert_eq!(attempt.attempt(), 3);
                Ok(())
            }),
        )
        .unwrap();
    registry
        .cancel_externally("external", expected_id, 3)
        .expect("external hook is registered")
        .await
        .expect("external hook succeeds");
}

#[test]
fn registration_can_be_removed_and_replaced_without_overwriting_duplicates() {
    let mut registry = qubit_task::handler::TaskHandlerRegistry::new();
    registry
        .register::<Echo, _>(descriptor("echo"), Arc::new(Handler))
        .unwrap();
    assert!(
        registry
            .register::<Echo, _>(descriptor("echo"), Arc::new(Handler))
            .is_err()
    );
    assert!(registry.unregister("echo"));
    assert!(!registry.unregister("echo"));
    registry
        .register::<Echo, _>(descriptor("echo"), Arc::new(Handler))
        .unwrap();
}

#[test]
fn builder_registers_async_functions_and_can_unregister_and_reregister() {
    let mut builder = TaskExecutionServiceBuilder::in_memory(16, ValueBytesCodecRegistry::empty());
    let handler = |_value: Echo, _context: qubit_task::handler::TaskContext| async {
        Ok(TaskRunOutcome::success())
    };
    builder
        .register_handler_fn::<Echo, _, _>(descriptor("echo"), handler)
        .unwrap();
    let duplicate = |_value: Echo, _context: qubit_task::handler::TaskContext| async {
        Ok(TaskRunOutcome::success())
    };
    assert!(
        builder
            .register_handler_fn::<Echo, _, _>(descriptor("echo"), duplicate)
            .is_err()
    );
    assert!(builder.unregister_handler("echo"));
    assert!(!builder.unregister_handler("echo"));
    let handler = |_value: Echo, _context: qubit_task::handler::TaskContext| async {
        Ok(TaskRunOutcome::success())
    };
    builder
        .register_handler_fn::<Echo, _, _>(descriptor("echo"), handler)
        .unwrap();
}

#[test]
fn run_result_convenience_constructors_preserve_classification() {
    assert!(matches!(
        TaskRunOutcome::success(),
        TaskRunOutcome::Succeeded(_)
    ));
    assert!(qubit_task::model::TaskRunError::retryable("temporary", "retry").retryable);
    assert!(!qubit_task::model::TaskRunError::permanent("invalid", "stop").retryable);
}
