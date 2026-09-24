#[cfg(feature = "inventory")]
use qubit_spi::ProviderSelection;
#[cfg(feature = "inventory")]
use qubit_task::spi;
use qubit_task::spi::TaskStoreConfig;

#[test]
fn task_store_config_defaults_to_bounded_memory_history() {
    assert!(matches!(
        TaskStoreConfig::default(),
        TaskStoreConfig::Memory { history_capacity: 1024 }
    ));
}

#[cfg(feature = "inventory")]
#[test]
fn discovered_scheduling_registry_contains_builtin_fair_fifo_provider() {
    let registry = spi::discovered_scheduling_policy_registry().expect("inventory builds");
    assert!(
        registry
            .provider_ids()
            .iter()
            .any(|id| id.as_str() == spi::FAIR_FIFO_PROVIDER_ID)
    );

    let resolver = registry
        .resolve_selected(&ProviderSelection::named(spi::FAIR_FIFO_PROVIDER_ID).expect("valid provider ID"))
        .expect("built-in policy resolves");
    let policy = resolver.create_configured(&()).expect("policy is created");
    assert!(policy.order(&Default::default(), &Default::default()).is_empty());
}

#[cfg(feature = "inventory")]
#[test]
fn discovered_engine_registry_contains_builtin_local_engine() {
    let registry = spi::discovered_task_execution_engine_registry().expect("inventory builds");
    assert!(
        registry
            .provider_ids()
            .iter()
            .any(|id| id.as_str() == spi::LOCAL_ENGINE_PROVIDER_ID)
    );

    let capacity = qubit_task::model::ResourceCapacity {
        cpu_slots: 3,
        ..Default::default()
    };
    let resolver = registry
        .resolve_selected(&ProviderSelection::named(spi::LOCAL_ENGINE_PROVIDER_ID).expect("valid provider ID"))
        .expect("built-in engine resolves");
    let engine = resolver.create_configured(&capacity).expect("engine is created");
    assert_eq!(engine.capacity().capacity.cpu_slots, 3);
}

#[cfg(feature = "inventory")]
#[test]
fn discovered_handler_registry_is_empty_without_linked_handlers() {
    let registry = spi::discovered_task_handler_registry().expect("empty inventory builds");
    assert!(registry.is_empty());
    assert!(
        registry
            .resolve_selected(&ProviderSelection::named("qubit.task.handler.missing").expect("valid provider ID"))
            .is_err()
    );
}
