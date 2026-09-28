mod support;

use qubit_task::store::MemoryTaskStore;
use tokio::test as tokio_test;

#[tokio_test]
async fn memory_store_obeys_core_contract() {
    let store = MemoryTaskStore::new(8);
    support::store_contract::check_core_contract(&store).await;
}

#[cfg(feature = "sqlite")]
#[tokio_test]
async fn sqlite_store_obeys_core_contract() {
    use qubit_task::store::SqliteTaskStore;

    let path = std::env::temp_dir().join(format!("rs-task-contract-{}.sqlite", uuid::Uuid::new_v4()));
    let store = SqliteTaskStore::open(&path).expect("sqlite store opens");
    support::store_contract::check_core_contract(&store).await;
}
