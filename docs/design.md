# qubit-task Design

The service registry uses `qubit_id::Id` as its only identity type. A submission token separates callbacks from reused IDs. Registry transitions happen under a mutex, while user callbacks run without that mutex. Terminal history is bounded by the builder setting. `TaskHandle` wraps the executor handle and never exposes executor-internal IDs.
