# Qubit Task

[![Rust CI](https://github.com/qubit-ltd/rs-task/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-task/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-task/coverage-badge.json)](https://qubit-ltd.github.io/rs-task/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-task.svg?color=blue)](https://crates.io/crates/qubit-task)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![中文文档](https://img.shields.io/badge/文档-中文版-blue.svg)](README.zh_CN.md)

Task-oriented execution services built on `qubit-executor` and
`qubit-thread-pool`.

## Installation

Add `qubit-task` and the task ID crate used by the examples to `Cargo.toml`:

```toml
[dependencies]
qubit-task = "0.6"
qubit-id = "0.6"
```

`TaskExecutionService` accepts a caller-provided task ID, runs a synchronous
callable on a thread pool, and keeps an in-memory status for lookup and
pre-start cancellation. A successful cancellation removes queued work and
releases its captured values before returning; cancellation after a worker has
claimed the job returns `false`, even if the callable has not started yet. The
returned `TaskHandle` owns the typed
result.

```rust
use qubit_id::Id;
use qubit_task::service::{TaskExecutionService, TaskStatus};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let service = TaskExecutionService::builder()
        .completed_history_capacity(128)
        .build()?;
    let id = Id::new(42);
    let handle = service.submit_callable(id, || Ok::<u32, std::io::Error>(7))?;
    assert_eq!(handle.get()?, 7);
    assert_eq!(service.status(id), Some(TaskStatus::Succeeded));
    service.shutdown();
    service.wait_termination();
    Ok(())
}
```

The service retains up to 1024 distinct terminal task IDs by default. Set
`completed_history_capacity(0)` to retain none. History is bounded and is not
persistent storage: `status(id)` returns `None` after eviction. A task ID can
be reused as soon as its previous submission has finished; the new terminal
record replaces the old one without consuming an additional history slot.
`stats().total` counts currently visible accepted tasks and retained terminal
IDs, not all tasks ever submitted. `thread_pool_stats()` provides a read-only
pool metrics snapshot without exposing the pool's submission API.

`wait_for_idle()` and `wait_for_current_tasks()` wait for registry
transitions. A result may still be publishing to its handle when they return;
use `TaskHandle::get()` or await the handle when the result is required.

## Learn More

- [User guide](docs/user-guide.md)
- [Design notes](docs/design.md)
- [API documentation](https://docs.rs/qubit-task)
- [中文用户指南](docs/user-guide.zh_CN.md)

## Testing

```bash
# Run tests with the default feature set
cargo test

# Run tests with all declared features
cargo test --all-features

# Project CI checks
./ci-check.sh

# Check code coverage
./coverage.sh
```

## License

Copyright (c) 2025 - 2026. Haixing Hu. All rights reserved.

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for the
full license text.

## Contributing

Contributions are welcome. Please follow the Rust API guidelines, keep public
API documentation and tests current, and run `./align-ci.sh` to format code and
`./ci-check.sh` to satisfy CI requirements before submitting a pull request.

## Author

**Haixing Hu** - *Qubit Co. Ltd.*

Repository: [https://github.com/qubit-ltd/rs-task](https://github.com/qubit-ltd/rs-task)
