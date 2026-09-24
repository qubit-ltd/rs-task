// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use qubit_task::TaskExecutionService;
use qubit_task::model::TaskOutput;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    futures::executor::block_on(async {
        let service = TaskExecutionService::in_memory().await?;
        let task_id = service
            .submit_local(|_| {
                Ok(TaskOutput {
                    summary: b"completed".to_vec(),
                })
            })
            .await?;
        let record = service.wait(task_id).await?;
        assert!(record.state.is_terminal());
        service.shutdown().await?;
        Ok::<(), Box<dyn std::error::Error>>(())
    })
}
