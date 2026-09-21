# qubit-task 用户指南

所有任务标识使用 `qubit_id::Id`。`TaskExecutionService::submit_callable` 返回 `TaskHandle`，句柄负责保存类型化结果，服务负责记录生命周期状态。

```rust
use qubit_id::Id;
use qubit_task::service::TaskExecutionService;

let service = TaskExecutionService::new()?;
let handle = service.submit_callable(Id::new(42), || Ok::<_, ()>(21))?;
assert_eq!(handle.get()?, 21);
service.wait_for_idle();
```

`wait_for_current_tasks` 等待调用时快照中的任务，`wait_for_idle` 等待注册表没有预留或已接受任务。任务进入终态后可以复用 ID；取消只在任务开始执行前可能成功。
