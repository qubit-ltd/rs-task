# TaskExecutionService 后续设计备忘

## 背景

`TaskExecutionService` 最初位于并发基础设施中，用于在 `ThreadPool`
之上提供带 `TaskId` 的任务提交、取消、状态跟踪和 idle 等待能力。随着后续计划
对齐 Java 版任务服务语义，它会逐渐承担业务任务状态流转、外部可见状态更新、
任务信息持久化、事件通知等职责。

这些职责已经超出基础执行设施 crate 的边界，因此当前将
`TaskExecutionService` 迁移到 `rs-task`。`rs-task` 依赖 `qubit-executor`
和 `qubit-thread-pool`，并复用其中的线程池、任务句柄和执行错误类型。

## 当前边界

当前版本的 `rs-task::service::TaskExecutionService` 仍然是执行层服务：

- 为每个任务分配或接收稳定的 `TaskId`。
- 使用 `qubit-thread-pool` 的 `ThreadPool` 执行任务。
- 维护进程内任务状态：
  - `Submitted`
  - `Running`
  - `Succeeded`
  - `Failed`
  - `Panicked`
  - `Cancelled`
- 支持通过 `TaskHandle` 获取任务执行结果。
- 支持提交前取消、立即关闭时取消队列任务、等待 idle。

当前版本不负责：

- 业务 `TaskInfo` 模型。
- 业务状态机，例如 `Created -> Submitted -> Initializing -> Running -> Completed`。
- 数据库持久化。
- 任务分类、目标对象、结果对象。
- 任务流水线。
- 事件总线发布。

## 目标分层

长期目标是形成以下依赖方向：

```text
qubit-executor + qubit-thread-pool
  ↑
rs-eventbus
  ↑
rs-task
```

其中：

- `qubit-executor` 和 `qubit-thread-pool` 提供执行基础设施，例如线程池、任务句柄、取消、shutdown。
- `rs-eventbus` 提供通用本地事件发布/订阅基础设施。
- `rs-task` 提供任务服务、任务状态流转和任务事件。

执行基础设施不应依赖 `rs-eventbus` 或 `rs-task`。

## 未来事件设计

后续引入 `rs-eventbus` 后，`rs-task` 可以发布任务相关事件：

- `TaskSubmitted`
- `TaskStarted`
- `TaskSucceeded`
- `TaskFailed`
- `TaskPanicked`
- `TaskCancelled`
- `TaskStatusChanged`

执行层事件可以从当前 `TaskExecutionService` 的状态变化产生：

```text
Submitted -> Running -> Succeeded
Submitted -> Running -> Failed
Submitted -> Running -> Panicked
Submitted -> Cancelled
```

这些事件只描述执行层状态，不直接等价于业务任务状态。

## 未来业务任务服务

如果要对齐 Java 版 `TaskExecutionService` 和 `AbstractTask` 的语义，应在
`rs-task` 中新增更高层的托管任务服务，例如：

```text
ManagedTaskService
ManagedTask
TaskInfoService
TaskAction
TaskStatusTransitionRule
```

该层负责业务生命周期：

```text
Created
  -> Submitted
  -> Initializing
  -> Running
  -> Completed
```

失败和取消路径：

```text
Submitted -> Failed
Initializing -> Failed
Running -> Failed
Initializing -> Cancelled
Running -> Cancelled
```

建议由业务任务包装层控制状态更新：

```text
submit(task)
  -> update TaskInfo: Submitted
  -> publish TaskSubmitted
  -> concurrent service submit

run_lifecycle(task)
  -> update TaskInfo: Initializing
  -> task.init()
  -> update TaskInfo: Running
  -> task.perform()
  -> update TaskInfo: Completed
```

异常时：

```text
run_lifecycle(task)
  -> update TaskInfo: Failed
  -> task.on_failure()
  -> publish TaskFailed
```

这样 `TaskExecutionService` 仍然负责执行层调度，业务任务服务负责业务状态。

## 与 rs-eventbus 的关系

`TaskExecutionService` 不应为了发布事件而直接耦合复杂事件总线实现。推荐做法是：

1. `rs-eventbus` 提供通用 `LocalEventBus`。
2. `rs-task` 在业务服务层持有 `LocalEventBus` 或事件发布接口。
3. 执行层状态变化被转换为任务事件。
4. 持久化、缓存刷新、通知等逻辑通过订阅事件完成。

这样可以避免：

- 执行基础设施反向依赖事件总线。
- 执行器线程池被数据库或远程事件发布阻塞。
- 任务状态机和并发调度逻辑互相污染。

## 实现注意事项

- 不要在内部状态锁持有期间执行数据库更新、事件发布或用户回调。
- 如果事件发布可能失败，应由业务服务层决定重试、记录死信或报警。
- 执行层 `TaskStatus::Running` 只表示 worker 已开始执行闭包，不表示业务任务已经完成初始化。
- 业务 `Running` 状态应由业务任务生命周期中的 `START` 动作触发。
- `Panicked` 可以映射为业务 `Failed`，但执行层仍应保留 `Panicked` 以便诊断。
