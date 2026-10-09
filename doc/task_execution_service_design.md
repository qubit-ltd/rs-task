# Qubit Task：带类型任务服务设计

[English design document](task_execution_service_design.en.md)

本文面向 `qubit-task` 0.10。带类型 SQLite 存储使用 `SqliteTaskStore::open` 打开，typed schema v4/v5 会在事务中迁移到 v6；旧 UUID schema 会被拒绝且不作修改。升级步骤见[迁移指南](migration.zh_CN.md)。

本文描述当前的 typed task API。旧的字节数组 request、精确 `(task_type, handler_version)` 路由、本地闭包提交和旧服务 builder 属于历史实现细节，不再构成公开契约。

## 模型与路由

每个任务都有一个稳定的 `TaskId`，它包装 `rs-id::Id`。应用注入 `IdGenerator`；跨进程使用 Snowflake 类生成器时，各进程必须使用不同节点编号，并配置合适的时钟条件。

服务将路由、分类和 payload 身份分开处理：

- `kind_id` 选择已注册的处理器。
- `category` 是应用查询过滤条件，不影响处理器路由。
- `Payload<T>` 将值与 `type_id`、`schema_version` 和 `codec_id` 绑定。

针对一个 `kind_id` 注册的处理器只接受一种 payload `type_id`，并声明支持的 schema 版本。codec 注册表独立映射 bytes codec，一个 codec 可以服务多个 schema 版本。编码会验证 typed request，并存储包含身份信息和字节的 `EncodedPayload`。任务开始后再执行解码与处理器分发。

## 服务与存储

`TaskExecutionServiceBuilder` 接收 typed `TaskStore`、bytes codec 注册表和 ID generator。`capacity(ResourceCapacity)` 配置本地执行引擎的资源预留额度。builder 持有 `TypedTaskHandlerRegistry`；服务构建完成后注册表固定。服务通过 store 写入已受理任务，并以不含 payload 的 `TaskSummary` 返回状态和历史查询结果。

内存与 SQLite store 实现相同的 typed store 契约。SQLite 使用 typed numeric-ID schema；遇到不兼容的旧 UUID schema 时会返回诊断错误，不会静默重新解释数据。Store ownership 用于隔离并发服务实例；恢复会在取得 ownership 后继续处理保留的 queued 任务。服务在取得 owner 前启动清理 worker。若取得 owner 后取消 `build()` future，仍持有 owner 的 guard 会让该 worker 异步释放。释放是最终一致的，因此立即再次 build 可能短暂遇到 owner 冲突。

单个调度器按有界分页扫描 queued 摘要，只在有运行名额时启动 handler。`max_running_tasks` 默认使用可用并行度；`scan_page_size` 默认 128，最大 256。重试期限保存在 SQLite schema 版本 6 中，重启后仍会遵守；仅显式标记可重试的 handler 错误会按尝试上限重试。运维人员修复配置后，可携带观察到的 state version 恢复 Blocked 任务。

## 执行与资源准入

单一调度器在启动 CAS 前预留请求的 CPU 槽位、GPU 设备或标签、内存字节、磁盘字节和自定义整数单位。暂时拿不到资源的任务保留在队列中，调度器继续检查后续可运行任务。默认允许后续任务成功越过较早的资源阻塞任务 32 次；可用 `TaskExecutionServiceBuilder::max_resource_bypasses(NonZeroUsize)` 修改这个正整数上限。达到上限后，即使不相关资源空闲，后续任务也要等待较早的任务。计数仅存在于当前进程，重启后清零。该策略既不保证严格 FIFO，也不保证等待时间上界。只有成功启动 CAS 的任务才占运行名额；所有完成或启动失败路径都会释放预留。配额不会固定 CPU 核心、在操作系统层面发现或隔离 GPU，也不会强制限制进程实际的内存或磁盘用量。

任务生命周期包括 `Queued`、`Running`、`Blocked`、`Succeeded`、`Failed`、`Panicked` 和 `Cancelled`。状态迁移会比较已保存的 state version 和 attempt，以拒绝过期写入。排队或 blocked 的任务可直接取消。运行中的任务按处理器声明的模式取消：协作式 handler 检查 `TaskContext::is_cancelled()`，并在安全边界停止；外部 hook 模式由 hook 执行取消。若 handler 不支持运行中取消，服务会向调用方报告。

## 进度与历史

处理器通过 task context 中的 `rs-progress::AsyncReporter` 汇报进度。`report_async()` 等待进度快照持久化完成；后续任务查询会读到当前阶段和指标值。进度更新不会推进任务生命周期 state version。

历史页按 `(accepted_at_ms, numeric task id)` 升序排列。排他的 `after` 游标包含这个排序键，而不是偏移量。每次查询读取各自的 store 快照；并发插入不会让游标变成快照 token。过滤条件包括生命周期状态、`category` 和 correlation key。`kind_id` 与 category 过滤保持独立。

## 可靠性边界

SQLite 持久化支持进程重启恢复，执行语义为至少一次。若任务在外部副作用完成后、结果保存前崩溃，恢复后 handler 可能再次执行，因此应用必须让副作用幂等，或用自己的事务策略保护它们。调度只在进程内进行；本 crate 不提供分布式调度、强制中断任意代码或业务副作用恰好一次保证。

后台调度或结果写入遇到存储故障时会锁存故障并停止新写入；诊断读取仍会访问 store。显式调用 `shutdown().await` 会停止调度，等待活跃处理器和通知排空，尝试释放 owner，并返回共享结果（包括已锁存的故障）。并发等待者看到同一次释放尝试的结果；取消其中一个等待者不会取消 supervisor 的排空。若 owner 释放暂时失败，之后显式调用 `shutdown()` 会重试。若释放时发生 panic，supervisor 会向等待者返回 `StoreUnavailable`，并将仍持有 owner 的 guard 交给清理 worker。最后一个外部 service 句柄被丢弃时也会异步请求同样的排空，但 `Drop` 不等待、不返回结果。若处理器始终不结束，owner 可能一直被占用。

配置 `TaskExecutionServiceBuilder::event_bus` 后，服务会启用 store outbox，并在任务恢复后启动异步 publisher。每个生命周期快照与任务状态迁移写入同一个 SQLite 事务，随后发布到 `task.lifecycle`；确认接纳后才删除 outbox 行。此功能要求 store 提供持久 outbox，`MemoryTaskStore` 不具备此能力，也不适合作为 durable 配置。outbox 启用前提交的状态不会在启动时回填。

投递语义为至少一次。若发布回执不确定，或 bus 已接纳事件但进程在删除 outbox 行前停止，同一快照可能再次发布。消费者应按 `(TaskId, state_version)` 去重、忽略旧版本，并在发现版本缺口时查询服务。应监控 outbox 行数与最老行年龄；使用 Redis Streams 时，还应查看 `XLEN` 和 `XPENDING`。具体配置和运维边界见[带类型 API 指南](typed-task-api.zh_CN.md)与[用户指南](user-guide.zh_CN.md#发布任务生命周期变化)。应用仍可显式调用有界 `TaskStore::prune_terminal_before` 清理终态历史，该操作会释放幂等键供复用。

预期的提交拒绝（容量、未完成任务数上限、幂等冲突和重复任务 ID）返回结构化请求错误，不会锁存服务；其他操作性 store 故障仍按原逻辑处理。外部取消 hook 由服务托管：同一 attempt 的并发调用共享 hook，取消调用方不会中断 hook，`shutdown()` 会等待 hook 完成。hook 失败会留下可查询诊断；任务仍运行时可再次调用 `cancel()` 重试。hook 必须对 (`TaskId`, attempt) 幂等。
