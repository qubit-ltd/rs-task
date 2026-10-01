# Qubit Task：带类型任务服务设计

[English design document](task_execution_service_design.en.md)

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

内存与 SQLite store 实现相同的 typed store 契约。SQLite 使用 typed numeric-ID schema；遇到不兼容的旧 UUID schema 时会返回诊断错误，不会静默重新解释数据。Store ownership 用于隔离并发服务实例；恢复会在取得 ownership 后继续处理保留的 queued 任务。

## 执行与资源准入

本地执行引擎在启动处理器前预留请求的 CPU 槽位、GPU 设备或标签、内存字节、磁盘字节和自定义整数单位。预留用于核算并发任务，在执行结束后释放。它们不会固定 CPU 核心、在操作系统层面发现或隔离 GPU，也不会强制限制进程实际的内存或磁盘用量。超过配置容量的请求无法运行；容量足够的请求会等待资源空闲。

任务生命周期包括 `Queued`、`Running`、`Blocked`、`Succeeded`、`Failed`、`Panicked` 和 `Cancelled`。状态迁移会比较已保存的 state version 和 attempt，以拒绝过期写入。排队或 blocked 的任务可直接取消。运行中的任务按处理器声明的模式取消：协作式 handler 检查 `TaskContext::is_cancelled()`，并在安全边界停止；外部 hook 模式由 hook 执行取消。若 handler 不支持运行中取消，服务会向调用方报告。

## 进度与历史

处理器通过 task context 中的 `rs-progress::AsyncReporter` 汇报进度。`report_async()` 等待进度快照持久化完成；后续任务查询会读到当前阶段和指标值。进度更新不会推进任务生命周期 state version。

历史页按 `(accepted_at_ms, numeric task id)` 升序排列。排他的 `after` 游标包含这个排序键，而不是偏移量。每次查询读取各自的 store 快照；并发插入不会让游标变成快照 token。过滤条件包括生命周期状态、`category` 和 correlation key。`kind_id` 与 category 过滤保持独立。

## 可靠性边界

SQLite 持久化支持进程重启恢复，执行语义为至少一次。若任务在外部副作用完成后、结果保存前崩溃，恢复后 handler 可能再次执行，因此应用必须让副作用幂等，或用自己的事务策略保护它们。调度只在进程内进行；本 crate 不提供分布式调度、强制中断任意代码或业务副作用恰好一次保证。

可选 Event Bus 集成提供 `TaskEvent` 传输类型和 codec，但生命周期发布目前尚未接入 typed execution service。任务查询仍是权威状态来源。完整示例和具体 API 契约见[typed API 指南](typed-task-api.zh_CN.md)。
