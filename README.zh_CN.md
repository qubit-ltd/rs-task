# qubit-task

`qubit-task` 在 `qubit-executor` 和 `qubit-thread-pool` 之上提供带状态跟踪的任务服务。

所有任务标识统一使用 `qubit_id::Id`。提交任务会返回 `TaskHandle`，可同步获取、非阻塞轮询或作为 future 等待。完整说明见 [中文用户指南](docs/user-guide.zh_CN.md) 和 [中文设计文档](docs/design.zh_CN.md)。
