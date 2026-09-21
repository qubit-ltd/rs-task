# qubit-task 设计

服务注册表唯一使用 `qubit_id::Id` 作为身份类型。提交 token 将复用 ID 后的旧回调与新任务隔离。注册表状态转换在互斥锁内完成，用户回调不持有该锁执行。终态历史受 builder 配置限制。`TaskHandle` 包装 executor 句柄，不暴露 executor 内部 ID。
