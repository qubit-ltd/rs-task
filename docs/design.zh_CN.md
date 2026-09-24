# qubit-task 设计

服务注册表唯一使用 `qubit_id::Id` 作为身份类型。提交 token 将复用 ID 后的旧回调与新任务隔离。注册表状态转换在互斥锁内完成，用户回调不持有该锁执行。终态历史受 builder 配置限制，容量按有效终态 ID 计算；复用 ID 完成新任务时会替换旧终态记录。`cancel(id)` 仅在 worker 领取排队 job 之前成功；领取后即使 callable 尚未开始也会返回 `false`。取消成功时，排队任务已从动态线程池移除，其捕获值也已在返回前释放。服务通过 `thread_pool_stats()` 快照提供线程池指标，不暴露可提交任务的线程池接口。`TaskHandle` 包装 executor 句柄，不暴露 executor 内部 ID。
