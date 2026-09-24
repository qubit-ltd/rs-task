# qubit-task 用户指南

[English version](user-guide.md)

本指南面向使用线程池执行任务，并且需要业务侧任务 ID、类型化结果和可查询生命周期状态的 Rust 应用。内容适用于 qubit-task 0.5.x，要求 Rust 1.94 或更高版本。

## 手册目标与读者

如果应用既要知道“任务返回了什么”，又要知道“任务当前处于哪个阶段”，可以使用本 crate。服务注册表位于内存中且有容量上限，不是持久化的作业数据库。

## 概念模型

一次已接受的提交有两种相互关联的观察方式：

| 观察对象 | API | 含义 |
| --- | --- | --- |
| 类型化结果 | TaskHandle<R, E> | 任务返回的 R，或 E/executor 错误。 |
| 服务状态 | TaskStatus | Submitted、Running 或某个终态。 |

Id 由调用方提供。任务处于活动状态或正在被接受时，不能再次提交相同 ID；进入终态后可以复用。status 与 stats 会报告活动记录，以及在配置的历史容量内保留的终态记录。

## 贯穿场景

假设应用收到编号为 42 的数据导入请求。它需要提交导入任务，让调用方取得类型化的导入数量，并在运维界面显示 Succeeded 或失败状态。最小流程如下：

1. 创建服务，并设置需要保留的终态数量。
2. 使用 Id::new(42) 提交 callable。
3. 从 TaskHandle::get 取得结果，再查询 status。
4. 所有已接受任务完成后关闭服务。

## 安装与最小配置

在 Cargo.toml 中加入：

~~~toml
[dependencies]
qubit-task = "0.5"
qubit-id = "0.6"
~~~

默认构造函数使用 qubit-thread-pool 的默认配置。如果需要自定义线程池或限制终态历史容量，可以使用 builder：

~~~rust
use qubit_task::service::TaskExecutionService;

let service = TaskExecutionService::builder()
    .completed_history_capacity(128)
    .build()?;
# Ok::<(), Box<dyn std::error::Error>>(())
~~~

## 核心工作流

提交 callable；需要类型化结果时消费对应的句柄：

~~~rust
use qubit_id::Id;
use qubit_task::service::{TaskExecutionService, TaskStatus};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let service = TaskExecutionService::new()?;
    let id = Id::new(42);
    let handle = service.submit_callable(id, || Ok::<u32, std::io::Error>(21))?;

    assert_eq!(handle.get()?, 21);
    assert_eq!(service.status(id), Some(TaskStatus::Succeeded));

    service.shutdown();
    service.wait_termination();
    Ok(())
}
~~~

如果任务返回 Result<(), E>，可以使用更简洁的 submit。TaskHandle::try_get 用于非阻塞检查，is_done 用于判断句柄是否进入终态；TaskHandle 还实现了 IntoFuture，可以作为 future 等待。

## 进阶用法

### 配置底层线程池

可以传入 qubit-thread-pool 提供的 ThreadPoolBuilder，调整线程池支持的属性：

使用此选项时，也要将线程池 crate 声明为直接依赖：

~~~toml
[dependencies]
qubit-thread-pool = "0.10"
~~~

~~~rust
use qubit_task::service::TaskExecutionService;
use qubit_thread_pool::ThreadPoolBuilder;

let service = TaskExecutionService::builder()
    .thread_pool(ThreadPoolBuilder::default().pool_size(4).queue_capacity(256))
    .build()?;
# Ok::<(), Box<dyn std::error::Error>>(())
~~~

### 暂停接收与取消排队任务

调用 suspend 后，新提交会返回 TaskExecutionServiceError::Suspended，已经接受的任务不受影响；恢复接收时调用 resume。cancel(id) 只有在 worker 尚未开始执行任务时才返回 true。它与线程池启动任务之间存在竞态，因此运行中的任务会返回 false。取消成功返回前，动态线程池会移除排队 job 并释放它捕获的值，因此该队列位置可立即用于后续提交。

服务不会暴露可直接向底层线程池提交任务的引用。需要查看池指标时，读取一次快照即可：

~~~rust
let pool_snapshot = service.thread_pool_stats();
println!("当前排队任务数：{}", pool_snapshot.queued_tasks);
~~~

`ThreadPoolStats` 仅用于观测，返回时可能已经过时，也不是同步原语。不同计数可能来自相邻时刻；需要协调任务时，应使用任务句柄或服务的等待方法。

### 等待任务

wait_for_current_tasks 等待调用时观察到的活动 ID 快照。wait_for_idle 等待注册表中不再有预留或已接受的活动任务。如果还必须确认结果已经发布到句柄，仍应调用 TaskHandle::get。

## 错误与诊断

错误分为两层：

- submit 和 submit_callable 返回提交错误，可能是 DuplicateTask、Suspended、Rejected 或 AcceptancePanicked。
- 已接受任务的结果由句柄返回。callable 自己返回的 Err(E) 表示任务失败；任务 panic 时，executor 的结果类型会报告 panic。

单个任务使用 status(id)，整体快照使用 stats()。TaskExecutionStats 统计当前可见的活动任务和保留的终态 ID，不是累计提交次数。复用 ID 会替换原终态，不会多占一个历史名额；终态记录被淘汰后，status(id) 会返回 None。

## 排障

| 现象 | 检查方式 |
| --- | --- |
| 提交返回 DuplicateTask | 等待原任务进入终态，或换用其他 Id。 |
| 提交返回 Suspended | 检查 is_suspended()，允许接收时调用 resume()。 |
| cancel 返回 false | ID 可能不存在、已进入终态或已经开始执行；检查 status(id)。 |
| 成功后 status(id) 返回 None | 任务可能未被接受，或记录已被有限历史淘汰。 |
| 关闭服务后句柄还没有结果 | 调用 TaskHandle::get 取得结果，再用 wait_termination 等待 worker 退出。 |

## 限制与最佳实践

- 提交和观察任务期间应保留服务实例；注册表和底层线程池都由它持有。
- submit 返回成功只表示服务接受了任务，不表示任务执行成功；结果重要时应始终检查或消费返回的句柄。
- 根据需要查询的状态数量设置 completed_history_capacity。历史仅保存在内存中，且容量有限。
- 不要把 cancel 当作停止已开始 callable 的机制。
- 需要优雅关闭线程池时调用 shutdown；应用必须等待 worker 退出时再调用 wait_termination。只有确实需要底层线程池立即停止语义时才使用 stop。

## 延伸阅读

- [项目 README](../README.zh_CN.md)
- [English user guide](user-guide.md)
- [设计说明](design.zh_CN.md)
- [API 文档](https://docs.rs/qubit-task)
