# qubit-task 用户指南

[English version](user-guide.md)

本指南适用于 Rust 1.94 或更高版本以及 `qubit-task` 0.6.x。该 crate 接受无法在当前业务请求中完成的工作，根据资源额度安排执行，并允许业务系统稍后查询任务进度。

## 选择持久化保证

`TaskExecutionService` 对外只有一个门面。服务实际装配的 `TaskStore` 决定历史是否跨重启保留，以及已接受的任务能否恢复。

| 配置 | 完成历史 | 服务重启后尚未完成的任务 |
| --- | --- | --- |
| `TaskExecutionService::in_memory()` | 有界内存历史 | 进程退出后丢失 |
| 具有持久历史的自定义 `TaskStore` | 持久化 | 取决于存储声明的恢复能力 |
| `recoverable_sqlite(path)` | SQLite | 恢复排队任务；中断的运行任务可能再次执行 |

恢复执行提供至少一次保证。进程退出前，处理器可能已经产生外部副作用，因此在重复副作用不安全时，处理器应使用幂等键或自己的事务方案。

## 使用内存服务执行本地任务

在应用中加入 crate 和异步运行时：

~~~toml
[dependencies]
qubit-task = "0.6"
tokio = { version = "1.53", features = ["macros", "rt-multi-thread"] }
~~~

`in_memory()` 会在调用位置明确表示易失语义。它使用本机执行引擎、系统可用 CPU 并行度（无法获取时为 1）、最多 1024 个等待任务，以及最多 1024 条终态历史。它不会探测 GPU。`submit_local` 接收进程内闭包并返回 `TaskId`；闭包在 Tokio 阻塞线程池运行，可以用该 ID 查询或等待任务状态和结果摘要。自定义异步处理器应自行把长时间 CPU 运算或阻塞 I/O 移出异步工作线程。

~~~rust,no_run
use qubit_task::TaskExecutionService;
use qubit_task::model::TaskOutput;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let service = TaskExecutionService::in_memory().await?;
    let id = service.submit_local(|context| {
        assert_eq!(context.attempt(), 1);
        Ok(TaskOutput { summary: b"已导入 21 行".to_vec() })
    }).await?;
    let record = service.wait(id).await?;
    println!("{}: {:?}", record.id, record.state);
    service.shutdown().await?;
    Ok(())
}
~~~

如果存储声明支持重启恢复，`submit_local` 会拒绝闭包提交，因为闭包无法在进程退出后从数据库重建。

## 注册版本化处理器

对于可以重建的任务，使用 `TaskRequest`。请求会保存任务类型、精确处理器版本、不透明 payload、资源需求和可选的关联键与幂等键。payload 由处理器自行解码；服务不会把旧请求静默交给新版本处理器。

实现 `TaskHandler`，并在异步构建器调用 `build()` 前注册它的 `Arc`。重复的 `(task_type, version)` 注册会被拒绝。恢复时找不到处理器的任务会进入 `Blocked`，并继续保留供查询；安装相应处理器后，可调用 `retry_blocked` 重新排队。

~~~rust,no_run
use std::sync::Arc;
use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::handler::{TaskContext, TaskHandler, TaskHandlerDescriptor};
use qubit_task::model::{TaskOutput, TaskRequest, TaskRunError};
use qubit_task::store::TaskFuture;

struct ImportV1;
impl TaskHandler for ImportV1 {
    fn descriptor(&self) -> TaskHandlerDescriptor {
        TaskHandlerDescriptor { task_type: "csv-import".into(), version: "1".into() }
    }

    fn run<'a>(&'a self, payload: &'a [u8], _context: TaskContext)
        -> TaskFuture<'a, Result<TaskOutput, TaskRunError>>
    {
        Box::pin(async move {
            Ok(TaskOutput { summary: format!("接收了 {} 字节", payload.len()).into_bytes() })
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let service = TaskExecutionServiceBuilder::in_memory()
        .register_handler(Arc::new(ImportV1))?
        .build().await?;
    let id = service.submit(TaskRequest::new("csv-import", "1", b"...".to_vec())).await?.id;
    let finished = service.wait(id).await?;
    assert!(finished.state.is_terminal());
    service.shutdown().await?;
    Ok(())
}
~~~

`TaskOutput` 用于保存小型摘要或引用。较大的结果应由业务系统保存在自己的数据存储中，再返回有大小上限的引用。

## 调度 CPU、GPU 和业务自定义资源

构建器接受显式的 `ResourceCapacity`。CPU 槽位表示并发预算，不会绑定操作系统 CPU。GPU 设备及标签需要由部署配置提供。自定义整数额度可表示内存单位、许可证数量或其他独占资源，但部署方必须统一这些额度的含义。

~~~rust,no_run
use std::collections::BTreeMap;
use qubit_task::model::ResourceCapacity;
use qubit_task::service::TaskExecutionServiceBuilder;

let capacity = ResourceCapacity {
    cpu_slots: 8,
    gpus: BTreeMap::from([
        ("gpu-0".into(), vec!["cuda".into()]),
        ("gpu-1".into(), vec!["cuda".into()]),
    ]),
    custom: BTreeMap::from([("memory_mib".into(), 32_768)]),
};
let builder = TaskExecutionServiceBuilder::in_memory().capacity(capacity);
~~~

服务会根据执行引擎公布的容量校验每个请求。超出已配置容量的请求会被拒绝；当前资源不足但以后可能满足的请求会继续排队。默认公平 FIFO 策略允许符合当前资源条件的任务越过队首，并在队首任务多次被越过后为其保留执行机会。等待队列有容量限制；队列满时返回 `QueueFull`，由调用方施加背压。

## 使用 SQLite 在重启后恢复

启用可选 feature 并指定持久化路径：

~~~toml
[dependencies]
qubit-task = { version = "0.6", features = ["sqlite"] }
~~~

~~~rust,ignore
let service = TaskExecutionServiceBuilder::recoverable_sqlite("./state/tasks.sqlite")?
    .register_handler(Arc::new(ImportV1))?
    .require_recovery(true)
    .build().await?;
~~~

SQLite 以事务方式保存任务请求和状态变化。操作系统文件锁确保同一数据库不会同时由多个服务进程执行。构建器取得所有权并扫描未完成任务后才返回。数据库被占用或所选能力不支持恢复时，服务启动失败，不会自动回退到内存。找不到历史任务对应的处理器时，任务保留在存储中并置为 `Blocked`，构建仍可成功。

`capabilities()` 会报告实际装配的存储能力 `persistent_history` 和 `restart_recovery`。第三方存储也可以持久化历史，但不支持恢复任务。

## 通过 `qubit-spi` 装配组件

`qubit-task` 为 `TaskStore`、`SchedulingPolicy`、`TaskExecutionEngine` 和 `TaskHandler` 定义了 SPI 服务族。启用 `inventory` feature 后，crate 可以收集最终程序链接的扩展 crate 所注册的 provider。应用负责选择 provider，并将创建出的 `Arc<dyn ...>` 传给 `TaskExecutionServiceBuilder::from_components`；SPI 不会推测数据库凭据、文件位置或资源容量。

内置内存存储、公平 FIFO 策略和本机执行引擎提供稳定 provider ID，可从 `qubit_task::spi` 查询。第三方 provider 实现对应的公开 `*Spec` 和 `qubit_spi::ServiceProvider`。应用装配阶段应检查 provider ID 冲突和重复的处理器类型/版本，再开始接收任务。服务实例的组件在其生命周期内保持固定。

## 发布状态事件

启用 `event-bus` feature 后，可将 `qubit_event_bus::EventBus` 具体门面注入构建器。状态变化后，服务会发布 `TaskEvent`。通知采用尽力而为语义：发布失败不会回滚任务状态。事件可能重复、延迟或丢失，因此消费者应比较 `state_version`，并在需要权威状态时查询服务。
当前版本将 `qubit-event-bus` 0.12 固定到 revision
`319fffb85c150c0d2b2f83655ee06799b19ef035`；发布器直接识别该 revision 的
`PublishAcknowledgement`，不依赖更新版本提供的接纳检查 API。

服务为通知创建一个串行发布线程和有界队列，默认容量为 256。可通过
`event_bus_buffer_capacity(NonZeroUsize)` 设置其他正数容量。状态转移只调用
`try_send`，不会等待事件总线完成发布；队列已满时丢弃新通知。服务关闭并停止接收入队后，晚到的通知也会丢弃，这些丢弃都不会改变任务结果。

配置了事件总线时，`notification_stats()` 返回通知统计快照；未配置时返回
`None`。`enqueued` 是成功进入本地队列的事件数，`queue_full` 和 `queue_closed`
分别统计因队列已满、队列已关闭而丢弃的事件。`accepted` 表示至少一个可见目的地接受了事件；其中同时存在拒绝目的地的事件也计入 `partial_rejection`。
`opaque_accepted` 表示 provider 报告接受、但没有公开目的地信息。`unaccepted`
统计没有任何已报告目的地接受的回执，包括空目的地列表和拦截器丢弃。
`publish_error` 统计发布调用返回错误的次数，`worker_panicked` 记录发布线程 panic。
这些值表示队列接纳、provider 回执或线程状态，不代表订阅者 handler 已处理完成。
计数单调递增并在 `u64::MAX` 饱和；同一快照的各字段不保证来自完全相同的时刻。

调用 `shutdown()` 时，服务先等待已受理任务结束，再关闭通知入队并排空队列中的事件，然后返回。它不会关闭由应用持有的事件总线。同步 provider 在专用操作系统线程上运行，不会占用 Tokio runtime worker；但如果 provider 永不返回，该线程就无法完成发布，`shutdown()` 也可能无限等待。不调用 `shutdown()` 而直接丢弃服务时，发送端关闭后发布线程仍会排空已入队通知再退出，同样受 provider 是否返回的影响。若发布线程发生 panic，`worker_panicked` 会记录此情况，shutdown 仍可观察到线程退出，但队列中尚未处理的通知可能丢失。

## 查询、取消和重试

使用 `get(TaskId)` 查询最新记录，使用 `list(TaskQuery)` 分页查看保留历史。`wait(TaskId)` 等待任务进入终态；如果任务进入 `Blocked` 并需要人工干预，等待会返回相应错误。`cancel(TaskId)` 可以立即取消排队任务。对于运行中任务，它只会在 `TaskContext` 中设置协作取消信号；处理器必须观察信号并退出后，执行引擎才会释放资源。

处理器用 `TaskRunError` 返回错误类别、诊断信息和是否可重试。不可重试错误进入 `Failed`，panic 进入 `Panicked`。可重试错误最多自动尝试三次（可通过构建器调整），达到上限后进入 `Blocked`。修复原因后再调用 `retry_blocked`。

## 从旧版 API 迁移

本次重设计有意移除调用方提供的 ID、`submit` 闭包、线程池专属 builder 选项和旧的 `TaskHandle<R, E>`。请改用服务生成的 `TaskId`、配合版本化处理器的可恢复 `TaskRequest`，或只适用于进程内任务的 `submit_local`。不要再依赖终态后复用 ID 的行为。

## 运行限制

本版本只在单个服务进程内调度任务，不提供多节点租约、分布式资源发现、工作流依赖、定时任务、任意代码强制中断或业务副作用恰好一次保证。未来的分布式执行实现可以实现相同的 `TaskExecutionEngine` 接口，而不要求更改服务门面。
