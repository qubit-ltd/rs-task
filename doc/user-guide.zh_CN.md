# qubit-task 用户指南

[English version](user-guide.md)

本指南适用于 Rust 1.94 或更高版本以及 `qubit-task` 0.6.x，面向需要有界后台执行、任务历史，并需选择易失存储或重启恢复方案的 Rust 服务开发者。该 crate 接受无法在当前业务请求中完成的工作，根据资源额度安排执行，并允许业务系统稍后查询任务进度。

## 概念模型

`TaskRequest` 描述可持久化的任务：任务类型、精确的处理器版本、payload、资源需求，以及可选的业务关联键。`TaskRecord` 保存可查询的生命周期状态和有大小上限的输出摘要。`TaskHandler` 负责解释请求；`TaskStore`、`SchedulingPolicy` 和 `TaskExecutionEngine` 分别决定持久化方式、队列选择和执行容量。通过 `submit_local` 提交的闭包则使用独立的进程内结果通道 `LocalTaskHandle`，进程重启后无法重建。

服务门面负责协调这些组件。存储声明历史是否持久化、未完成任务是否可恢复；资源容量控制受理和并发执行额度，不负责发现或绑定操作系统上的 CPU、GPU。

## 场景：提交数据导入任务并及时返回

假设某个 API 收到 CSV 导入请求，需要尽快响应，同时让导入继续运行。若任务必须支持重启恢复，应把导入参数编码进带版本的 `TaskRequest`，构建服务前注册与请求版本匹配的处理器，再把受理后的任务 ID 返回调用方。调用方随后可通过 `get`、`list` 或 `wait` 查看状态。后续章节会逐步说明这一流程，以及如何选择存储和资源保证。

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

`in_memory()` 会在调用位置明确表示易失语义。它使用本机执行引擎、系统可用 CPU 并行度（无法获取时为 1）、最多 1024 个等待任务，以及最多 1024 条终态历史。它不会探测 GPU。`submit_local` 接收进程内闭包并返回类型化的 `LocalTaskHandle<R, E>`；闭包在 Tokio 阻塞线程池运行。句柄提供闭包的进程内返回值或原始错误，而 `TaskRecord.output` 只保留较小的 `TaskOutput` 摘要。自定义异步处理器应自行把长时间 CPU 运算或阻塞 I/O 移出异步工作线程。

内存 store 默认最多保留 2048 条非终态记录，`Blocked` 也计入上限。可将 `MemoryTaskStore::with_limits(history_capacity, payload_budget, unfinished_limit)` 的结果通过 `TaskExecutionServiceBuilder::store(Arc::new(...))` 注入以定制上限；达到上限会返回 `UnfinishedRecordLimitExceeded`，已有记录的幂等重放仍可成功。

~~~rust,no_run
use qubit_task::TaskExecutionService;
use qubit_task::model::TaskOutput;
use qubit_task::service::LocalTaskOutcome;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let service = TaskExecutionService::in_memory().await?;
    let handle = service.submit_local(|context| {
        assert_eq!(context.attempt(), 1);
        LocalTaskOutcome::<usize, std::io::Error>::Succeeded {
            value: 21_usize,
            summary: TaskOutput { summary: "已导入 21 行".as_bytes().to_vec() },
        }
    }).await?;
    let imported_rows = handle.result().await??;
    assert_eq!(imported_rows, 21);
    service.shutdown().await?;
    Ok(())
}
~~~

如果存储声明支持重启恢复，`submit_local` 会拒绝闭包提交，因为闭包无法在进程退出后从数据库重建。
调用方等待 `submit_local` 时取消或超时后，后台受理仍可能完成，但调用方会失去返回句柄，无法取得原始
类型化的值或错误。调用方需要在停止等待后继续找回任务时，应使用带稳定键的 `submit`。
需要协作取消时，处理器观察 `TaskContext::is_cancelled()` 后返回
`LocalTaskOutcome::Cancelled`；随后 `handle.result()` 返回
`LocalTaskResultError::Cancelled`。成功的 `TaskRunOutcome` 或 `TaskRecord.output`
不会被迟到的取消请求覆盖。

## 注册版本化处理器

对于可以重建的任务，使用 `TaskRequest`。请求会保存任务类型、精确处理器版本、不透明 payload、资源需求和可选的关联键与幂等键。payload 由处理器自行解码；服务不会把旧请求静默交给新版本处理器。

实现 `TaskHandler`，并在异步构建器调用 `build()` 前注册它的 `Arc`。重复的 `(task_type, version)` 注册会被拒绝。恢复时找不到处理器的任务会进入 `Blocked`，并继续保留供查询；安装相应处理器后，可调用 `retry_blocked` 重新排队。

~~~rust,no_run
use std::sync::Arc;
use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::handler::{TaskContext, TaskHandler, TaskHandlerDescriptor, TaskRunOutcome, TaskRunResult};
use qubit_task::model::{TaskOutput, TaskRequest};
use qubit_task::store::TaskFuture;

struct ImportV1;
impl TaskHandler for ImportV1 {
    fn descriptor(&self) -> TaskHandlerDescriptor {
        TaskHandlerDescriptor { task_type: "csv-import".into(), version: "1".into() }
    }

    fn run<'a>(&'a self, payload: &'a [u8], _context: TaskContext)
        -> TaskFuture<'a, TaskRunResult>
    {
        Box::pin(async move {
            Ok(TaskRunOutcome::Succeeded(TaskOutput {
                summary: format!("接收了 {} 字节", payload.len()).into_bytes(),
            }))
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let service = TaskExecutionServiceBuilder::in_memory()
        .register_handler(Arc::new(ImportV1))?
        .build().await?;
    let request = TaskRequest::new("csv-import", "1", b"...".to_vec())
        .with_idempotency_key("csv-import-request-42");
    let id = service.submit(request).await?.id;
    let finished = service.wait(id).await?;
    assert!(finished.state.is_terminal());
    service.shutdown().await?;
    Ok(())
}
~~~

`TaskOutput` 用于保存小型摘要或引用。较大的结果应由业务系统保存在自己的数据存储中，再返回有大小上限的引用。
需要重启后重建的任务应使用带精确处理器版本的 `TaskRequest`；现有 SQLite 重启恢复测试覆盖了公共服务门面上的该流程。

服务级 `submit` 必须使用稳定且非空的幂等键，并在首次调用前生成和保存。调用方超时后，
可用 `get_by_idempotency_key` 查询；返回 `None` 只代表查询瞬间没有记录，应以相同请求和同一键重试。
对应任务记录被清理或淘汰后，该键可以重用。内存预设最多保留 64 MiB 的任务 payload，默认最多有
64 个受理中提交，共享 64 MiB 的受理 payload 预算。这些额度只统计 payload 字节，不是进程总内存上限。
需要更长的恢复窗口时应选 SQLite 或其他持久化存储。`shutdown_until(deadline)` 会启动正常排空，
只限制当前调用者的等待；任务和存储所有权会保持到排空完成。

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

服务会根据执行引擎公布的容量校验每个请求。超出已配置容量的请求会被拒绝；当前资源不足但以后可能满足的请求会继续排队。默认公平 FIFO 策略允许符合当前资源条件的任务越过队首，并在队首任务多次被越过后为其保留执行机会。等待队列有容量限制；队列满时返回 `QueueFull`，由调用方施加背压。自动重试遇到满队列时，任务会记录为 `Blocked`，等待容量恢复后可显式重试，不会突破队列上限。

### 限制运行并发与重启恢复

资源槽位和任务并发数是两个独立上限。可用
`max_running_tasks(NonZeroUsize)` 限制同时运行的尝试数；即使请求零 CPU
槽，也会占用一个运行名额。默认值为本机可用并行度，无法获取时为 1。
重启时未完成记录数必须不超过 `queue_capacity + max_running_tasks`；否则构建失败并保留记录。调大其中一个上限后再重启。

~~~rust,no_run
use std::num::NonZeroUsize;
use qubit_task::service::TaskExecutionServiceBuilder;

let builder = TaskExecutionServiceBuilder::in_memory()
    .max_running_tasks(NonZeroUsize::new(8).expect("limit must be positive"));
~~~

`max_attempts` 统计同一任务 ID 跨进程启动的总次数。恢复时达到上限的
`Queued` 或 `Running` 记录会进入 `Blocked`，不会再次启动。对耗尽预算的记录调用
`retry_blocked` 会返回 `TaskServiceError::AttemptsExhausted`；提交新任务才能获得新预算。
此行为改变了 0.6.0 的重试契约。

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

SQLite 将不可变请求与生命周期状态分开保存，状态更新只写生命周期 JSON，不会重复写入大型 payload。schema 0/1 数据库在打开时以单个事务迁移到 schema 2，并保留任务与幂等索引；更高的未知 schema 版本或未知记录格式会明确报错。操作系统文件锁确保同一数据库不会同时由多个服务进程执行。构建器取得所有权并扫描未完成任务后才返回。数据库被占用或所选能力不支持恢复时，服务启动失败，不会自动回退到内存。找不到历史任务对应的处理器时，任务保留在存储中并置为 `Blocked`，构建仍可成功。

`capabilities()` 会报告实际装配的存储能力 `persistent_history` 和 `restart_recovery`。第三方存储也可以持久化历史，但不支持恢复任务。每个 `TaskStore` 实现都必须提供 `count_states()`，在一次聚合中统计所有保留记录。`stats()` 只调用一次该方法，并向调用者传播统计失败。状态计数和执行引擎资源快照先后读取，因此是时间相邻但非原子的两个快照。统计成本是一次聚合查询，不随历史分页数增长。

## 通过 `qubit-spi` 装配组件

`qubit-task` 为 `TaskStore`、`SchedulingPolicy`、`TaskExecutionEngine` 和 `TaskHandler` 定义了 SPI 服务族。启用 `inventory` feature 后，crate 可以收集最终程序链接的扩展 crate 所注册的 provider。应用负责选择 provider，并将创建出的 `Arc<dyn ...>` 传给 `TaskExecutionServiceBuilder::from_components`；SPI 不会推测数据库凭据、文件位置或资源容量。

内置内存存储、公平 FIFO 策略和本机执行引擎提供稳定 provider ID，可从 `qubit_task::spi` 查询。第三方 provider 实现对应的公开 `*Spec` 和 `qubit_spi::ServiceProvider`。应用装配阶段应检查 provider ID 冲突和重复的处理器类型/版本，再开始接收任务。服务实例的组件在其生命周期内保持固定。

## 发布状态事件

启用 `event-bus` feature 后，可将 `qubit_event_bus::EventBus` 具体门面注入构建器。状态变化后，服务会发布 `TaskEvent`。通知采用尽力而为语义：发布失败不会回滚任务状态。事件可能重复、延迟或丢失，因此消费者应比较 `state_version`，并在需要权威状态时查询服务。
当前版本依赖 `qubit-event-bus` 0.14。通用 `NotificationPublisher` 返回 provider receipt；服务再按 `AdmissionOutcome` 映射到现有任务通知统计。

服务使用 `rs-event-bus` 的 `NotificationPublisher` 管理串行发布线程和有界队列，默认容量为 256。可通过
`event_bus_buffer_capacity(NonZeroUsize)` 设置其他正数容量。状态转移只调用
`NotificationPublisher::try_publish`，不会等待事件总线完成发布；队列已满时丢弃新通知。服务关闭并停止接收入队后，晚到的通知也会丢弃，这些丢弃都不会改变任务结果。

配置了事件总线时，`notification_stats()` 返回通知统计快照；未配置时返回
`None`。`enqueued` 是成功进入本地队列的事件数，`queue_full` 和 `queue_closed`
分别统计因队列已满、队列已关闭而丢弃的事件。`accepted` 表示至少一个可见目的地接受了事件；其中同时存在拒绝目的地的事件也计入 `partial_rejection`。
`opaque_accepted` 表示 provider 报告接受、但没有公开目的地信息。`unaccepted`
统计没有任何已报告目的地接受的回执，包括空目的地列表和拦截器丢弃。
`publish_error` 统计发布调用返回错误的次数，`worker_panicked` 记录发布线程 panic。
这些值表示队列接纳、provider 回执或线程状态，不代表订阅者 handler 已处理完成。
计数单调递增并在 `u64::MAX` 饱和；同一快照的各字段不保证来自完全相同的时刻。

调用 `shutdown()` 时，服务先停止新的受理并等待进行中的提交完成受理，再等待已受理任务结束；随后关闭通知入队并排空队列中的事件。存储故障路径在服务取得关闭协调权后也会执行相同的通知排空。服务自有发布器占用一条线程；服务不创建订阅时，不会产生订阅接收线程。通知线程在此期间发生 panic 时，`worker_panicked` 会记录故障，尚未处理的通知可能丢失，`shutdown()` 会返回 `TaskServiceError::NotificationClose`。等待 blocking close 任务失败时也返回这一错误。通知关闭失败不会回滚任务状态；并发或后续的 shutdown 调用会收到相同的关闭结果。该方法不会关闭由应用持有的事件总线。同步 provider 在专用操作系统线程上运行，不会占用 Tokio runtime worker；但如果 provider 永不返回，该线程就无法完成发布，`shutdown()` 仍可能无限等待。不调用 `shutdown()` 而直接丢弃服务时，发送端关闭后发布线程仍会排空已入队通知再退出，同样受 provider 是否返回的影响。

## 错误与诊断

`TaskRunError` 用错误类别、诊断文本和可重试标记描述处理器失败。不可重试错误会以 `Failed` 结束，处理器 panic 则以 `Panicked` 结束。持久化诊断文本有长度上限；如需保留更完整的信息，应由业务应用记录原始错误或写入自己的结果存储。进程内任务的 `LocalTaskHandle<R, E>` 会保留原始类型化错误。

`Blocked` 记录可查询，并附有需要人工处理的原因，例如找不到对应版本的处理器，或重试时队列已满。可用 `get` 或 `list` 查看记录，修复注册或容量问题后调用 `retry_blocked`。生命周期事件可通过 `notification_stats()` 区分本地队列丢弃、发布错误和 worker 故障；这些计数不表示订阅者已处理事件。

## 查询、取消和重试

历史页使用 `(accepted_at_ms, id)` 复合游标，保证同一毫秒受理的任务也有稳定顺序。SQLite
历史默认永久保留；调用方可显式调用
`prune_terminal_before(accepted_before_ms, max_rows)` 清理受理时间早于阈值的终态记录，
每次最多删除 `max_rows` 条。排队、运行中和 `Blocked` 记录不会被删除。清理会同时移除
幂等键，因此该键之后可以重新受理。需要归档时，请在清理前备份持久历史。Builder 的
`runtime_handle(Handle)` 指定服务后台任务使用的 runtime；该 runtime 须存活到
`shutdown()` 返回。
服务与两种内置 store 都将 `TaskQuery.limit` 限制为 256；超过上限返回
`InvalidRequest`，`limit=0` 按 1 处理。内存 store 的分页选择额外空间随页长有界增长。

~~~rust,no_run
use qubit_task::service::TaskExecutionServiceBuilder;

let service = TaskExecutionServiceBuilder::in_memory()
    .runtime_handle(tokio::runtime::Handle::current())
    .build()
    .await?;
~~~

使用 `get(TaskId)` 查询最新记录，使用 `list(TaskQuery)` 分页查看保留历史。`wait(TaskId)` 等待任务进入终态；如果任务进入 `Blocked` 并需要人工干预，等待会返回相应错误。`cancel(TaskId)` 可以立即取消排队任务。对于运行中任务，它会持久化 `cancel_requested` 并在 `TaskContext` 中设置协作取消信号；这只是取消请求。处理器必须返回 `TaskRunOutcome::Cancelled`，服务才会以 `TaskState::Cancelled` 确认取消。如果处理器返回成功或失败，那个结果仍是权威结果。协作取消集成测试覆盖了这一契约。

处理器用 `TaskRunError` 返回错误类别、诊断信息和是否可重试。不可重试错误进入 `Failed`；执行引擎报告的 panic 进入 `Panicked`，不再根据业务错误类别字符串推断。自动重试默认采用 1 秒起步、逐次翻倍、最高 60 秒的退避，可用 `TaskExecutionServiceBuilder::retry_policy(RetryPolicy::new(initial, maximum)?)` 配置。到期时间与排队状态一同持久化，重启后不会提前执行；`retry_blocked` 会清除到期时间并立即使任务可运行。队列满时任务进入 `Blocked`，不会突破队列上限。

## 从旧版 API 迁移

本次重设计移除调用方提供的 ID、`submit` 闭包、线程池专属 builder 选项和旧的 `TaskHandle<R, E>`。这里没有通用的持久化句柄：`submit_local` 现在为进程内闭包返回 `LocalTaskHandle<R, E>`；需要重建的任务仍使用 `TaskRequest` 和服务生成的 `TaskId`。第三方 `TaskStore` 需要新增 `count_states()`，一次聚合返回所有保留状态的计数。这些是有意的源码破坏性变更，下游实现和调用点应一起迁移。当前工作区中没有 `rs-*` crate 直接依赖 `rs-task`。

`TaskQuery.states` 现在是 `Vec<TaskStateKind>`；筛选只比较生命周期类别，忽略
失败消息和阻塞原因等诊断内容。关闭开始后服务会拒绝写操作。SQLite 写入受存储
所有权 fencing 保护，旧 store 句柄也不能绕过。SQLite 同时只执行一个阻塞数据库
操作；调用方须在 Tokio runtime 中轮询 store 操作。

历史分页的 `TaskQuery.after` 和 `TaskPage.next` 已改为
`TaskCursor { accepted_at_ms, id }`。第三方 `SchedulingPolicy` 收到的 `QueuedTask`
现在包含 `resources`，不再包含完整 `TaskRequest`。`TaskStore` 新增
`prune_terminal_before`；默认实现返回 `UnsupportedCapability`。

## 排障

- **提交时收到 `QueueFull`：** 有界等待队列已满。调用方可以施加背压、等待队列前进；如果部署能够安全保留更多待执行任务，也可以提高队列上限。自动重试遇到队列满时会进入 `Blocked`；有空位后调用 `retry_blocked`。
- **重启后任务仍处于 `Blocked`：** 查看持久化诊断，并确认服务注册了完全匹配 `(task_type, handler_version)` 的处理器。修正注册或原因后调用 `retry_blocked`。
- **调用 `cancel` 后任务仍在运行：** 取消采用协作方式。处理器需要检查 `TaskContext::is_cancelled()` 并返回 `TaskRunOutcome::Cancelled`；服务不会强行中断任意代码。
- **SQLite 服务启动失败：** 检查数据库路径是否可用，以及是否已有其他服务进程持有数据库。恢复失败时不会自动回退到内存存储。
- **没有收到状态事件：** 检查 `notification_stats()` 中的队列丢弃、provider 错误或发布线程 panic 计数。事件采用尽力而为语义；任务状态应以服务查询结果为准。

## 运行限制

请求上限按 UTF-8 字节数计算：`task_type` 128、`handler_version` 64、
`correlation_key` 和 `idempotency_key` 各 256；metadata 最多 32 项，键 128、
值 4096、键值合计 16384。超限请求在受理前被拒绝。持久化诊断类别最多 128
字节；阻塞原因、panic 消息和其他诊断消息最多 4096 字节。执行诊断会在有效的
UTF-8 字符边界裁剪；`LocalTaskHandle` 仍保留原始类型化结果和错误值。

本版本只在单个服务进程内调度任务，不提供多节点租约、分布式资源发现、工作流依赖、定时任务、任意代码强制中断或业务副作用恰好一次保证。未来的分布式执行实现可以实现相同的 `TaskExecutionEngine` 接口，而不要求更改服务门面。

## 延伸阅读

- [项目概览与快速开始](../README.zh_CN.md)
- [API 文档](https://docs.rs/qubit-task)
- [English user guide](user-guide.md)
- [TaskExecutionService 详细设计](task_execution_service_design.md)
