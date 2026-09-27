# qubit-task 用户指南

[English version](user-guide.md)

本指南适用于 Rust 1.94 或更高版本以及 `qubit-task` 0.6.x，面向需要把耗时工作移出请求路径、限制后台并发，并查询任务进度的 Rust 服务开发者。

## 场景：API 接受 CSV 导入后立即返回

假设一个管理 API 收到 CSV 导入请求。文件可能很大，导入还会访问数据库；让 HTTP 请求一直等到导入结束会占住连接，也不方便用户刷新页面查看进度。我们希望 API 在任务受理后返回任务 ID，后台按资源额度执行导入，调用方之后能查到成功、失败或需要人工处理的状态。

本指南按这个业务流程展开：先明确任务是否需要跨进程恢复，再注册处理器并提交请求；随后检查任务结果，按需配置资源、重试、取消和通知。示例中的 `TaskHandler` 用于演示接入边界，生产应用应在其中解析真实导入参数、调用自己的文件与数据库服务，并将业务结果写入应用管理的存储。

### 先选任务在进程退出后的去向

如果导入任务只需在当前进程运行，或结果只交还给当前调用代码，可使用 `in_memory()` 和 `submit_local`。进程退出后，尚未完成的任务和历史都会丢失。

如果进程重启后还要继续处理已受理任务，应使用可恢复的存储（本指南以 SQLite 为例），通过 `TaskRequest` 保存任务类型、处理器版本和 payload。运行中的任务在进程退出时可能已经产生外部副作用，恢复后可能再次执行；处理器应设计为幂等，或用应用自己的事务方案保护副作用。

| 选择 | 提交方式 | 进程退出后的行为 |
| --- | --- | --- |
| 临时、本进程工作 | `submit_local` 闭包 | 未完成工作和内存历史丢失；返回值通过 `LocalTaskHandle` 获取 |
| 可重建、需要恢复的工作 | 带稳定幂等键的 `TaskRequest` | 持久存储可恢复排队任务；中断的运行任务可能再次执行 |

接下来的[本地任务入门](#本地任务入门使用内存服务执行闭包)给出最短运行路径。真正需要在重启后恢复的导入任务，请继续看[注册版本化处理器并提交任务](#注册版本化处理器并提交任务)。

读者完成主要接入后，可按实际问题继续查阅：资源不足或队列已满时看[资源额度与队列](#让任务按资源额度运行)；需要跨重启恢复时看[SQLite 恢复](#使用-sqlite-在重启后恢复)；需要查看历史、取消或重试时看[查询、取消和重试](#查询取消和重试)；通知和组件替换属于可选集成。

## 选择存储提供的保证

`TaskExecutionService` 对外只有一个门面。服务实际装配的 `TaskStore` 决定历史是否跨重启保留，以及已接受的任务能否恢复。

| 配置 | 完成历史 | 服务重启后尚未完成的任务 |
| --- | --- | --- |
| `TaskExecutionService::in_memory()` | 有界内存历史 | 进程退出后丢失 |
| 具有持久历史的自定义 `TaskStore` | 持久化 | 取决于存储声明的恢复能力 |
| `TaskExecutionServiceBuilder::recoverable_sqlite(path)` | SQLite | 恢复排队任务；中断的运行任务可能再次执行 |

恢复执行提供至少一次保证。进程退出前，处理器可能已经产生外部副作用，因此在重复副作用不安全时，处理器应使用幂等键或自己的事务方案。

## 本地任务入门：使用内存服务执行闭包

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

## 注册版本化处理器并提交任务

对于需要从存储中恢复的 CSV 导入，使用 `TaskRequest`。请求会保存任务类型、精确处理器版本、不透明 payload、资源需求和可选的关联键与幂等键。payload 由处理器自行解码；服务不会把旧请求静默交给新版本处理器。提交成功后，API 可把返回的 `TaskId` 发给客户端，客户端用 `get`、`list` 或 `wait` 查询进度。

实现 `TaskHandler`，并在异步构建器调用 `build()` 前注册它的 `Arc`。重复的 `(task_type, version)` 注册会被拒绝。恢复时找不到处理器的任务会进入 `Blocked`，并继续保留供查询；安装相应处理器后，可调用 `retry_blocked` 重新排队。

下面先注册 `csv-import` 的 `1` 版处理器，再提交请求并等待完成。示例的 payload 是不透明字节；真实服务应将文件位置、租户 ID 等必要参数编码进去，而不是把大文件本身复制进任务记录。

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
    // wait 返回终态摘要；生产代码应将 Failed、Panicked 等结果映射为业务状态。
    assert!(finished.state.is_terminal());
    service.shutdown().await?;
    Ok(())
}
~~~

这段演示处理器返回接收字节数摘要，因此 `wait` 返回的 `TaskSummary` 可用于查看最终状态和摘要；需要读取完整请求时再调用 `get` 获取 `TaskRecord`。`TaskOutput` 适合保存小型摘要或引用；较大的结果应由业务系统保存在自己的数据存储中，再返回有大小上限的引用。
需要重启后重建的任务应使用带精确处理器版本的 `TaskRequest`；现有 SQLite 重启恢复测试覆盖了公共服务门面上的该流程。

服务级 `submit` 必须使用稳定且非空的幂等键，并在首次调用前生成和保存。调用方超时后，
可用 `get_by_idempotency_key` 查询；该接口返回不含 payload 的 `TaskSummary`，返回
`None` 只代表查询瞬间没有记录，应以相同请求和同一键重试。需要 payload 时调用
`get(summary.id)`。
对应任务记录被清理或淘汰后，该键可以重用。内存预设最多保留 64 MiB 的任务 payload，默认最多有
64 个受理中提交，共享 64 MiB 的受理 payload 预算。这些额度只统计 payload 字节，不是进程总内存上限。
需要更长的恢复窗口时应选 SQLite 或其他持久化存储。`shutdown_until(deadline)` 会启动正常排空，
只限制当前调用者的等待；任务和存储所有权会保持到排空完成。

## 让任务按资源额度运行

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

I/O handler 可以请求零 CPU 槽，但仍占用 `max_running_tasks` 名额。显式设置该上限可控制并发网络或磁盘操作。CPU 密集型 handler 应至少请求一个槽，并通过 `spawn_blocking` 或专用执行后端运行阻塞工作。

~~~rust,no_run
use std::num::NonZeroUsize;
use qubit_task::model::TaskRequest;
use qubit_task::service::TaskExecutionServiceBuilder;

let builder = TaskExecutionServiceBuilder::in_memory()
    .max_running_tasks(NonZeroUsize::new(64).expect("limit must be positive"));
let mut request = TaskRequest::new("http-fetch", "1", Vec::new());
request.resources.cpu_slots = 0;
~~~

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

SQLite schema 3 将请求元数据、payload BLOB 与生命周期 JSON 分列保存，摘要查询和状态转换不读取或解码 payload。schema 0/1/2 数据库在打开时以单个事务迁移到 schema 3，并保留任务与幂等索引；更高的未知 schema 版本或未知记录格式会明确报错。操作系统文件锁确保同一数据库不会同时由多个服务进程执行。构建器取得所有权并扫描未完成任务后才返回。数据库被占用或所选能力不支持恢复时，服务启动失败，不会自动回退到内存。找不到历史任务对应的处理器时，任务保留在存储中并置为 `Blocked`，构建仍可成功。

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

调用 `shutdown()` 时，服务先停止新的受理并等待进行中的提交完成受理，再等待已受理任务结束；随后关闭通知入队并排空队列中的事件。存储故障路径在服务取得关闭协调权后也会执行相同的通知排空。服务自有发布器占用一条线程；服务不创建订阅时，不会产生订阅接收线程。通知线程在此期间发生 panic 时，`worker_panicked` 会记录故障，尚未处理的通知可能丢失，`shutdown()` 会返回 `TaskServiceError::NotificationClose`。等待 blocking close 任务失败时也返回这一错误。通知关闭失败不会回滚任务状态；并发或后续的 shutdown 调用会收到相同的关闭结果。该方法不会关闭由应用持有的事件总线。同步 provider 在专用操作系统线程上运行，不会占用 Tokio runtime worker；`shutdown()` 默认最多等待通知线程 30 秒，可通过 `event_bus_close_timeout(Duration)` 配置。超时会返回 `NotificationClose`，但发布线程会继续处理已经接收的通知；并发或后续 shutdown 调用会收到相同的已保存结果。直接丢弃服务而不调用 `shutdown()` 时，发送端关闭后发布线程仍会排空已入队通知再退出，耗时取决于 provider 是否返回。

## 错误与诊断

如果引擎的 `prepare()` 返回 `EngineError::Closed`，服务会将其视为永久调度故障，停止受理并返回
`SchedulerUnavailable`；排队记录保留在存储中以便恢复。`activate()` 返回同一错误时只影响当前尝试，
该任务会进入 `Blocked`。取消等待 builder `build()` 的 future 不会取消后台构建 worker：worker 会在恢复页边界
停止扫描，释放已取得的 owner，并且不启动调度器。调用方取消后，这些清理会在后台异步完成。

`TaskRunError` 用错误类别、诊断文本和可重试标记描述处理器失败。不可重试错误会以 `Failed` 结束，处理器 panic 则以 `Panicked` 结束。持久化诊断文本有长度上限；如需保留更完整的信息，应由业务应用记录原始错误或写入自己的结果存储。进程内任务的 `LocalTaskHandle<R, E>` 会保留原始类型化错误。

`Blocked` 记录可查询，并附有需要人工处理的原因，例如找不到对应版本的处理器，或重试时队列已满。可用 `get` 或 `list` 查看记录，修复注册或容量问题后调用 `retry_blocked`。生命周期事件可通过 `notification_stats()` 区分本地队列丢弃、发布错误和 worker 故障；这些计数不表示订阅者已处理事件。

## 查询、取消和重试

历史页使用 `(accepted_at_ms, id)` 复合游标，保证同一毫秒受理的任务也有稳定顺序。SQLite
历史默认永久保留；调用方可显式调用
`prune_terminal_before(accepted_before_ms, max_rows)` 清理受理时间早于阈值的终态记录，
每次最多删除 `max_rows` 条。排队、运行中和 `Blocked` 记录不会被删除。清理会同时移除
幂等键，因此该键之后可以重新受理。需要归档时，请在清理前备份持久历史。Builder 的
常见维护任务可先归档 30 天以前的记录，再循环调用
`prune_terminal_before(cutoff, 100)`，直到单次删除数少于 100。应单独检查
`Blocked` 摘要；符合策略时先用 `abandon_blocked(id, state_version)` 按版本放弃，
再在后续清理中删除。Builder 的
`runtime_handle(Handle)` 指定服务后台任务使用的 runtime；该 runtime 须存活到
`shutdown()` 返回。丢弃最后一个服务句柄会启动异步排空，但无法向调用方报告结果；需要确认任务和通知都已完成时应显式调用 `shutdown()`。通过 `runtime_handle` 注入的 runtime 必须保持运行，直到排空完成。调度器 panic 会唤醒等待者并返回 `TaskServiceError::SchedulerUnavailable`；存储故障仍返回 `StoreUnavailable`。调度器不会自动重启。自定义引擎一旦启动工作就必须返回可跟踪的执行句柄；如果引擎在启动未跟踪的副作用后 panic，应用应终止并由外部监督器重启进程。
服务与两种内置 store 都将 `TaskQuery.limit` 限制为 256；超过上限返回
`InvalidRequest`，`limit=0` 按 1 处理。内存 store 的分页选择额外空间随页长有界增长。

应用先按自身需要归档记录，再可用以下批处理函数清理 30 天以前的终态记录：

~~~rust,no_run
use std::num::NonZeroUsize;
use qubit_task::store::{StoreError, TaskStore};

async fn prune_old_terminal(
    store: &impl TaskStore,
    now_ms: u64,
) -> Result<usize, StoreError> {
    let cutoff = now_ms.saturating_sub(30 * 24 * 60 * 60 * 1_000);
    let batch = NonZeroUsize::new(100).expect("100 is nonzero");
    let mut total = 0;
    loop {
        let removed = store.prune_terminal_before(cutoff, batch).await?;
        total += removed;
        if removed < batch.get() {
            return Ok(total);
        }
    }
}
~~~

~~~rust,no_run
use qubit_task::service::TaskExecutionServiceBuilder;

let service = TaskExecutionServiceBuilder::in_memory()
    .runtime_handle(tokio::runtime::Handle::current())
    .build()
    .await?;
~~~

使用 `get(TaskId)` 查询最新记录，使用 `list(TaskQuery)` 分页查看保留历史。`wait(TaskId)` 等待任务进入终态；如果任务进入 `Blocked` 并需要人工干预，等待会返回相应错误。`cancel(TaskId)` 可以立即取消排队任务。对于运行中任务，它会持久化 `cancel_requested` 并在 `TaskContext` 中设置协作取消信号；这只是取消请求。处理器必须返回 `TaskRunOutcome::Cancelled`，服务才会以 `TaskState::Cancelled` 确认取消。如果处理器返回成功或失败，那个结果仍是权威结果。协作取消集成测试覆盖了这一契约。

处理器用 `TaskRunError` 返回错误类别、诊断信息和是否可重试。不可重试错误进入 `Failed`；执行引擎报告的 panic 进入 `Panicked`，不再根据业务错误类别字符串推断。自动重试默认采用 1 秒起步、逐次翻倍、最高 60 秒的退避，可用 `TaskExecutionServiceBuilder::retry_policy(RetryPolicy::new(initial, maximum)?)` 配置。到期时间与排队状态一同持久化，重启后不会提前执行；`retry_blocked` 会清除到期时间并立即使任务可运行。队列满时任务进入 `Blocked`，不会突破队列上限。

## 从 0.5 及更早 API 迁移

0.6 移除了调用方指定任务 ID、用 `submit` 提交闭包、线程池专用构建配置和旧的
`TaskHandle<R, E>`。进程内闭包改用 `submit_local`，并通过
`LocalTaskHandle<R, E>` 取得类型化结果；需要重建或恢复的任务使用带稳定幂等键的
`TaskRequest`，服务负责生成 `TaskId`。这两种提交方式分别表达本地结果和可恢复描述，
没有通用的持久化任务句柄。

查询与扩展接口也有不兼容变化：`TaskQuery.states` 改为
`Vec<TaskStateKind>`，历史分页游标改为 `TaskCursor { accepted_at_ms, id }`，
调度策略收到的 `QueuedTask` 只暴露 `resources`。自定义 `TaskStore` 必须实现
单次聚合统计 `count_states()`、不读取 payload 的摘要查询 `get_summary()`、有界恢复
预检 `has_unfinished_over_limit(limit)`；`transition` 返回 `TaskSummary`，`list` 的
分页记录也为摘要。存储还可实现有界终态清理 `prune_terminal_before` 和带版本检查的
`abandon_blocked`；默认不支持时会明确返回 `UnsupportedCapability`。

SQLite 释放 owner 后会拒绝旧句柄写入；SQLite 操作通过 Tokio blocking worker 串行执行，
调用方须在 Tokio runtime 中轮询异步接口。升级前应同步更新应用调用点和自定义
`TaskStore`，再运行应用的编译与恢复测试。当前工作区没有直接依赖 rs-task 的兄弟 crate。

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

## 无 payload 状态查询与 Blocked 运维

`TaskPage.records`、`wait` 和 `retry_blocked` 返回 `TaskSummary`。它包含请求元数据、
生命周期状态和输出摘要，但没有 payload 字段。单条状态查询可用 `get_summary`；
只有业务代码需要完整请求和 payload 时才调用 `get`。SQLite schema 3 将
`request_info_json`、`payload BLOB` 和 `lifecycle_json` 分列保存；历史查询、状态等待
和状态转换只读取元数据与生命周期列。schema 0、1、2 会在一个事务中迁移，并保留任务
payload、幂等键和生命周期值。

运维人员可按状态分页浏览 `Blocked` 记录，并应用年龄阈值筛选。只对人工选定放弃的记录
调用 `abandon_blocked(id, state_version)`。版本检查可防止并发 `retry_blocked` 被误取消：
若分页读取后任务已变化，放弃操作返回 `StoreError::Conflict`。分页游标不代表并发写入期间
的全局快照。放弃后，可传入截止时间和行数上限调用 `prune_terminal_before`，分批清理终态
历史。完整流程见[`blocked_maintenance.rs`](../examples/blocked_maintenance.rs)。

存储故障发生后，`wait` 和本地任务句柄会及时报告错误。共享关闭结果会等到调度器退出、
已跟踪的执行句柄结束并释放存储所有权后才完成。调用方可用 `shutdown_until` 限制本次等待；
超时不会停止后台排空，也不会提前释放所有权。

## 延伸阅读

- [项目概览与快速开始](../README.zh_CN.md)
- [API 文档](https://docs.rs/qubit-task)
- [English user guide](user-guide.md)
- [TaskExecutionService 详细设计](task_execution_service_design.md)
