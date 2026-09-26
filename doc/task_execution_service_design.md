# qubit-task：资源感知的异步任务执行服务设计

## 1. 目标与边界

`qubit-task` 面向业务系统提交后不能立即完成的任务。提交者得到任务标识，之后通过查询或事件了解排队、运行和完成情况；服务依据本机可用的 CPU、GPU 和业务自定义资源安排执行。

对业务系统只暴露一个 `TaskExecutionService` 门面。实际存储能力由所装配的 `TaskStore` 决定，服务通过 `capabilities()` 报告装配后的能力。下表是典型能力组合，不要求用封闭枚举限制未来的组合：

| 存储能力 | 任务队列与当前状态 | 完成记录 | 进程重启后的未完成任务 |
| --- | --- | --- | --- |
| `Volatile` | 内存 | 有界内存 | 不恢复 |
| `PersistentHistory` | 内存 | 持久化 | 不恢复 |
| `Recoverable` | 持久化 | 持久化 | 恢复排队任务；重新执行中断的运行任务 |

本期只在一个服务器进程内调度和执行。存储后端可由业务系统在构造服务时注入，`qubit-task` 不强制绑定 SQL、Redis、MongoDB 或文件系统。任务的跨节点分配、自动故障转移、工作流依赖、周期任务、强制中断任意运行中代码，以及恰好一次的业务副作用，不属于本期能力。

这里的“异步”指提交后立即得到受理结果、任务随后执行，以及提供异步查询和等待接口；不要求每个业务处理器都是 Rust `Future`。CPU 密集和阻塞任务必须在合适的工作线程运行，不能阻塞调度循环。

## 2. 现状与重新设计的原因

当前 `TaskExecutionService` 使用 `qubit-thread-pool::ThreadPool`，支持调用方给定的 `qubit_id::Id`、状态查询、提交前取消和默认保留 1024 条终态记录。终态 ID 可重用；历史只有有界内存状态。现有实现不按资源需求决定启动时机，也不能从持久化任务描述重建任务。`rs-execution-services` 已有 CPU、阻塞和 Tokio IO 执行域，可供本机执行后端复用。

新设计保留“执行基础设施”和“业务任务调度”之间的层次：任务执行引擎负责资源预约和运行工作；`qubit-task` 的服务内核负责受理、队列、状态和通知。需要调整现有公开 API，不能把旧版状态和 ID 复用语义直接视为新版契约。

## 3. 核心模型

### 3.1 任务身份与描述

- `TaskId` 标识一次提交，由服务生成，提交后保持稳定；业务自己的标识放在 `correlation_key` 中。`TaskExecutionService::submit` 必须提供稳定且非空的 `idempotency_key`。同一键与相同任务描述重复提交时返回原 `TaskId`；内容不同则拒绝。`get_by_idempotency_key` 返回 `None` 只描述查询瞬间，调用方须用同键、同请求重试。去重键的有效期与存储保留期一致，记录过期后键可重用。
- `TaskRequest` 包含 `task_type`、`handler_version`、有大小上限的 `payload`、资源需求和可选业务关联字段。它不包含进程地址、闭包或持久化后无法重建的对象。
- 处理器通过 `TaskHandlerRegistry` 在服务启动时按 `(task_type, handler_version)` 注册。注册表可由 `rs-spi` 发现的处理器 provider 构建，也允许应用显式注入实例。服务恢复旧任务前检查处理器是否存在；缺失时将任务置为 `Blocked` 并报告不可运行原因，不把它当作业务执行失败，也不静默丢弃。当前没有 payload 预检接口，解码失败由处理器返回为执行错误。
- `submit` 接受可重建的 `TaskRequest` 并返回受理时的 `TaskRecord`。`submit_local` 接受本地闭包并返回 `LocalTaskHandle<R, E>`：句柄提供仅存在于当前进程的完整结果值或原始业务错误，同时 `TaskRecord.output` 仅保存小型摘要或引用。该方法仅在不承诺重启恢复的存储上可用，使用可恢复存储时明确拒绝。两种提交方式始终通过同一个服务门面。
- `TaskContext` 向处理器提供 `TaskId`、尝试次数、取消信号和实际分配的资源标识。大结果由业务写入外部存储；任务记录只保存有大小上限的结果摘要或引用、错误类别和诊断信息。

### 3.2 资源模型

`ResourceCapacity` 在构造服务时配置本机容量：CPU 并发槽位、GPU 设备列表，以及业务自定义的非负整数资源额度。`ResourceRequest` 声明任务启动到退出期间需要独占的额度；GPU 请求为设备数量或符合指定标签的设备，执行引擎返回实际分配的设备 ID。CPU 槽位是并发预算，不等同于操作系统 CPU 隔离；GPU 数量不自动推断显存或实际占用。需要显存配额时将其显式建模为自定义额度，并由部署方保证额度含义一致。

提交时验证请求的每项需求不超过配置容量，无法满足的任务立即拒绝。通过验证但当前没有空闲额度的任务进入有界队列。调度器选择候选任务，`TaskExecutionEngine` 原子预约全部资源并安排执行；任务实际结束后释放预约。不能先占用部分资源再等待其余部分，以免产生资源死锁。容量变更本期仅在重建服务时生效。

队列默认按受理顺序扫描，允许后续较小任务越过暂时无法运行的任务。每轮只从 ready 队列和已到期的 retry deadline 中取至多 `scan_budget` 个候选；远期重试任务按截止时间放在独立有序队列中。队列锁只保护队列操作，不跨越策略、store 或 engine 调用。达到可配置的最大越过次数后，调度器优先为被越过的任务留出所需资源，停止启动会继续占用这些资源的后续任务。在运行任务最终退出、资源正确归还的前提下，这避免大任务被持续插队。队列容量、运行并发上限和扫描预算均可配置；队列满时明确拒绝并允许调用方重试，不无限堆积内存。默认受理 worker 数量上限为 64，受理中 payload 总额度为 64 MiB；预留额度随后台 `accept` 完成后释放，即使调用方取消等待也不会提前释放。

### 3.3 状态与查询

公开状态为 `Queued`、`Running`、`Blocked`、`Succeeded`、`Failed`、`Panicked`、`Cancelled`；`Queued` 包含已受理但尚未获资源的任务，`Blocked` 表示需要运维或业务方修复后才能重新入队。受理中的临时状态和终态提交中的内部状态不向业务方承诺。每条 `TaskRecord` 包含 `TaskId`、业务关联、受理/启动/结束时间、尝试次数、当前状态、资源请求及分配结果、失败摘要和单调递增的状态版本。

基本转换为 `Queued -> Running -> Succeeded | Failed | Panicked`，或 `Queued -> Cancelled`。缺少处理器、达到重试上限或自动重试时等待队列已满会进入 `Blocked`；容量原因消失后可显式重新入队，或由业务方取消。运行中收到取消请求时先记录 `cancel_requested`，通过 `TaskContext` 协作通知处理器；`cancel_requested` 只表示发起了请求。只有处理器实际退出并返回 `TaskRunOutcome::Cancelled` 才进入 `Cancelled`；返回成功或失败时保留该业务结果。`LocalTaskHandle::result()` 等待权威终态写入后，才返回类型化结果、业务错误或明确的取消错误。`max_attempts` 是同一 TaskId 跨进程启动的总次数；恢复时达到上限的 Queued/Running 任务转为 Blocked，人工重试也不能重置预算。`test_shutdown_keeps_scheduler_running_for_retry_after_close` 用信号控制首次执行并验证关闭受理后的自动重试，调度器仅在关闭协调器确认队列和运行任务均为空后退出。

提供按 `TaskId` 和幂等键查询、按状态与业务关联键分页列举、查询任务计数及资源快照、等待单个任务终态的接口。`stats()` 通过一次 `TaskStore::count_states()` 聚合查询得到所有保留状态计数，再读取执行引擎资源快照；二者相邻读取但不是同一事务中的原子快照。历史页按 `(accepted_at_ms ASC, id ASC)` 排序，以复合游标稳定处理同毫秒受理的记录；`TaskQuery.limit` 最大为 256，0 按 1 处理，服务和内置 store 执行相同校验。内存 store 用最多 `limit+1` 个排序键选页，再按 ID 克隆结果，额外选择空间为 O(limit)；游标不提供并发写入或清理期间的全局快照。存储统计失败向调用方传播，查询成本不随历史页数增长。`get` 与按键查询对不存在或已清理的记录返回 `None`，存储错误单独返回。`correlation_key` 仅供过滤与业务关联。内存存储限制终态历史数量且默认最多保留 64 MiB payload，并最多保留 2048 条非终态记录（包括 `Blocked`）；空间不足时先按终态完成顺序淘汰终态记录及幂等映射，仍不足则返回容量错误，非终态记录不得淘汰。可用 `with_limits` 与 builder 预设配置额度。SQLite 历史默认不自动清理，显式有界清理只删除早于受理时间阈值的终态记录，并同步移除其幂等键。

## 4. 服务接口与职责划分

业务应用是装配入口：通过 `rs-spi` 发现 provider，按部署配置选择并创建组件，再注入统一的 `TaskExecutionService`。`rs-spi` 提供注册、发现、选择和创建能力；组件之间的依赖由应用明确接线，服务构建器负责检查组合是否合法。这与 IoC 的装配思路相近，但不把 `rs-spi` 假定为会自动推断依赖图的完整容器。

```text
业务应用 / 装配入口
  ├─ rs-spi -> TaskStore provider ──────────────┐
  ├─ rs-spi -> SchedulingPolicy provider ───────┤
  ├─ rs-spi -> TaskExecutionEngine provider ────┤
  ├─ rs-spi -> TaskHandler providers ────────────┤
  └─ rs-event-bus -> 具体 EventBus ───────────────┤
                                                ↓
                                    TaskExecutionService
                                    ├─ TaskCoordinator
                                    └─ TaskScheduler
```

| 模块 | 职责 | 扩展方式 |
| --- | --- | --- |
| `TaskExecutionService` | 对业务暴露提交、查询、取消、等待及能力查询 | 唯一公共门面，不按存储能力拆成多种服务类型 |
| `TaskCoordinator` | 受理与状态转换、幂等检查、生命周期、关闭和故障协调 | 服务内部固定逻辑，避免第三方绕过状态不变量 |
| `TaskScheduler` | 管理待执行任务并驱动调度循环 | 内部固定逻辑；候选任务的排序/公平性由 `SchedulingPolicy` 接口决定 |
| `TaskStore` | 权威任务记录、查询和能力声明；需要时提供持久受理与恢复操作 | 接口；库内提供基本内存实现，第三方可通过 SPI 提供数据库或文件等实现 |
| `TaskExecutionEngine` | 检查本执行域的资源容量，原子预约资源，启动处理器，归还资源并报告执行结果 | 接口；库内提供本机实现，未来可扩展分布式实现 |
| `TaskHandler` 与注册表 | 按任务类型和版本执行具体业务任务 | 处理器接口通过 SPI 发现多个实现；注册表负责唯一性校验和索引 |
| `rs-event-bus::EventBus` | 可选任务状态通知 | 直接使用该库提供的抽象，不定义 `TaskEventSink` |

以下签名表达设计边界，不是已存在的可编译 API：

```text
TaskExecutionService
  capabilities() -> ServiceCapabilities
  submit(TaskRequest with stable idempotency_key) -> TaskRecord
  submit_local(local_task) -> LocalTaskHandle<R, E> | UnsupportedCapability
  get(TaskId) -> TaskRecord | NotFound | Error
  list(TaskQuery) -> Page<TaskRecord>
  cancel(TaskId) -> CancelOutcome
  retry_blocked(TaskId) -> RetryOutcome
  stats() -> TaskStats
  wait(TaskId) -> TaskRecord | Blocked | Error
  shutdown() -> Result
  shutdown_until(deadline) -> Result
```

`TaskExecutionService` 是一个门面类型，而不是为不同存储能力实现多套公共 trait。构建时由装配的 `TaskStore` 决定是否启动恢复流程，并可配置 `require_recovery`：要求恢复但所选存储不支持时，构建失败。`capabilities()` 返回存储能力及本地闭包支持情况。`submit_local` 返回 `LocalTaskHandle<R, E>`，可等待类型化业务值/错误或取消、阻塞、存储故障等明确终态结果；稳定 ID 仍可通过 `task_id()` 获取，`TaskRecord.output` 只用于查询持久摘要。若存储声明重启恢复，则返回 `UnsupportedCapability`，防止不可重建闭包被当作可恢复任务。提交返回 `Ok` 只代表受理：不可恢复存储已保存于本机队列；可恢复存储已完成持久化受理。它不代表任务已启动或成功。

如果调用方取消等待或超时等待 `submit_local`，后台受理仍可能完成，但调用方会失去返回句柄，无法找回原始类型化结果。需要在请求停止等待后继续定位任务时，应使用带稳定幂等键的 `submit`。

`TaskScheduler` 使用 `SchedulingPolicy` 从待执行任务中选择候选项，再向 `TaskExecutionEngine` 请求原子分配和启动。资源账本归执行引擎所有，避免调度器与执行器对剩余资源有不同认识。本期 `LocalTaskExecutionEngine` 在服务所在机器执行；今后替换为分布式实现时，提交与查询模型不必重写。`TaskStore` 是状态依据；不得由协调器或执行引擎另建一套相互竞争的权威状态。

执行引擎必须把处理器的返回错误、panic 和基础设施启动失败区分开。启动失败时释放资源、记录可诊断原因，并按明确的有限重试策略重新排队或标记失败；不能让任务永久占有资源。业务返回错误默认是终态 `Failed`，本期不自动重试，避免无意重复副作用。处理器可以主动返回可重试的基础设施错误。自动重试按 1 秒起步、指数翻倍、最高 60 秒执行，可由 `RetryPolicy` 配置；`retry_not_before_ms` 与 `Queued` 状态原子持久化，到期前调度器不启动任务，恢复会保留到期时间。`ExecutionOutcome` 显式区分业务返回、panic 与 worker 停止，panic 不再依赖错误类别字符串。SQLite 使用 `PRAGMA user_version` 管理 schema；schema 2 将不可变 `request_json` 与仅含生命周期字段的 `lifecycle_json` 分列，状态转换只更新状态索引列和生命周期 JSON。schema 0/1 在单个事务内逐行验证并迁移，损坏记录会回滚整个迁移；未知 schema 或记录格式拒绝打开或读取。

### 4.1 使用 rs-spi 发现和装配扩展模块

`qubit-task` 引入 `qubit-spi`，为 `TaskHandler`、`TaskExecutionEngine`、`SchedulingPolicy` 和 `TaskStore` 定义服务族。扩展 crate 提供带稳定 provider ID 和元数据的工厂；应用可以显式注册，也可以启用 `rs-spi` 的 `inventory` 功能，从已链接的扩展 crate 自动发现 provider。库内基本实现也以 provider 形式提供，同时允许直接构造。应用负责把需要的 crate 链接进最终程序，并在启动配置中选择 provider；发现机制不负责运行时加载动态库，也不替应用决定存储位置、凭证或资源配额。

处理器和基础设施组件的选择方式不同：

- 处理器允许多个 provider 同时存在。`qubit-task` 从发现结果及显式注册项建立 `(task_type, handler_version) -> provider` 映射；同一键由两个 provider 声明时启动失败，错误列出两个注册来源。`rs-spi` 负责发现和 provider ID 冲突检查，任务类型及版本的唯一性由 `qubit-task` 校验。恢复扫描前完成映射构建和所需处理器初始化。
- 存储、执行引擎和调度策略各选择一个 provider。应用使用 `rs-spi` 的具名选择并传入运行时配置；数据库连接、文件路径和资源配额在创建时注入，不放入链接期静态注册项。同一服务族的 provider 共用 `ServiceSpec::Config` 类型，因此每个服务族有配置封套，携带 provider ID 和具体配置对象；provider 校验其类型与内容，并返回明确的配置错误。测试或已有业务对象仍可直接注入。
- 应用选择 `TaskStore` provider 后，查询其 `StoreCapabilities`，并在需要重启恢复时设置 `require_recovery`。存储缺失、初始化失败、能力不满足或不符合其能力契约时构建失败，不静默替换为内存实现。应用也可查询服务装配完成后的有效能力。

`rs-spi` 的 registry 可以在应用装配阶段修改；服务启动后将选择结果固定为本服务实例使用的组件，不在任务运行期间悄悄切换 provider。处理器升级采用新 `handler_version`，旧版本需要保留到相应未完成任务处理完或经过显式迁移；不能只因发现了新版就用它解码旧 payload。链接期发现作为可选 feature 提供，基础 SPI 注册与显式注入不依赖 `inventory`，以保持最小构建可用。事件总线实例由应用创建并直接注入；如应用希望用 SPI 选择事件总线实现，可在应用的装配层完成，不在 `qubit-task` 中增设通知接口。

库内至少提供 `MemoryTaskStore`、默认公平调度策略、`LocalTaskExecutionEngine` 和便于包装本地函数的处理器适配器，并将这些实现注册为可发现的 provider。为使恢复能力可以直接使用，另提供一个可选的本地 SQLite `TaskStore` provider，满足 `restart_recovery` 契约；第三方仍可提供其他 SQL、Redis、MongoDB 或文件系统实现。具体数据库依赖通过可选 feature 隔离，不强加给只用内存服务的应用。

### 4.2 无需 SPI 装配的默认配置

直接使用不应要求业务应用先建立 SPI registry，但入口名称必须明确揭示存储和恢复语义。不提供无参数的 `TaskExecutionService::new()` 或隐式选择内存存储的 `Default`；库内提供以下具名入口，它们使用与 SPI provider 相同的基本组件，不形成另一套执行逻辑：

| 入口 | 默认装配 | 适用场景 |
| --- | --- | --- |
| `TaskExecutionService::in_memory()` | `MemoryTaskStore`、默认公平调度策略、`LocalTaskExecutionEngine`；终态历史 1024 条、非终态记录默认 2048 条，不启用事件总线 | 明确接受进程重启丢失未完成任务 |
| `TaskExecutionServiceBuilder::in_memory()` | 在内存预设上覆盖资源、容量、策略、存储以外的组件和处理器 | 局部定制内存服务，无需使用 SPI |
| `TaskExecutionServiceBuilder::in_memory_with_payload_budget(limit)` | 设置内存存储的常驻 payload 字节上限 | 调整内存保留预算 |
| `TaskExecutionServiceBuilder::recoverable_sqlite(path)` | 可选 SQLite `TaskStore`、默认调度策略、本机执行引擎；强制要求恢复能力 | 单节点重启恢复；构建前须注册稳定的处理器 |
| `TaskExecutionServiceBuilder::from_components(store, engine, policy)` | 由应用传入直接创建或经 SPI 解析的组件 | 自定义装配，不隐式补入内存存储 |

默认 CPU 并发槽位取 `available_parallelism()`，无法取得时使用 1；独立的最大运行任务数默认取相同并行度，最低为 1，零 CPU 资源请求仍消耗一个运行名额。每个任务默认请求 1 个 CPU 槽位。默认队列最多容纳 1024 个等待任务，内存终态历史保留最近 1024 条；默认不自动发现 GPU、不给任何 GPU 额度，GPU 任务需要显式配置设备。默认调度策略按提交顺序扫描，并设置有界越过次数防止大任务长期饥饿。恢复扫描先核算未完成记录数，再分页恢复；上限为 `queue_capacity + max_running_tasks`，超限或存储页无效会使构建失败并保留历史。上述容量均可通过 builder 覆盖。无事件总线时查询和等待接口仍完整可用。

便捷入口不会触发全局 SPI 自动选择，不会因为链接了某个第三方 provider 就改变行为。`in_memory()` 可直接用于 `submit_local`；使用 `TaskRequest` 前仍须提供相应处理器。应用需要自定义组件时，可以直接传入实例，也可以从 `rs-spi` registry 解析后装配。通用 builder 在没有显式选择存储或具名预设时必须拒绝构建。`recoverable_sqlite` 只在启用相应 feature 时存在，打开失败或恢复能力检查失败会返回构建错误，不回退到内存。构建返回前必须完成必要的存储初始化与恢复准备；如果恢复扫描是异步的，构造方法也应是异步的，不能返回一个尚未准备好接收任务的服务。

### 4.3 可替换组件的最小契约

所有需要由 `rs-spi` 创建的组件接口都应支持作为 `Arc<dyn ...>` 注入。下面是职责和操作语义；具体 Rust 方法签名在实施计划中确定。

| 接口 | 必要操作 | 不负责的事 |
| --- | --- | --- |
| `TaskStore` | 报告能力；原子受理或返回已存在的幂等任务；按版本条件转换状态；查询与分页；单次聚合统计保留状态；按能力恢复未完成任务 | 启动处理器、判断本机 GPU 是否空闲 |
| `SchedulingPolicy` | 根据待执行任务的有界快照、资源快照及等待信息，返回候选任务顺序 | 更改权威状态、预约资源、执行用户代码 |
| `TaskExecutionEngine` | 报告可用容量；尝试完整预约任务资源；启动一次任务尝试；报告退出并释放预约 | 决定任务状态、持久化历史、向业务发布事件 |
| `TaskHandler` | 声明稳定的任务类型与版本；校验和解码自己的 payload；使用 `TaskContext` 执行业务逻辑 | 修改队列或服务级状态 |

`TaskStore` 的内存队列索引可由 `TaskScheduler` 缓存，以便高效选择候选任务；索引须由已受理记录构建并在恢复时重建，不能成为第二套权威状态。`SchedulingPolicy` 只选择候选任务，实际能否启动由执行引擎的原子预约结果决定；策略实现不能通过直接写存储绕过协调器。执行引擎返回的结果要区分资源暂不足、永久不满足、启动失败、处理器失败、panic 和取消，以便协调器做正确的状态转换。

为避免资源预约与状态写入之间启动用户代码，`TaskExecutionEngine` 使用两阶段执行交接：先 `prepare` 取得一份有界期的 `PreparedExecution`，完成资源预约但不调用处理器；协调器随后把该尝试的 `Running` 状态写入 `TaskStore`；写入成功才 `activate`，写入失败则 `abort` 并归还资源。`PreparedExecution` 在未激活时被丢弃，也必须释放预约。`Running` 表示该尝试已获得执行资源并进入启动流程，不承诺处理器第一行代码已经运行。`activate` 若失败，协调器以相同尝试代际将任务重新排队或标记失败，不能让它永久留在 `Running`。

### 4.4 装配与启动顺序

1. 应用选择具名预设，或利用 `rs-spi` 发现并创建 `TaskStore`、`SchedulingPolicy`、`TaskExecutionEngine` 和处理器，再按需直接注入 `rs-event-bus` 实例。
2. 服务构建器检查组件和配置，读取 `StoreCapabilities`，验证 `require_recovery`、资源容量、队列上限，以及处理器类型与版本的唯一性。第三方 provider 的初始化错误按原组件和 provider ID 返回，不自动替换实现。
3. 若存储支持重启恢复，先取得独占所有权，再分页装载未完成任务。遗留 `Running` 记录转为待调度状态；缺少精确版本处理器的任务进入可查询的 `Blocked`，其余任务重建待执行队列。payload 解码由处理器负责，当前没有单独的预检钩子。
4. 启动调度循环，确认组件已可接收任务后才向应用返回服务。任一步失败都释放已取得的存储所有权及本机资源，不返回半启动的服务。

服务运行中不热切换存储、执行引擎或策略。应用若需改变这些组件，应有序关闭旧服务，再以新配置创建服务；持久化任务的重接管遵守存储所有权和恢复契约。

## 5. 存储能力分层与恢复

`TaskStore` 是唯一的任务状态和历史读写接口，不再分成 `HistoryStore` 与 `DurableTaskStore` 两个对外抽象。`TaskStore::capabilities()` 返回可扩展的 `StoreCapabilities`，至少包含 `persistent_history` 与 `restart_recovery` 两项；后者要求已受理任务及必要状态具备持久性。能力在服务启动时确定，运行期间不能悄悄改变。服务的 `capabilities()` 汇总存储能力和装配配置；它说明服务支持哪种行为，不保证每个历史任务都有仍可用的处理器。

存储契约包含受理、条件状态转换、按 ID 查询、分页列举、保留策略，以及恢复相关操作。没有恢复能力的实现可对恢复操作返回 `UnsupportedCapability`；服务仅在能力声明支持时调用，构建时检查能力声明与配置，具体行为由契约测试和运行时错误保证。`PersistentHistory` 类型的实现可以在内存中保持活跃队列，并将终态写入外部存储；历史写入失败不能改写已经发生的业务结果，但必须在健康状态中暴露，并在历史查询依赖该存储时返回错误，不能伪装为 `NotFound`。

`count_states()` 是 `TaskStore` 的必需操作：在一个 store 一致性边界内聚合所有当前保留记录，分别返回 `Queued`、`Running`、`Blocked` 与终态数量；已淘汰的终态不计入。实现不应通过多页 `list()` 逐条计数。SQLite 使用单个分组聚合查询，内存实现遍历其受保护的记录集合。第三方 provider 必须实现此方法并随 API 破坏性升级编译迁移。

当 `restart_recovery = true` 时，同一个 `TaskStore` 必须额外满足以下原子语义：

1. 受理时写入任务描述、初始状态和去重键，成功提交后才能向调用方返回 `Ok`。
2. 以任务版本和当前服务所有权代际为条件转换状态，拒绝过期执行回调。
3. 启动时取得该队列的独占所有权，扫描未完成任务并进行恢复；同一队列不能被两个服务实例同时正常调度。
4. 能分页读取记录并实施配置的保留策略，不能删除未完成任务。

存储后端可以使用事务、条件写、日志加锁等方式满足契约；仅暴露普通 `save/get` 的后端不能声明重启恢复能力。独占所有权必须能阻止旧实例继续提交状态变更；服务失去所有权时停止受理和启动新任务。在无法证明旧实例已经退出或被隔离时，新实例不得自动接管。即使状态写入有代际保护，已经运行的旧处理器仍可能继续产生外部副作用，不能将存储代际误称为业务层的恰好一次保证。单节点本期只承诺服务进程重启后的恢复，不承诺多个节点同时竞争任务。具体 SQL、Redis、MongoDB 或文件后端可由业务系统通过 SPI 提供；库内用能力对应的契约测试验证实现，不因某个 provider 自称支持恢复就直接信任它。

恢复规则：`Queued` 任务重新入队；遗留的 `Running` 任务重新入队等待下一次尝试。由于进程可能在业务副作用发生之后、终态持久化之前退出，可恢复模式只能保证**至少一次执行**，不能保证恰好一次。业务处理器应使用 `TaskId` 或自身业务键做幂等。超过恢复/重试次数上限、处理器版本缺失或任务描述无法解码时，记录进入 `Blocked` 并保留诊断信息，不自动启动，也不当作正常成功。优雅关闭先关闭新受理，再等待已进入受理流程的提交完成持久化或失败，然后等待已受理任务退出；超时退出后的恢复仍按上述规则处理。

可恢复存储操作失败时，服务停止新任务受理和后续调度，并通过 `last_store_error()` 暴露诊断。若执行已结束但终态没有提交，持久记录仍为 `Running`，服务不对外报告成功；进程重启后该任务可能再次执行。当前实现不在同一进程内重试失败的状态写入，也不对 SQLite 记录实施自动历史清理。

## 6. 事件通知

启用 `event-bus` feature 后，应用可以向服务注入 `rs-event-bus` 提供的 `EventBus` 门面。服务向 `task.lifecycle` 主题发布 `TaskEvent`，事件包含 `TaskId`、状态版本、状态和业务关联键，不携带大 payload；不配置事件总线时仍可使用查询接口。这里不另设事件发布 trait、适配器或 SPI 服务族。

当前 Cargo 配置依赖 `qubit-event-bus` 0.13。通用 `NotificationPublisher` 返回 provider receipt；本服务按 `AdmissionOutcome` 映射到原有业务统计字段。

服务使用 `rs-event-bus` 的 `NotificationPublisher` 维护有界串行队列，默认容量为 256，可用 `TaskExecutionServiceBuilder::event_bus_buffer_capacity(NonZeroUsize)` 配置。任务状态转移只尝试非阻塞入队，不等待同步 provider；队列满时丢弃新通知。队列关闭后的入队尝试也会丢弃。通知失败不会回滚已提交的任务状态，通知可能丢失、重复或延迟。消费者按 `TaskId` 和状态版本去重，再查询服务取得权威状态。不同并发状态转移按实际入队顺序串行发布，不保证跨生产者按 `state_version` 全局排序。

`TaskExecutionService::notification_stats()` 在配置总线时返回统计快照，未配置时返回 `None`。`enqueued` 统计进入本地队列的事件，`queue_full` 与 `queue_closed` 统计对应的丢弃；`accepted` 表示至少一个已报告目的地接受，`partial_rejection` 表示同一事件同时有接受和拒绝目的地，`opaque_accepted` 表示 provider 接受但未暴露目的地，`unaccepted` 表示没有可见目的地接受（含空列表和 interceptor drop），`publish_error` 记录发布错误，`worker_panicked` 记录线程 panic。计数为单调饱和值；它们只描述本地排队、provider 的接纳回执和 worker 状态，不代表 subscriber handler 已完成。

`TaskExecutionServiceBuilder::runtime_handle` 可指定服务自有 admission、scheduler、completion、shutdown 和发布器关闭等待使用的 Tokio runtime；默认使用进程级 runtime。调用方须保证注入 runtime 存活到关闭协调器完成。`shutdown()` 等待最终关闭结果；`shutdown_until(deadline)` 先启动或复用同一协调器，再限制当前调用者的等待时间。到期返回 `ShutdownTimedOut` 不会取消任务、释放存储所有权或终止事件发布器；后续 `shutdown()` 可继续等待共享结果。关闭在任务工作收敛并释放存储所有权后关闭通知入队，等待 worker 处理完已入队事件再返回；不会关闭应用注入的 `EventBus`。直接丢弃服务时，发送端关闭后 worker 也会自然排空队列。worker panic 会记入统计并通知 shutdown worker 已结束；panic 时剩余队列事件可能丢失。发布调用在独立操作系统线程中执行，避免占用 Tokio runtime worker，但同步 provider 若一直阻塞，显式 shutdown 仍可能无限等待。可靠跨进程投递仍需持久化后端增加事务性 outbox，本期通知不提供 outbox、重试或最终处理保证。

## 7. 关键操作顺序与不变量

```text
提交：验证描述和资源 -> 检查容量/去重 -> 写入权威存储 -> 返回 TaskRecord 或 LocalTaskHandle -> 唤醒调度
启动：选择候选任务 -> 执行引擎 prepare 并预约全部资源 -> TaskStore 提交 Running -> 执行引擎 activate -> 调用处理器
结束：取得处理器结果 -> 提交终态 -> 释放资源 -> 唤醒调度 -> 锁外通知
取消：已排队则原子移出并提交 Cancelled；运行中则记录请求并通知处理器，处理器返回 Cancelled 才确认
恢复：取得独占所有权 -> 检查处理器版本 -> 装载未完成记录 -> 重建队列 -> 开始调度
```

实现时须特别处理“执行引擎接受任务”与“`Running` 持久化”之间的竞态：处理器不得早于 `Running` 的成功提交开始，提交失败则回滚尚未启动的工作并释放资源。所有状态转换及资源账本更新应具有清晰的线性化点；不在状态锁内执行用户代码、外部存储调用或事件发布。任务回调带 `TaskId`、尝试序号和所有权代际，旧回调不能覆盖新尝试的状态。任何任务终止路径，包括 panic、取消与执行引擎拒绝，都必须归还已预约资源。查询允许看到尚未发布事件的已提交状态。

## 8. 查询、写入与记录边界

历史状态筛选使用 `TaskStateKind`，不携带或比较 `Failed`、`Blocked` 等状态中的诊断载荷；该公开类型变更要求调用方将 `TaskQuery.states` 从 `TaskState` 迁移为 `TaskStateKind`。关闭开始后服务写操作通过 admission gate 拒绝；关闭会等待已取得 permit 的写操作完成。SQLite 的 `accept` 与 `transition` 还要求当前 store 持有匹配所有权 epoch，释放所有权后旧句柄无法写入。所有权状态与连接操作按“连接锁后所有权锁”的顺序串行化。

SQLite 使用单个连接，因此同时运行的阻塞数据库操作上限为 1。异步 store 调用先取得 Tokio semaphore permit，再通过 `spawn_blocking` 执行同步 SQLite 工作；permit 由阻塞闭包持有到操作完成，即使调用方取消等待中的 future，也不会释放正在执行操作的容量。轮询 SQLite store future 需要 Tokio runtime。

请求及诊断文本限额按 UTF-8 字节计算：`task_type` 128、`handler_version` 64、`correlation_key` 与 `idempotency_key` 各 256；metadata 最多 32 项，键 128、值 4096、键值总计 16384。超限请求在持久化受理前返回 `InvalidRequest`，Memory 和 SQLite store 也执行相同的请求边界检查。诊断类别最多 128 字节，Blocked 原因、Panicked 消息及其他诊断最多 4096 字节。执行阶段的诊断在 UTF-8 字符边界裁剪；`LocalTaskHandle` 的类型化错误通道仍传递原始值。既有单 payload 16 MiB 与 output summary 64 KiB 上限保持不变；受理中另有默认 64 MiB 总 payload 和 64 worker 数量预算，内存存储默认常驻 payload 上限也为 64 MiB。这些预算不构成进程总内存严格上界。

## 9. 验证与迁移

核心验证包括：资源不足排队、GPU 设备分配、非法资源请求、队列满拒绝、越过次数后的防饥饿、取消与启动竞态、panic 后资源归还、重复提交、历史存储故障、事件故障、持久受理失败、终态写入失败、重启恢复，以及旧实例回调被版本/代际拒绝。还要验证 `in_memory()` 无 SPI 装配可执行本地任务、通用 builder 未选存储时拒绝构建、默认容量可覆盖、SQLite 便捷入口完成恢复后才返回，以及链接第三方 provider 不改变默认行为。SPI 场景要验证跨 crate 自动发现、未链接 provider 不会被发现、重复处理器键报错、运行时配置注入、`StoreCapabilities` 与实际操作一致、`require_recovery` 失败而不降级，以及旧处理器版本恢复。`TaskStore` 提供按能力分组的可复用契约测试套件，让外部后端检验原子受理、条件更新、恢复扫描、独占所有权和单次状态聚合；`stats()` 在服务层由拒绝 `list()` 的测试存储验证只执行一次 `count_states()`。

实施分三步：先交付统一门面、内存 `TaskStore`、默认调度策略、本机执行引擎和 SPI 服务族；再完成可恢复 `TaskStore`、SQLite provider 和重启场景；最后直接接入可选的 `rs-event-bus` 事件通知。新版将破坏旧公开 API：使用 `in_memory()` 代替含糊的无参数构造，使用版本化 `TaskRequest` 或只适用于本地闭包的 `LocalTaskHandle<R, E>`，并用服务生成且不复用的 `TaskId`。`submit_local` 不再返回旧的通用 `TaskHandle<R, E>`；第三方 `TaskStore` 还必须实现 `count_states()`，以一次查询返回所有保留状态的计数。迁移调用代码、provider 实现和测试，不要求保留兼容层。检查当前 `rust-common` 工作区与相关 `rs-*` 仓库后，没有发现直接依赖 `rs-task` 的实际下游，因此当前没有需要同步迁移的兄弟 crate。
