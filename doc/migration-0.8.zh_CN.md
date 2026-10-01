# qubit-task 0.8 迁移说明

0.8 有意破坏了存储分页与故障清理接口。下游 `TaskStore` 实现和调用方需要一起更新；本版本不保留兼容重载。

## 将恢复游标从任务 ID 改为位置游标

`TaskId` 标识一条任务，无法区分同一毫秒受理的多条历史记录。`TaskCursor` 同时携带受理时间和 ID 次序键。自定义存储应更新签名：

```rust
// 之前
fn scan_unfinished<'a>(&'a self, cursor: Option<TaskId>) -> TaskFuture<'a, Result<StoredTaskPage, StoreError>>;

// 之后
fn scan_unfinished<'a>(&'a self, cursor: Option<TaskCursor>) -> TaskFuture<'a, Result<RecoveryPage, StoreError>>;
```

从 `qubit_task::model` 导入 `TaskCursor` 和 `RecoveryPage`。`RecoveryPage.next` 类型为 `Option<TaskCursor>`；`TaskPage.next` 与 `TaskQuery.after` 也使用同一类型。游标为排他边界，历史按 `(accepted_at_ms ASC, id ASC)` 排序。字段破坏性变更为 `RecoveryPage.next: Option<TaskId>` → `RecoveryPage.next: Option<TaskCursor>`。

将 `page.next` 原样传给下一页，不要只用 ID 拼造游标：

```rust
let mut after = None;
loop {
    let page = store.scan_unfinished(after).await?;
    process(page.tasks).await?;
    after = page.next;
    if after.is_none() {
        break;
    }
}
```

即使最后一页正好有 256 条，`next` 仍为 `None`。恢复页只包含不带 payload 的摘要；实现必须保持严格排序和排他续页语义。

## 将 finalizer panic 当作服务故障处理

attempt finalizer 与 handler future 分开受监督。若状态迁移或记账逻辑 panic，服务会锁存 `StoreUnavailable`、关闭新受理，并主动启动共享 shutdown 协调器。诊断中包含任务和尝试信息，回收进程内 finalizer 计数，并在 shutdown 屏障满足前保留存储所有权。panic 不会伪造成功或终态任务。观察并等待 `shutdown()` 完成，再修复原因或重启。shutdown 尚未完成时，不要让另一个进程打开同一持久库。

## SQLite schema 3 索引

现在打开已有 schema 3 数据库时会确保所需的历史与未完成任务索引。如果已知的局部索引存在旧的同名定义，打开过程会在事务内重建它。schema 与记录格式仍为版本 3；索引修复不会重写任务记录或元数据。和其他存储升级一样，部署前备份数据库，并等待首次打开完成后再接收流量。

## 从独立 crate 验证 typed store API

[独立 typed-store consumer](../tests/fixtures/typed-store-consumer/) 通过公开的 `TaskStore` trait 检查 `accept_encoded`、`get_encoded_task`、start/transition 和 query。它始终验证 memory store；启用 `sqlite` 后还会验证临时目录中的 `SqliteTaskStore::open_next`，并在退出时清理该目录。运行命令：`cargo run --manifest-path tests/fixtures/typed-store-consumer/Cargo.toml --features sqlite,typed-contract`。这是 API smoke test，不能代替 backend 自己的崩溃、写入排空和事务中断测试。

显式运行 `.infra/tools/verify-packaged-consumer.sh` 会验证打包后的 crate 和通过 registry 解析依赖的 consumer，且不使用兄弟仓库路径覆盖。脚本未成功运行时，不得声称包级验证已通过。

严格包检查先审查 `cargo package --list`，再在不使用 `--no-verify` 的情况下执行 Cargo 自带验证：

```sh
cargo package --manifest-path /path/to/rs-task/Cargo.toml \
  --target-dir /tmp/superpowers-rs-task-gc6n4m1u/package-target \
  --locked --allow-dirty --no-default-features --features sqlite
```

`--allow-dirty` 用于包含已审阅的工作区变更，不会关闭 package verification。将 `package/qubit-task-0.8.0.crate` 解到专用且已校验的临时工作区，再创建另一个 consumer manifest 指向该解包目录，manifest 中不得有 `[patch]`。使用独立 `CARGO_HOME`，复制用户的 Cargo registry/认证配置与 credentials，但不要打印秘密；检查其中和当前命令工作目录祖先上的 Cargo config，拒绝路径 patch。构建并运行 consumer 的无默认特性、单特性、组合特性、全特性及合同套件，同时输出解析出的 registry source。registry 解析失败时应报告阻塞；不得换用兄弟 checkout，也不得声称包检查通过。

恢复语义仍是至少一次：进程可能在执行外部业务副作用后、写入任务终态前崩溃，任务之后会再次运行。请使用幂等键，或在应用事务/outbox 中保护副作用；任务存储不提供业务效果恰好一次。
