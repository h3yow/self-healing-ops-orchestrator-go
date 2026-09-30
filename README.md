# self-healing-ops-orchestrator-go

本地运维自愈编排器：用本地状态存储（`MemoryStore`）与可控模拟执行器（`SimExecutor`）
代替真实远程节点，演示故障事件在**并发接入、维护窗口、重复投递、超时失败、进程中断**
等场景下如何得到一致、可审计的处理结果。

## 架构

```
FaultEvent ──> Orchestrator.Submit ──准入判定──> executePlan ──> verify ──> RunRecord(终结)
                    │                    │            │
                    └──── 全部状态变化写入 Store 的审计日志（AuditEntry） ────┘
```

| 文件 | 职责 |
|---|---|
| `types.go` | 事件、步骤、判定、审计等核心类型与状态枚举 |
| `orchestrator.go` | 事件准入、去重抑制、步骤编排、恢复确认 |
| `store.go` | 本地状态存储接口与内存实现（并发安全） |
| `executor.go` | 模拟执行器：可按“步骤×尝试次数”编排成功/失败/挂起 |
| `checker.go` | 恢复确认器：判定目标状态是否达成 |
| `maintenance.go` | 维护窗口与修复策略 |

## 事件状态流转

```
received
   ├─ suppressed   抑制窗口内同来源重复事件，归并到已有运行，不重复触发
   ├─ rejected     被拒绝，原因（RejectReason）可区分：
   │                 maintenance_window  处于维护窗口
   │                 policy_not_allowed  无匹配策略或严重级别不足
   │                 duplicate_active    同来源已有进行中的运行
   └─ accepted ──> remediating ──> verifying ──> resolved   恢复确认通过
                                              ├─> failed     步骤失败或确认未达成
                                              └─> manual     无法自动判定，需人工
```

被拒绝/归并的事件同样会落一条 `RunRecord` 与审计条目，拒绝原因可追溯。

## 幂等与重试规则

- **事件去重**：同一 `Source` 在 `SuppressWindow` 内的重复投递被归并；准入判定
  （去重 → 维护窗口 → 策略 → 活跃运行）在同一把锁内原子完成，并发投递下
  保证恰好一个事件被接受。
- **步骤幂等**：`StepRecord.State == succeeded` 的步骤在任何情况下都不会再交给
  执行器（包括重试、并发批次、进程恢复后）。测试用 `SimExecutor.Calls` 断言
  每个已完成步骤的实际执行次数。
- **重试**：单步失败（业务失败 `OK=false` 或执行错误/超时）按 `MaxRetries` 重试，
  每次尝试有独立超时（`Step.Timeout` 或 `Config.DefaultTimeout`）；尝试次数、
  最后一次错误都持久化在 `StepRecord`。
- **依赖**：`DependsOn` 全部成功的步骤才会进入同一并发批次；依赖失败/跳过的
  步骤标记为 `skipped`，不会执行，留下可判断的现场。
- **中断恢复**：进程重启后调用 `Recover`，未终结的运行继续推进；`running`
  状态视为中断残留允许重跑，`succeeded` 一律跳过。

## 维护窗口

`Window{Start, End}` 为半开区间 `[Start, End)`。事件到达时刻落在任一窗口内即拒绝，
原因记为 `maintenance_window`，与 `policy_not_allowed`、`duplicate_active` 明确区分，
且被拒绝的事件不会触发任何修复动作。

## 恢复确认与结果汇总

修复动作结束后进入 `verifying`：

1. 全部步骤成功 → 候选结论 `success`，否则 `failed`；
2. 逐项执行策略中的 `Checks`：任一检查返回 manual → 结论降为 `manual`；
   任一检查不通过 → 结论为 `failed`；
3. **冲突结论不得静默覆盖**：`VerdictSet` 为 true 的运行再次得出不同结论时，
   保留原结论，并在审计日志中记录 `conflict: ...` 条目。

所有状态变化以 `AuditEntry{RunID, EventID, Category, Message}` 追加到 Store，
可关联到原始事件与本次执行；`Category` 区分 `event` / `step` / `verdict`。

## 验证方法

```sh
go test ./...        # 全部测试
go test -race ./...  # 并发安全（竞态检测）
go test -v ./...     # 查看事件、编排步骤与最终判定依据的日志
```

测试覆盖：

| 测试 | 场景 |
|---|---|
| `TestConcurrentDuplicateEvents` | 16 路并发重复事件，恰好 1 次被接受，步骤各执行 1 次 |
| `TestMaintenanceWindowRejection` | 维护窗口拒绝，且与策略拒绝原因可区分 |
| `TestStepTimeoutAndRetry` | 超时 → 失败 → 重试成功，尝试次数符合预期 |
| `TestStepFailsPermanently` | 重试耗尽后失败，依赖步骤被跳过而非执行 |
| `TestRecoverAfterCrash` | 中断恢复：已完成步骤零重复执行，残留步骤继续 |
| `TestVerdictOutcomes` | 确认结果为 成功/失败/需人工 三种判定 |
| `TestVerdictNotOverwritten` | 冲突结论保留原值并留下审计记录 |
