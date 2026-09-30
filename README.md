# Self-Healing Ops Orchestrator (Go)

一套**本地**运维自愈编排能力：用本地持久化状态和可控的模拟执行器代替真实远程节点，
让故障事件在并发接入、维护窗口和重复投递下都得到一致、可审计的处理结果。

## 能力总览

| 需求 | 实现位置 | 说明 |
|------|----------|------|
| 事件接收 / 归并 / 去重 | `internal/event` | 按 `source|fault_type|target` 归并；抑制窗口内重复事件归并到同一事件列表并以明确原因拒绝 |
| 维护窗口 / 策略拒绝 | `internal/event/window.go`、`policy.go` | 一次性窗口、每日重复窗口、窗口链；严重度与故障类型策略；拒绝原因可区分 |
| DAG 编排与状态推进 | `internal/workflow` | 依赖调度、有界并发、步骤重试/超时/取消；每次状态转移先落盘 |
| 进程恢复 | `internal/workflow`（租约锁 + `Resume`） | 崩溃后租约过期方可接管；已 `succeeded` 步骤绝不重跑，`running` 重置为 `pending` |
| 恢复确认 / 结果汇总 | `internal/reconcile` | 探测目标状态，判定 success/failed/manual/indeterminate；冲突结论返回错误而不覆盖 |
| 可审计 | `internal/audit` | 每条记录带 `event_id` + `incident_id` + `exec_id`，JSONL 顺序追加并 fsync |
| 门面装配 | `internal/orchestrator` | `Handle`（接入→编排→确认）、`Recover`（中断续跑） |
| 模拟执行器 | `internal/executor` | 按步骤脚本化输出/失败/延时，记录每次真实调用（用于校验幂等） |

## 事件与状态流转

```
                         维护窗口活跃? ──是──▶ rejected(maintenance_window)
                              │否
   新事件 ──Ingest──▶  策略不满足? ──是──▶ rejected(policy_not_met)
                              │否
                   存在同 source key 的未关闭事件
                   且在抑制窗口内/在途? ──是──▶ 归并事件ID + rejected(suppressed_duplicate)
                              │否
                              ▼
                          active ──开始编排──▶ remediating
                         成功且健康 ▶ resolved(success)
                         执行/目标失败 ▶ failed(failed)
                         冲突信号/探测失败 ▶ failed + manual / indeterminate（人工处理）
```

- **抑制窗口（suppression window）**：事件首次开启与进入终态时设定。窗口内同源重复事件
  会被**归并**（`Incident.EventIDs` 追加）并返回 `suppressed_duplicate`，绝不重复触发修复；
  窗口结束后同一来源可以开启一个新的处理周期。
- **在途保护**：`active` / `remediating` 期间的重复投递同样归并拒绝，避免一次故障被并发修复多次。

### 编排执行状态

步骤：`pending → running → succeeded | failed | skipped | cancelled`
执行：`running → succeeded | failed | cancelled | interrupted`

- 依赖失败/取消时，下游步骤标记为 `skipped`（且从不执行），保留可继续处理的状态。
- 每一步在执行前、完成后都持久化；崩溃发生在"步骤完成落盘之后、下一步之前"。
- 租约锁（`LockToken` + `LockAcquiredAt`）保证同一执行同时只有一个 runner：
  崩溃者的锁在租约内仍有效，过期后新进程才能 `Resume` 接管。

## 幂等与重试规则

1. **事件幂等**：接收器串行化接入 + 存储 CAS；25 个并发重复事件只有 1 个触发编排，
   其余全部归并并返回 `suppressed_duplicate`（见集成测试）。
2. **步骤重试**：`Step.MaxAttempts` 控制最大尝试次数；每次失败记录到审计；
   成功后立即停止。超时走 `context.WithTimeout`，超时计入一次尝试。
3. **恢复幂等**：`Resume` 只驱动非 `succeeded` 步骤；已完成步骤的执行器调用次数在恢复后
   仍然恰好为 1。
4. **结论幂等**：同一执行重复得出**相同**结论直接返回已存记录；得出**不同**结论返回
   `reconcile.ErrConclusionConflict`，原结论不被静默覆盖。

## 维护窗口

- `OneShotWindow{Start, End}`：单个绝对时间区间。
- `DailyWindow{HourStart, MinStart, HourEnd, MinEnd, Location}`：每日重复，支持跨午夜。
- `MaintenanceChain`：任一窗口活跃即拒绝。
- 窗口判断发生在接入最前面，拒绝原因为 `maintenance_window`，与策略拒绝 / 抑制拒绝明确区分。

## 判定规则（reconcile.Judge）

| 执行结果 | 目标探测 | 结论 |
|----------|----------|------|
| 步骤成功 | 健康 | `success` |
| 步骤成功 | 不健康 | `manual`（动作成功但故障仍在，人工确认） |
| 步骤失败 | 健康 | `manual`（信号冲突，不冒称成功） |
| 步骤失败 | 不健康 | `failed` |
| 任意 | 探测出错 | `indeterminate`（无法确认，转人工） |

## 存储

`internal/store` 为每个 key 写一个 JSON 文件（`state/incidents/…`、`state/executions/…`、
`state/conclusions/…`），写采用临时文件 + fsync + 原子 rename；带 `version` 乐观锁，
版本不匹配返回 `store.ErrConflict`。

## 验证方法

```bash
# 全部单元 + 集成测试（含竞态检测）
go test -race ./...

# 只看集成场景（事件、步骤、判定依据都会打印到测试日志）
go test -v . -run TestIntegration

# 端到端演示：成功重试 / 重复抑制 / 崩溃恢复，并打印完整审计轨迹
go run ./cmd/demo
```

### 测试场景与可观测输出

- `TestIntegration_ConcurrentDuplicateEvents`：25 并发重复事件 → 仅 1 次修复，
  每个步骤执行器调用恰好 1 次；打印事件数、触发/抑制计数与审计轨迹。
- `TestIntegration_MaintenanceWindowRejection`：窗口内拒绝原因 `maintenance_window`，
  窗口结束后同来源事件正常修复成功。
- `TestIntegration_FailureRetryThenSuccess` / `TestIntegration_TimeoutThenFailed`：
  瞬时失败重试成功；超时重试耗尽后失败、下游 skipped。
- `TestIntegration_InterruptAndRecovery`：崩溃后新进程 `Recover`，
  已完成步骤零重复执行，最终 `success`，并打印恢复前后步骤状态与审计链。
- 单元测试 `internal/event`、`internal/workflow`、`internal/reconcile`、`internal/executor`
  的 `t.Logf` 会打印具体事件、编排步骤与最终判定依据。

审计日志为 JSONL，每行一条，例如：

```json
{"seq":17,"time":"...","event_id":"evt-c1","exec_id":"exec-...","entity":"step",
 "action":"step_succeeded","to":"cleanup","reason":"freed 40GB"}
```
