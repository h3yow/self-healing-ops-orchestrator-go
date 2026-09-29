// Package orchestrator 提供本地运维自愈编排能力：
// 故障事件接入、去重抑制、维护窗口校验、修复流程编排、
// 恢复确认与结果汇总。所有状态保存在本地，执行器为可控模拟实现。
package orchestrator

import "time"

// EventStatus 故障事件在处理流水线中的状态。
type EventStatus string

const (
	EventReceived    EventStatus = "received"    // 已接收，待评估
	EventSuppressed  EventStatus = "suppressed"  // 抑制窗口内重复事件被归并
	EventRejected    EventStatus = "rejected"    // 被拒绝（维护窗口/策略不满足）
	EventAccepted    EventStatus = "accepted"    // 通过评估，进入编排
	EventRemediating EventStatus = "remediating" // 修复流程执行中
	EventVerifying   EventStatus = "verifying"   // 修复动作完成，恢复确认中
	EventResolved    EventStatus = "resolved"    // 恢复确认通过
	EventFailed      EventStatus = "failed"      // 修复失败
	EventManual      EventStatus = "manual"      // 需要人工介入
)

// RejectReason 区分事件被拒绝的原因。
type RejectReason string

const (
	RejectNone            RejectReason = ""
	RejectMaintenance     RejectReason = "maintenance_window" // 处于维护窗口
	RejectPolicy          RejectReason = "policy_not_allowed" // 策略不允许（如级别过低/来源不受信）
	RejectDuplicateActive RejectReason = "duplicate_active"   // 同一来源已有进行中的修复
)

// FaultEvent 外部投递的故障事件。
type FaultEvent struct {
	ID        string    // 事件唯一 ID（投递方生成）
	Source    string    // 事件来源（节点/服务标识），去重键
	Kind      string    // 故障类型，用于匹配修复策略
	Severity  int       // 严重级别，越大越严重
	Detail    string    // 描述
	EmittedAt time.Time // 事件发生时间
}

// StepState 编排步骤状态。
type StepState string

const (
	StepPending   StepState = "pending"
	StepRunning   StepState = "running"
	StepSucceeded StepState = "succeeded"
	StepFailed    StepState = "failed"
	StepSkipped   StepState = "skipped" // 依赖失败导致跳过
)

// Step 修复流程中的一个步骤。
type Step struct {
	Name       string        // 步骤名（计划内唯一）
	DependsOn  []string      // 依赖的步骤名，全部成功后才可执行
	Timeout    time.Duration // 单步执行超时，0 表示用默认值
	MaxRetries int           // 失败可重试次数
	Action     string        // 交给执行器的动作标识
}

// StepRecord 步骤的一次性持久化状态（恢复后据此跳过已完成步骤）。
type StepRecord struct {
	Name     string
	State    StepState
	Attempts int
	LastErr  string
}

// Verdict 恢复确认后的最终判定。
type Verdict string

const (
	VerdictSuccess Verdict = "success" // 目标状态达成
	VerdictFailed  Verdict = "failed"  // 确认未达成
	VerdictManual  Verdict = "manual"  // 无法自动判定，需人工
)

// AuditEntry 审计日志条目：所有状态变化可追溯。
type AuditEntry struct {
	At       time.Time
	RunID    string // 本次执行（修复运行）ID
	EventID  string // 关联的原始事件 ID
	Category string // event / step / verdict
	Message  string
}
