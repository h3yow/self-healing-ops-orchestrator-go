package orchestrator

import (
	"context"
	"sync"
	"time"
)

// Config 编排器配置。
type Config struct {
	SuppressWindow time.Duration // 同一来源重复事件的抑制窗口
	DefaultTimeout time.Duration // 步骤默认超时
	Now            func() time.Time // 可注入时钟，便于测试
}

// Orchestrator 自愈编排器。
type Orchestrator struct {
	cfg      Config
	store    Store
	exec     Executor
	checker  Checker
	policies *PolicySet
	windows  []Window

	mu       sync.Mutex
	lastSeen map[string]time.Time // source -> 最近一次接受事件的时间（抑制窗口）
}

// New 创建编排器。
func New(cfg Config, store Store, exec Executor, checker Checker, policies *PolicySet, windows []Window) *Orchestrator {
	return &Orchestrator{}
}

// SubmitResult 事件投递结果。
type SubmitResult struct {
	Accepted bool
	Status   EventStatus
	Reason   RejectReason
	RunID    string // 被接受或归并到的运行 ID
}

// Submit 接收故障事件：去重、维护窗口与策略校验，通过后启动编排。
func (o *Orchestrator) Submit(ctx context.Context, ev FaultEvent) SubmitResult {
	return SubmitResult{}
}

// Recover 进程重启后恢复：继续推进未终结的运行，已完成步骤不重复执行。
func (o *Orchestrator) Recover(ctx context.Context) error { return nil }

// Run 查询运行记录。
func (o *Orchestrator) Run(runID string) (RunRecord, bool, error) { return RunRecord{}, false, nil }
