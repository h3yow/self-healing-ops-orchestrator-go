package orchestrator

import (
	"context"
	"sync"
	"time"
)

// ExecRequest 一次步骤执行请求。
type ExecRequest struct {
	RunID  string
	Step   string
	Action string
	Params map[string]string
}

// ExecResult 步骤执行结果。
type ExecResult struct {
	OK     bool
	Output string
	Err    error // 非 nil 表示执行级错误（与 OK=false 的业务失败区分）
}

// Executor 模拟执行器接口：代替真实远程节点。
type Executor interface {
	Execute(ctx context.Context, req ExecRequest) ExecResult
}

// SimExecutor 可控的模拟执行器：可按 步骤名:尝试次数 编排行为。
type SimExecutor struct {
	mu        sync.Mutex
	scripts   map[string][]func(ctx context.Context) ExecResult // key: step name
	calls     map[string]int                                    // step -> 已调用次数
	defaultFn func(ctx context.Context) ExecResult
}

// NewSimExecutor 创建模拟执行器，默认所有动作成功。
func NewSimExecutor() *SimExecutor {
	return &SimExecutor{
		scripts: make(map[string][]func(ctx context.Context) ExecResult),
		calls:   make(map[string]int),
		defaultFn: func(ctx context.Context) ExecResult {
			return ExecResult{OK: true, Output: "ok"}
		},
	}
}

// Script 为某一步骤按尝试次数编排行为；超出脚本长度时沿用最后一条。
func (s *SimExecutor) Script(step string, fns ...func(ctx context.Context) ExecResult) {}

// Calls 返回某步骤被执行器实际调用的次数（用于验证幂等/不重复执行）。
func (s *SimExecutor) Calls(step string) int { return 0 }

// Execute 实现 Executor。
func (s *SimExecutor) Execute(ctx context.Context, req ExecRequest) ExecResult {
	return ExecResult{OK: true}
}

// Fail 构造一个业务失败结果。
func Fail(output string) func(ctx context.Context) ExecResult {
	return func(ctx context.Context) ExecResult { return ExecResult{OK: false, Output: output} }
}

// OK 构造一个成功结果。
func OK(output string) func(ctx context.Context) ExecResult {
	return func(ctx context.Context) ExecResult { return ExecResult{OK: true, Output: output} }
}

// Hang 构造一个直到 ctx 取消才返回的行为（用于超时测试）。
func Hang() func(ctx context.Context) ExecResult {
	return func(ctx context.Context) ExecResult {
		<-ctx.Done()
		return ExecResult{OK: false, Err: ctx.Err()}
	}
}

// Sleep 构造一个耗时 d 后成功的行为。
func Sleep(d time.Duration) func(ctx context.Context) ExecResult {
	return func(ctx context.Context) ExecResult {
		select {
		case <-ctx.Done():
			return ExecResult{OK: false, Err: ctx.Err()}
		case <-time.After(d):
			return ExecResult{OK: true, Output: "slept " + d.String()}
		}
	}
}
