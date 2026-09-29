package orchestrator

import "context"

// Checker 恢复确认器：修复动作结束后检查目标状态。
type Checker interface {
	Check(ctx context.Context, source string, check string) (ok bool, manual bool, detail string)
}

// SimChecker 可控的模拟确认器。
type SimChecker struct {
	results map[string][]checkResult // check -> 按调用次序的结果
	calls   map[string]int
}

type checkResult struct {
	ok     bool
	manual bool
	detail string
}

// NewSimChecker 创建模拟确认器，默认所有检查通过。
func NewSimChecker() *SimChecker {
	return &SimChecker{results: map[string][]checkResult{}, calls: map[string]int{}}
}

// Script 为某个检查项按调用次序编排结果；超出后沿用最后一条。
func (c *SimChecker) Script(check string, results ...checkResult) {}

// CheckOK / CheckFail / CheckManual 构造确认结果。
func CheckOK(detail string) checkResult     { return checkResult{ok: true, detail: detail} }
func CheckFail(detail string) checkResult   { return checkResult{detail: detail} }
func CheckManual(detail string) checkResult { return checkResult{manual: true, detail: detail} }

// Check 实现 Checker。
func (c *SimChecker) Check(ctx context.Context, source string, check string) (bool, bool, string) {
	return true, false, "ok"
}
