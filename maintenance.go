package orchestrator

import "time"

// Window 维护窗口：窗口内拒绝自动修复（需人工变更管控）。
type Window struct {
	Name  string
	Start time.Time
	End   time.Time
}

// Contains 判断 t 是否落在窗口内（含起点，不含终点）。
func (w Window) Contains(t time.Time) bool {
	return !t.Before(w.Start) && t.Before(w.End)
}

// Policy 修复策略：按故障类型匹配。
type Policy struct {
	Kind        string   // 匹配的故障类型
	MinSeverity int      // 低于该级别拒绝自动修复
	Plan        []Step   // 修复编排计划
	Checks      []string // 恢复确认项（交给 Checker 判定）
}

// PolicySet 策略集合。
type PolicySet struct {
	policies map[string]Policy
}

// NewPolicySet 创建策略集合。
func NewPolicySet(ps ...Policy) *PolicySet {
	set := &PolicySet{policies: make(map[string]Policy, len(ps))}
	for _, p := range ps {
		set.policies[p.Kind] = p
	}
	return set
}

// Match 按故障类型查找策略。
func (ps *PolicySet) Match(kind string) (Policy, bool) {
	if ps == nil {
		return Policy{}, false
	}
	p, ok := ps.policies[kind]
	return p, ok
}
