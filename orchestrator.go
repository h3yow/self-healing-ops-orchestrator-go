package orchestrator

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Config 编排器配置。
type Config struct {
	SuppressWindow time.Duration    // 同一来源重复事件的抑制窗口
	DefaultTimeout time.Duration    // 步骤默认超时
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
	runSeq   int
}

// New 创建编排器。
func New(cfg Config, store Store, exec Executor, checker Checker, policies *PolicySet, windows []Window) *Orchestrator {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.DefaultTimeout <= 0 {
		cfg.DefaultTimeout = 30 * time.Second
	}
	return &Orchestrator{
		cfg:      cfg,
		store:    store,
		exec:     exec,
		checker:  checker,
		policies: policies,
		windows:  windows,
		lastSeen: make(map[string]time.Time),
	}
}

// SubmitResult 事件投递结果。
type SubmitResult struct {
	Accepted bool
	Status   EventStatus
	Reason   RejectReason
	RunID    string // 被接受或归并到的运行 ID
}

// Submit 接收故障事件：去重、维护窗口与策略校验，通过后同步执行编排。
// 准入判定（去重/窗口/策略/活跃运行）在单锁内原子完成，保证并发投递结果一致。
func (o *Orchestrator) Submit(ctx context.Context, ev FaultEvent) SubmitResult {
	now := o.cfg.Now()

	o.mu.Lock()
	// 1. 抑制窗口内同来源重复事件 -> 归并，不重复触发。
	if t, ok := o.lastSeen[ev.Source]; ok && now.Sub(t) < o.cfg.SuppressWindow {
		o.mu.Unlock()
		rec, found, _ := o.store.FindActiveRun(ev.Source)
		runID := ""
		if found {
			runID = rec.RunID
		}
		o.audit("", ev.ID, "event", fmt.Sprintf("suppressed duplicate from %s within %s", ev.Source, o.cfg.SuppressWindow))
		return SubmitResult{Accepted: false, Status: EventSuppressed, Reason: RejectNone, RunID: runID}
	}

	// 2. 维护窗口拒绝。
	for _, w := range o.windows {
		if w.Contains(now) {
			rec := o.newRunRecordLocked(ev)
			rec.EventStatus = EventRejected
			rec.RejectWhy = RejectMaintenance
			_ = o.store.SaveRun(rec)
			o.mu.Unlock()
			o.audit(rec.RunID, ev.ID, "event", fmt.Sprintf("rejected: maintenance window %q", w.Name))
			return SubmitResult{Accepted: false, Status: EventRejected, Reason: RejectMaintenance, RunID: rec.RunID}
		}
	}

	// 3. 策略校验：无匹配策略或级别不足。
	pol, ok := o.policies.Match(ev.Kind)
	if !ok || ev.Severity < pol.MinSeverity {
		rec := o.newRunRecordLocked(ev)
		rec.EventStatus = EventRejected
		rec.RejectWhy = RejectPolicy
		_ = o.store.SaveRun(rec)
		o.mu.Unlock()
		o.audit(rec.RunID, ev.ID, "event", fmt.Sprintf("rejected: policy not allowed (kind=%s severity=%d)", ev.Kind, ev.Severity))
		return SubmitResult{Accepted: false, Status: EventRejected, Reason: RejectPolicy, RunID: rec.RunID}
	}

	// 4. 同一来源已有进行中的运行。
	if rec, found, _ := o.store.FindActiveRun(ev.Source); found {
		o.mu.Unlock()
		o.audit(rec.RunID, ev.ID, "event", "rejected: duplicate active run for source "+ev.Source)
		return SubmitResult{Accepted: false, Status: EventRejected, Reason: RejectDuplicateActive, RunID: rec.RunID}
	}

	// 5. 接受：登记运行记录并占用抑制窗口，然后释放锁再执行编排。
	rec := o.newRunRecordLocked(ev)
	rec.EventStatus = EventAccepted
	for _, st := range pol.Plan {
		rec.Steps = append(rec.Steps, StepRecord{Name: st.Name, State: StepPending})
	}
	_ = o.store.SaveRun(rec)
	o.lastSeen[ev.Source] = now
	o.mu.Unlock()

	o.audit(rec.RunID, ev.ID, "event", fmt.Sprintf("accepted: kind=%s severity=%d", ev.Kind, ev.Severity))
	o.executePlan(ctx, rec, pol)
	return SubmitResult{Accepted: true, Status: EventAccepted, RunID: rec.RunID}
}

// Recover 进程重启后恢复：继续推进未终结的运行，已完成步骤不重复执行。
func (o *Orchestrator) Recover(ctx context.Context) error {
	runs, err := o.store.ListRuns()
	if err != nil {
		return err
	}
	for _, rec := range runs {
		if rec.Terminal() || rec.EventStatus == EventRejected {
			continue
		}
		pol, ok := o.policies.Match(rec.Kind)
		if !ok {
			continue
		}
		o.audit(rec.RunID, rec.EventID, "event", "recover: resume run")
		o.executePlan(ctx, rec, pol)
	}
	return nil
}

// Run 查询运行记录。
func (o *Orchestrator) Run(runID string) (RunRecord, bool, error) {
	return o.store.GetRun(runID)
}

// executePlan 按依赖关系推进步骤，完成后进入恢复确认。
func (o *Orchestrator) executePlan(ctx context.Context, rec RunRecord, pol Policy) {
	rec.EventStatus = EventRemediating
	_ = o.store.SaveRun(rec)

	for {
		// 先在无并发的情况下扫描出本批次：可执行步骤与需跳过的步骤。
		type runnable struct {
			st  *StepRecord
			def Step
		}
		var batch []runnable
		progress := false
		for i := range rec.Steps {
			st := &rec.Steps[i]
			// pending 待执行；running 视为上次进程中断的残留，允许重跑。
			if st.State != StepPending && st.State != StepRunning {
				continue
			}
			stepDef := findStep(pol.Plan, st.Name)
			if stepDef == nil {
				st.State = StepFailed
				st.LastErr = "step definition missing"
				progress = true
				continue
			}
			switch depsState(rec.Steps, stepDef.DependsOn) {
			case StepFailed, StepSkipped:
				st.State = StepSkipped
				st.LastErr = "dependency not satisfied"
				o.audit(rec.RunID, rec.EventID, "step", fmt.Sprintf("step %s skipped: dependency not satisfied", st.Name))
				progress = true
			case StepSucceeded:
				batch = append(batch, runnable{st: st, def: *stepDef})
				progress = true
			}
		}
		// 同批次步骤并发执行；写回发生在 wg.Wait 之后，避免读写竞争。
		var wg sync.WaitGroup
		for _, r := range batch {
			wg.Add(1)
			go func(r runnable) {
				defer wg.Done()
				o.runStep(ctx, &rec, r.st, r.def)
			}(r)
		}
		wg.Wait()
		_ = o.store.SaveRun(rec)
		if !progress {
			break
		}
	}

	o.verify(ctx, &rec, pol)
	_ = o.store.SaveRun(rec)
}

// runStep 带重试与超时执行单步；已成功的步骤直接跳过（幂等）。
func (o *Orchestrator) runStep(ctx context.Context, rec *RunRecord, st *StepRecord, def Step) {
	if st.State == StepSucceeded {
		return
	}
	timeout := def.Timeout
	if timeout <= 0 {
		timeout = o.cfg.DefaultTimeout
	}
	st.State = StepRunning
	maxAttempts := def.MaxRetries + 1
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		st.Attempts++
		o.audit(rec.RunID, rec.EventID, "step", fmt.Sprintf("step %s attempt %d/%d (timeout %s)", st.Name, attempt, maxAttempts, timeout))
		actx, cancel := context.WithTimeout(ctx, timeout)
		res := o.exec.Execute(actx, ExecRequest{RunID: rec.RunID, Step: st.Name, Action: def.Action})
		cancel()
		if res.OK {
			st.State = StepSucceeded
			st.LastErr = ""
			o.audit(rec.RunID, rec.EventID, "step", fmt.Sprintf("step %s succeeded: %s", st.Name, res.Output))
			return
		}
		if res.Err != nil {
			st.LastErr = res.Err.Error()
		} else {
			st.LastErr = res.Output
		}
		o.audit(rec.RunID, rec.EventID, "step", fmt.Sprintf("step %s attempt %d failed: %s", st.Name, attempt, st.LastErr))
	}
	st.State = StepFailed
}

// verify 恢复确认：根据目标状态给出最终判定，结论只允许设置一次。
func (o *Orchestrator) verify(ctx context.Context, rec *RunRecord, pol Policy) {
	rec.EventStatus = EventVerifying
	_ = o.store.SaveRun(*rec)

	stepsOK := true
	for _, st := range rec.Steps {
		if st.State != StepSucceeded {
			stepsOK = false
		}
	}

	verdict := VerdictSuccess
	basis := "all steps succeeded"
	if !stepsOK {
		verdict = VerdictFailed
		basis = "one or more steps failed or skipped"
	}
	for _, chk := range pol.Checks {
		ok, manual, detail := o.checker.Check(ctx, rec.Source, chk)
		o.audit(rec.RunID, rec.EventID, "verdict", fmt.Sprintf("check %s -> ok=%v manual=%v (%s)", chk, ok, manual, detail))
		switch {
		case manual:
			if verdict != VerdictFailed {
				verdict = VerdictManual
				basis = fmt.Sprintf("check %s requires manual decision: %s", chk, detail)
			}
		case !ok:
			verdict = VerdictFailed
			basis = fmt.Sprintf("check %s not satisfied: %s", chk, detail)
		}
	}

	// 冲突结论不得静默覆盖：已设置的结论保留并记录冲突。
	if rec.VerdictSet && rec.Verdict != verdict {
		o.audit(rec.RunID, rec.EventID, "verdict", fmt.Sprintf("conflict: existing verdict %s kept, new verdict %s discarded", rec.Verdict, verdict))
	} else if !rec.VerdictSet {
		rec.Verdict = verdict
		rec.VerdictSet = true
	}
	o.audit(rec.RunID, rec.EventID, "verdict", fmt.Sprintf("final verdict=%s basis: %s", rec.Verdict, basis))

	switch rec.Verdict {
	case VerdictSuccess:
		rec.EventStatus = EventResolved
	case VerdictManual:
		rec.EventStatus = EventManual
	default:
		rec.EventStatus = EventFailed
	}
}

// newRunRecordLocked 生成新的运行记录，调用方必须持有 o.mu。
func (o *Orchestrator) newRunRecordLocked(ev FaultEvent) RunRecord {
	o.runSeq++
	seq := o.runSeq
	return RunRecord{
		RunID:       fmt.Sprintf("run-%d-%s", seq, ev.ID),
		EventID:     ev.ID,
		Source:      ev.Source,
		Kind:        ev.Kind,
		EventStatus: EventReceived,
	}
}

// audit 追加审计条目。
func (o *Orchestrator) audit(runID, eventID, category, msg string) {
	_ = o.store.AppendAudit(AuditEntry{
		At:       o.cfg.Now(),
		RunID:    runID,
		EventID:  eventID,
		Category: category,
		Message:  msg,
	})
}

// findStep 按计划名查找步骤定义。
func findStep(plan []Step, name string) *Step {
	for i := range plan {
		if plan[i].Name == name {
			return &plan[i]
		}
	}
	return nil
}

// depsState 汇总依赖步骤状态：任一失败/跳过 -> 该状态；全部成功 -> 成功；否则 pending。
func depsState(steps []StepRecord, deps []string) StepState {
	allOK := true
	for _, dep := range deps {
		found := false
		for _, st := range steps {
			if st.Name == dep {
				found = true
				switch st.State {
				case StepSucceeded:
				case StepFailed:
					return StepFailed
				case StepSkipped:
					return StepSkipped
				default:
					allOK = false
				}
			}
		}
		if !found {
			return StepFailed
		}
	}
	if allOK {
		return StepSucceeded
	}
	return StepPending
}
