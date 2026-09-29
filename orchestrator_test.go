package orchestrator

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// testClock 可手动推进的时钟。
type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func baseTime() time.Time { return time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC) }

func diskPolicy() Policy {
	return Policy{
		Kind:        "disk_full",
		MinSeverity: 3,
		Plan: []Step{
			{Name: "diagnose", Action: "diagnose", Timeout: 100 * time.Millisecond},
			{Name: "cleanup", Action: "cleanup", DependsOn: []string{"diagnose"}, Timeout: 100 * time.Millisecond, MaxRetries: 2},
			{Name: "verify-space", Action: "df", DependsOn: []string{"cleanup"}, Timeout: 100 * time.Millisecond},
		},
		Checks: []string{"disk-usage-ok"},
	}
}

func newTestOrch(clk *testClock, store Store, exec *SimExecutor, checker *SimChecker, windows []Window) *Orchestrator {
	return New(Config{
		SuppressWindow: time.Second,
		DefaultTimeout: 100 * time.Millisecond,
		Now:            clk.Now,
	}, store, exec, checker, NewPolicySet(diskPolicy()), windows)
}

func dumpAudit(t *testing.T, store Store, runID string) {
	t.Helper()
	entries, _ := store.Audit()
	t.Logf("---- audit trail for %s ----", runID)
	for _, e := range entries {
		if runID != "" && e.RunID != runID && e.RunID != "" {
			continue
		}
		t.Logf("[%s] run=%s event=%s %s", e.Category, e.RunID, e.EventID, e.Message)
	}
}

func dumpRun(t *testing.T, rec RunRecord) {
	t.Helper()
	t.Logf("run=%s event=%s status=%s reject=%q verdict=%s", rec.RunID, rec.EventID, rec.EventStatus, rec.RejectWhy, rec.Verdict)
	for _, st := range rec.Steps {
		t.Logf("  step=%s state=%s attempts=%d err=%q", st.Name, st.State, st.Attempts, st.LastErr)
	}
}

// TestConcurrentDuplicateEvents 并发重复事件：抑制窗口内只触发一次修复。
func TestConcurrentDuplicateEvents(t *testing.T) {
	clk := &testClock{now: baseTime()}
	store := NewMemoryStore()
	exec := NewSimExecutor()
	exec.Script("cleanup", Sleep(50*time.Millisecond)) // 让修复耗时，制造并发窗口
	checker := NewSimChecker()
	orch := newTestOrch(clk, store, exec, checker, nil)

	const n = 16
	results := make([]SubmitResult, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = orch.Submit(context.Background(), FaultEvent{
				ID: fmt.Sprintf("ev-%d", i), Source: "node-1", Kind: "disk_full", Severity: 5,
			})
		}(i)
	}
	wg.Wait()

	accepted, suppressed := 0, 0
	var runID string
	for _, r := range results {
		if r.Accepted {
			accepted++
			runID = r.RunID
		} else if r.Status == EventSuppressed {
			suppressed++
		}
	}
	t.Logf("accepted=%d suppressed=%d", accepted, suppressed)
	if accepted != 1 {
		t.Fatalf("expect exactly 1 accepted, got %d", accepted)
	}
	if accepted+suppressed != n {
		t.Fatalf("expect all others suppressed, got accepted=%d suppressed=%d", accepted, suppressed)
	}
	for _, step := range []string{"diagnose", "cleanup", "verify-space"} {
		if c := exec.Calls(step); c != 1 {
			t.Fatalf("step %s executed %d times, want 1 (idempotent)", step, c)
		}
	}
	rec, _, _ := store.GetRun(runID)
	dumpRun(t, rec)
	if rec.EventStatus != EventResolved || rec.Verdict != VerdictSuccess {
		t.Fatalf("expect resolved/success, got %s/%s", rec.EventStatus, rec.Verdict)
	}
	dumpAudit(t, store, runID)
}

// TestMaintenanceWindowRejection 维护窗口拒绝，且与策略拒绝可区分。
func TestMaintenanceWindowRejection(t *testing.T) {
	clk := &testClock{now: baseTime()}
	store := NewMemoryStore()
	exec := NewSimExecutor()
	windows := []Window{{Name: "change-freeze", Start: baseTime().Add(-time.Hour), End: baseTime().Add(time.Hour)}}
	orch := newTestOrch(clk, store, exec, NewSimChecker(), windows)

	res := orch.Submit(context.Background(), FaultEvent{ID: "ev-mw", Source: "node-1", Kind: "disk_full", Severity: 5})
	t.Logf("maintenance submit -> accepted=%v status=%s reason=%s", res.Accepted, res.Status, res.Reason)
	if res.Accepted || res.Reason != RejectMaintenance {
		t.Fatalf("expect maintenance rejection, got %+v", res)
	}
	if exec.Calls("diagnose") != 0 {
		t.Fatal("rejected event must not trigger any step")
	}

	// 窗口外但级别不足 -> 策略拒绝，原因不同。
	clk.Advance(2 * time.Hour)
	res2 := orch.Submit(context.Background(), FaultEvent{ID: "ev-low", Source: "node-2", Kind: "disk_full", Severity: 1})
	t.Logf("policy submit -> accepted=%v status=%s reason=%s", res2.Accepted, res2.Status, res2.Reason)
	if res2.Accepted || res2.Reason != RejectPolicy {
		t.Fatalf("expect policy rejection, got %+v", res2)
	}
	if res.Reason == res2.Reason {
		t.Fatal("rejection reasons must be distinguishable")
	}
	dumpAudit(t, store, "")
}

// TestStepTimeoutAndRetry 步骤超时与失败重试：重试成功后不重复执行。
func TestStepTimeoutAndRetry(t *testing.T) {
	clk := &testClock{now: baseTime()}
	store := NewMemoryStore()
	exec := NewSimExecutor()
	// cleanup 第一次超时，第二次失败，第三次成功（MaxRetries=2 -> 共 3 次尝试）。
	exec.Script("cleanup", Hang(), Fail("disk busy"), OK("cleaned"))
	checker := NewSimChecker()
	orch := newTestOrch(clk, store, exec, checker, nil)

	res := orch.Submit(context.Background(), FaultEvent{ID: "ev-retry", Source: "node-1", Kind: "disk_full", Severity: 5})
	if !res.Accepted {
		t.Fatalf("expect accepted, got %+v", res)
	}
	rec, _, _ := store.GetRun(res.RunID)
	dumpRun(t, rec)
	if c := exec.Calls("cleanup"); c != 3 {
		t.Fatalf("cleanup attempts = %d, want 3", c)
	}
	if rec.EventStatus != EventResolved || rec.Verdict != VerdictSuccess {
		t.Fatalf("expect resolved/success after retries, got %s/%s", rec.EventStatus, rec.Verdict)
	}
	dumpAudit(t, store, res.RunID)
}

// TestStepFailsPermanently 重试耗尽后失败，结论为 failed，原因可区分。
func TestStepFailsPermanently(t *testing.T) {
	clk := &testClock{now: baseTime()}
	store := NewMemoryStore()
	exec := NewSimExecutor()
	exec.Script("cleanup", Fail("permission denied")) // 始终失败
	orch := newTestOrch(clk, store, exec, NewSimChecker(), nil)

	res := orch.Submit(context.Background(), FaultEvent{ID: "ev-fail", Source: "node-1", Kind: "disk_full", Severity: 5})
	rec, _, _ := store.GetRun(res.RunID)
	dumpRun(t, rec)
	if c := exec.Calls("cleanup"); c != 3 { // 1 + MaxRetries(2)
		t.Fatalf("cleanup attempts = %d, want 3", c)
	}
	if rec.EventStatus != EventFailed || rec.Verdict != VerdictFailed {
		t.Fatalf("expect failed, got %s/%s", rec.EventStatus, rec.Verdict)
	}
	// 依赖 cleanup 的步骤必须被跳过而不是执行。
	if exec.Calls("verify-space") != 0 {
		t.Fatal("dependent step must be skipped, not executed")
	}
	var skipped bool
	for _, st := range rec.Steps {
		if st.Name == "verify-space" && st.State == StepSkipped {
			skipped = true
		}
	}
	if !skipped {
		t.Fatal("verify-space should be skipped")
	}
	dumpAudit(t, store, res.RunID)
}

// TestRecoverAfterCrash 进程中断后恢复：已完成步骤不重复执行，未完成步骤继续。
func TestRecoverAfterCrash(t *testing.T) {
	clk := &testClock{now: baseTime()}
	store := NewMemoryStore()

	// 模拟崩溃现场：diagnose 已成功，cleanup 处于 running（中断残留），verify-space 未开始。
	crashed := RunRecord{
		RunID: "run-crash", EventID: "ev-crash", Source: "node-9", Kind: "disk_full",
		EventStatus: EventRemediating,
		Steps: []StepRecord{
			{Name: "diagnose", State: StepSucceeded, Attempts: 1},
			{Name: "cleanup", State: StepRunning, Attempts: 1},
			{Name: "verify-space", State: StepPending},
		},
	}
	if err := store.SaveRun(crashed); err != nil {
		t.Fatal(err)
	}

	// 用同一个 store 重建编排器（模拟进程重启）。
	exec := NewSimExecutor()
	orch := newTestOrch(clk, store, exec, NewSimChecker(), nil)
	if err := orch.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}

	if c := exec.Calls("diagnose"); c != 0 {
		t.Fatalf("succeeded step re-executed %d times, want 0", c)
	}
	if c := exec.Calls("cleanup"); c != 1 {
		t.Fatalf("interrupted step should resume once, got %d", c)
	}
	if c := exec.Calls("verify-space"); c != 1 {
		t.Fatalf("pending step should run once, got %d", c)
	}
	rec, _, _ := store.GetRun("run-crash")
	dumpRun(t, rec)
	if rec.EventStatus != EventResolved || rec.Verdict != VerdictSuccess {
		t.Fatalf("expect resolved/success after recovery, got %s/%s", rec.EventStatus, rec.Verdict)
	}
	dumpAudit(t, store, "run-crash")
}

// TestVerdictOutcomes 恢复确认：成功 / 失败 / 需人工 三种判定。
func TestVerdictOutcomes(t *testing.T) {
	cases := []struct {
		name       string
		check      checkResult
		wantStatus EventStatus
		wantVerdict Verdict
	}{
		{"check-pass", CheckOK("usage 40%"), EventResolved, VerdictSuccess},
		{"check-fail", CheckFail("usage 98%"), EventFailed, VerdictFailed},
		{"check-manual", CheckManual("metrics unavailable"), EventManual, VerdictManual},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clk := &testClock{now: baseTime()}
			store := NewMemoryStore()
			checker := NewSimChecker()
			checker.Script("disk-usage-ok", tc.check)
			orch := newTestOrch(clk, store, NewSimExecutor(), checker, nil)

			res := orch.Submit(context.Background(), FaultEvent{ID: "ev-" + tc.name, Source: "node-1", Kind: "disk_full", Severity: 5})
			rec, _, _ := store.GetRun(res.RunID)
			dumpRun(t, rec)
			if rec.EventStatus != tc.wantStatus || rec.Verdict != tc.wantVerdict {
				t.Fatalf("want %s/%s, got %s/%s", tc.wantStatus, tc.wantVerdict, rec.EventStatus, rec.Verdict)
			}
			dumpAudit(t, store, res.RunID)
		})
	}
}

// TestVerdictNotOverwritten 冲突结论不得静默覆盖。
func TestVerdictNotOverwritten(t *testing.T) {
	clk := &testClock{now: baseTime()}
	store := NewMemoryStore()
	// 已有结论为 failed 的未终结运行（如人工正在处理）。
	rec := RunRecord{
		RunID: "run-conflict", EventID: "ev-conflict", Source: "node-7", Kind: "disk_full",
		EventStatus: EventRemediating,
		Steps: []StepRecord{
			{Name: "diagnose", State: StepSucceeded, Attempts: 1},
			{Name: "cleanup", State: StepPending},
			{Name: "verify-space", State: StepPending},
		},
		Verdict: VerdictFailed, VerdictSet: true,
	}
	if err := store.SaveRun(rec); err != nil {
		t.Fatal(err)
	}
	// 恢复后所有步骤成功、检查也通过 -> 新结论为 success，与已有 failed 冲突。
	orch := newTestOrch(clk, store, NewSimExecutor(), NewSimChecker(), nil)
	if err := orch.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, _, _ := store.GetRun("run-conflict")
	dumpRun(t, got)
	if got.Verdict != VerdictFailed {
		t.Fatalf("existing verdict must be kept, got %s", got.Verdict)
	}
	// 审计中应留下冲突记录。
	entries, _ := store.Audit()
	var conflict bool
	for _, e := range entries {
		if e.Category == "verdict" && e.RunID == "run-conflict" {
			t.Logf("verdict audit: %s", e.Message)
			if got := e.Message; len(got) >= 8 && got[:8] == "conflict" {
				conflict = true
			}
		}
	}
	if !conflict {
		t.Fatal("conflict must be recorded in audit log")
	}
	dumpAudit(t, store, "run-conflict")
}
