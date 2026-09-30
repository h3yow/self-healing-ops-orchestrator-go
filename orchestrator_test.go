package bootstrap_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/h3yow/self-healing-ops-orchestrator-go/internal/audit"
	"github.com/h3yow/self-healing-ops-orchestrator-go/internal/clock"
	"github.com/h3yow/self-healing-ops-orchestrator-go/internal/event"
	"github.com/h3yow/self-healing-ops-orchestrator-go/internal/executor"
	"github.com/h3yow/self-healing-ops-orchestrator-go/internal/orchestrator"
	"github.com/h3yow/self-healing-ops-orchestrator-go/internal/reconcile"
	"github.com/h3yow/self-healing-ops-orchestrator-go/internal/store"
	"github.com/h3yow/self-healing-ops-orchestrator-go/internal/workflow"
)

const diskFault = "disk_full"

func diskPlan() workflow.Plan {
	return workflow.Plan{FaultType: diskFault, Steps: []workflow.Step{
		{ID: "detect", Name: "detect", MaxAttempts: 1},
		{ID: "cleanup", Name: "cleanup", DependsOn: []string{"detect"}, MaxAttempts: 3, Timeout: time.Second},
		{ID: "restart", Name: "restart", DependsOn: []string{"cleanup"}, MaxAttempts: 1},
		{ID: "verify", Name: "verify", DependsOn: []string{"restart"}, MaxAttempts: 1},
	}}
}

type harness struct {
	dir   string
	st    store.Store
	clk   *clock.FakeClock
	sim   *executor.Simulator
	sink  *audit.FileSink
	al    *audit.Logger
	plans map[string]workflow.Plan
}

func newHarness(t *testing.T, healthy bool, maint event.Maintenance, lease time.Duration,
	probeErr error) *harness {
	t.Helper()
	dir := t.TempDir()
	st, err := store.NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	clk := clock.NewFakeClock(time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC))
	sink, err := audit.NewFileSink(filepath.Join(dir, "audit.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sink.Close() })
	al := audit.NewLogger(sink)
	sim := executor.NewSimulator()
	probe := reconcile.ProbeFn(func(_ context.Context, _, _, _ string) reconcile.ProbeResult {
		return reconcile.ProbeResult{Healthy: healthy, Detail: fmt.Sprintf("healthy=%v", healthy), Error: probeErr}
	})
	plans := map[string]workflow.Plan{diskFault: diskPlan()}
	_ = probe
	_ = sim
	return &harness{
		dir: dir, st: st, clk: clk, sim: sim, sink: sink, al: al, plans: plans,
	}
}

func (h *harness) orchestrator(maint event.Maintenance, healthy bool, probeErr error) *orchestrator.Orchestrator {
	probe := reconcile.ProbeFn(func(_ context.Context, _, _, _ string) reconcile.ProbeResult {
		return reconcile.ProbeResult{Healthy: healthy, Detail: fmt.Sprintf("healthy=%v", healthy), Error: probeErr}
	})
	cfg := orchestrator.Config{
		Store:       h.st,
		Clock:       h.clk,
		Maintenance: maint,
		Policy: event.PolicyChain{Rules: []event.Policy{
			&event.MinSeverityPolicy{Min: event.SevHigh},
			&event.AllowedFaultsPolicy{Allowed: map[string]bool{diskFault: true}},
		}},
		Audit:       h.al,
		Suppression: time.Minute,
		Lease:       lease,
		MaxParallel: 4,
	}
	return orchestrator.New(cfg, h.sim, probe, h.plans)
}

var lease = 20 * time.Millisecond

func mkEvent(id string) event.Event {
	return event.Event{
		ID: id, Source: "node-7", FaultType: diskFault, Target: "/var",
		Severity: event.SevHigh, Message: "disk usage 95%",
	}
}

func scriptAll(sim *executor.Simulator, modify func(string, *executor.ScriptedResult)) {
	base := map[string]executor.ScriptedResult{}
	for _, s := range diskPlan().Steps {
		base[s.ID] = executor.ScriptedResult{Output: s.ID + ":ok"}
	}
	for id, r := range base {
		if modify != nil {
			modify(id, &r)
		}
		sim.Script(id, r)
	}
}

// Scenario 1: many concurrent duplicate deliveries -> exactly one remediation.
func TestIntegration_ConcurrentDuplicateEvents(t *testing.T) {
	h := newHarness(t, true, event.AllowsAll{}, lease, nil)
	scriptAll(h.sim, nil)
	o := h.orchestrator(event.AllowsAll{}, true, nil)

	const n = 25
	var wg sync.WaitGroup
	outcomes := make([]*orchestrator.Outcome, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			out, err := o.Handle(context.Background(), mkEvent(fmt.Sprintf("evt-%02d", i)))
			if err != nil {
				t.Errorf("handle %d: %v", i, err)
				return
			}
			outcomes[i] = out
		}()
	}
	wg.Wait()

	triggered, suppressed := 0, 0
	var execID string
	for _, out := range outcomes {
		if out == nil {
			continue
		}
		if out.Rejected != nil {
			if out.Rejected.Reason != event.RejectSuppressed {
				t.Fatalf("unexpected rejection %s", out.Rejected.Reason)
			}
			suppressed++
		}
		if out.ExecID != "" {
			triggered++
			execID = out.ExecID
		}
	}
	t.Logf("events=%d triggered=%d suppressed=%d execID=%s", n, triggered, suppressed, execID)
	if triggered != 1 {
		t.Fatalf("exactly one remediation must run, got %d", triggered)
	}
	if suppressed != n-1 {
		t.Fatalf("remaining must be suppressed, got %d", suppressed)
	}
	for _, id := range []string{"detect", "cleanup", "restart", "verify"} {
		if got := h.sim.Invocations(id); got != 1 {
			t.Fatalf("step %s ran %d times, want exactly 1", id, got)
		}
	}
	logAuditTrail(t, h)
}

// Scenario 2: maintenance window rejection is distinct from suppression/policy.
func TestIntegration_MaintenanceWindowRejection(t *testing.T) {
	h := newHarness(t, true, nil, lease, nil)
	now := h.clk.Now()
	win := &event.OneShotWindow{Start: now, End: now.Add(30 * time.Minute)}
	o := h.orchestrator(win, true, nil)

	out, err := o.Handle(context.Background(), mkEvent("evt-m1"))
	if err != nil {
		t.Fatal(err)
	}
	if out.Rejected == nil || out.Rejected.Reason != event.RejectMaintenanceWindow {
		t.Fatalf("want maintenance_window rejection, got %+v", out.Rejected)
	}
	t.Logf("event=evt-m1 rejected reason=%s msg=%q", out.Rejected.Reason, out.Rejected.Message)

	// After window ends, the same event source is accepted and remediated.
	h.clk.Advance(31 * time.Minute)
	scriptAll(h.sim, nil)
	out2, err := o.Handle(context.Background(), mkEvent("evt-m2"))
	if err != nil {
		t.Fatal(err)
	}
	if out2.Rejected != nil || out2.Verdict != reconcile.VerdictSuccess {
		t.Fatalf("post-window outcome=%+v", out2)
	}
	t.Logf("event=evt-m2 after window -> exec=%s verdict=%s", out2.ExecID, out2.Verdict)
}

// Scenario 3a: step fails then succeeds on retry -> success verdict.
func TestIntegration_FailureRetryThenSuccess(t *testing.T) {
	h := newHarness(t, true, event.AllowsAll{}, lease, nil)
	scriptAll(h.sim, func(id string, r *executor.ScriptedResult) {
		if id == "cleanup" {
			r.Failures = []error{errors.New("device busy"), errors.New("device busy"), nil}
		}
	})
	o := h.orchestrator(event.AllowsAll{}, true, nil)
	out, err := o.Handle(context.Background(), mkEvent("evt-r1"))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("retry scenario exec=%s verdict=%s cleanup-runs=%d",
		out.ExecID, out.Verdict, h.sim.Invocations("cleanup"))
	if out.Verdict != reconcile.VerdictSuccess {
		t.Fatalf("verdict=%s", out.Verdict)
	}
	if h.sim.Invocations("cleanup") != 3 {
		t.Fatalf("cleanup should run 3 times, got %d", h.sim.Invocations("cleanup"))
	}
}

// Scenario 3b: step times out repeatedly -> failed verdict, dependents skipped.
func TestIntegration_TimeoutThenFailed(t *testing.T) {
	h := newHarness(t, false, event.AllowsAll{}, lease, nil)
	scriptAll(h.sim, func(id string, r *executor.ScriptedResult) {
		if id == "cleanup" {
			r.Delay = 30 * time.Millisecond
		}
	})
	plan := diskPlan()
	plan.Steps[1].Timeout = 5 * time.Millisecond
	plan.Steps[1].MaxAttempts = 2
	h.plans[diskFault] = plan
	o := h.orchestrator(event.AllowsAll{}, false, nil)

	out, err := o.Handle(context.Background(), mkEvent("evt-t1"))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("timeout scenario exec=%s verdict=%s", out.ExecID, out.Verdict)
	if out.Verdict != reconcile.VerdictFailed {
		t.Fatalf("verdict=%s want failed", out.Verdict)
	}
	if h.sim.Invocations("restart") != 0 {
		t.Fatal("dependent step must be skipped after hard failure")
	}
}

// Scenario 4: crash mid-run, new process recovers; completed steps never rerun.
func TestIntegration_InterruptAndRecovery(t *testing.T) {
	h := newHarness(t, true, event.AllowsAll{}, lease, nil)
	scriptAll(h.sim, nil)
	o := h.orchestrator(event.AllowsAll{}, true, nil)

	// Inject crash after cleanup completes and persists.
	o.Engine().CrashHook = func(execID, stepID string) bool { return stepID == "cleanup" }

	out, err := o.Handle(context.Background(), mkEvent("evt-c1"))
	if err != nil {
		t.Fatal(err)
	}
	if out.ExecID == "" {
		t.Fatal("expected an execution id even when interrupted")
	}
	t.Logf("crashed execution=%s (Handle returned before conclusion)", out.ExecID)

	// Incident is remediating and the exec is interrupted on disk.
	x, err := o.Engine().Get(context.Background(), out.ExecID)
	if err != nil {
		t.Fatal(err)
	}
	if x.Status != workflow.ExecInterrupted {
		t.Fatalf("status=%s want interrupted", x.Status)
	}
	t.Logf("pre-recovery steps: detect=%s cleanup=%s restart=%s verify=%s",
		x.Steps["detect"].Status, x.Steps["cleanup"].Status,
		x.Steps["restart"].Status, x.Steps["verify"].Status)

	// Simulate process restart: brand-new orchestrator over the SAME store.
	h.clk.Advance(lease + time.Second) // move the frozen clock past the stale lease
	o2 := h.orchestrator(event.AllowsAll{}, true, nil)
	recovered, err := o2.Recover(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(recovered) != 1 || recovered[0].Verdict != reconcile.VerdictSuccess {
		t.Fatalf("recovery=%+v err=%v", recovered, err)
	}
	t.Logf("recovered exec=%s status=%s verdict=%s",
		recovered[0].ExecID, recovered[0].Status, recovered[0].Verdict)

	for _, id := range []string{"detect", "cleanup"} {
		if got := h.sim.Invocations(id); got != 1 {
			t.Fatalf("completed step %s re-executed after recovery: %d", id, got)
		}
	}
	if h.sim.Invocations("restart") != 1 || h.sim.Invocations("verify") != 1 {
		t.Fatalf("remaining steps must run once after recovery")
	}
	logAuditTrail(t, h)
}

// Scenario 5: conflicting conclusions are never silently overwritten.
func TestIntegration_ConflictConclusionPreserved(t *testing.T) {
	h := newHarness(t, true, event.AllowsAll{}, lease, nil)
	scriptAll(h.sim, nil)
	o := h.orchestrator(event.AllowsAll{}, true, nil)
	out, err := o.Handle(context.Background(), mkEvent("evt-x1"))
	if err != nil {
		t.Fatal(err)
	}
	// Re-concluding the same exec with a contradictory result must fail.
	_, err = o.Reconciler().ConcludeFor(context.Background(), out.ExecID,
		out.IncidentID, "evt-x1", "/var", diskFault, nil, false)
	if !errors.Is(err, reconcile.ErrConclusionConflict) {
		t.Fatalf("want conflict, got %v", err)
	}
	t.Logf("contradictory conclusion rejected: %v", err)
}

func logAuditTrail(t *testing.T, h *harness) {
	t.Helper()
	entries, err := audit.ReadEntries(filepath.Join(h.dir, "audit.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("--- audit trail (%d entries) ---", len(entries))
	for _, e := range entries {
		t.Logf("seq=%-3d event=%-7s exec=%-22s entity=%-10s action=%-24s %s -> %s %s",
			e.Seq, e.EventID, short(e.ExecID), e.Entity, e.Action, e.From, e.To, reason(e.Reason))
	}
}

func short(s string) string {
	if len(s) > 22 {
		return s[:22]
	}
	return s
}

func reason(s string) string {
	if s == "" {
		return ""
	}
	return "[" + s + "]"
}
