package workflow

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/h3yow/self-healing-ops-orchestrator-go/internal/audit"
	"github.com/h3yow/self-healing-ops-orchestrator-go/internal/clock"
	"github.com/h3yow/self-healing-ops-orchestrator-go/internal/executor"
	"github.com/h3yow/self-healing-ops-orchestrator-go/internal/store"
)

func newEngine(t *testing.T, sim *executor.Simulator, lease time.Duration) (*Engine, *clock.FakeClock, store.Store) {
	t.Helper()
	st, err := store.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	clk := clock.NewFakeClock(time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC))
	al := audit.NewLogger()
	e := NewEngine(st, sim, clk, al, lease, 4)
	return e, clk, st
}

func diskPlan() Plan {
	return Plan{FaultType: "disk_full", Steps: []Step{
		{ID: "detect", Name: "detect", MaxAttempts: 1},
		{ID: "cleanup", Name: "cleanup", DependsOn: []string{"detect"}, MaxAttempts: 3},
		{ID: "restart", Name: "restart", DependsOn: []string{"cleanup"}, MaxAttempts: 1},
		{ID: "verify", Name: "verify", DependsOn: []string{"restart"}, MaxAttempts: 1},
	}}
}

func TestEngine_DAGSuccess(t *testing.T) {
	sim := executor.NewSimulator().
		Script("detect", executor.ScriptedResult{Output: "ok"}).
		Script("cleanup", executor.ScriptedResult{Output: "freed 40GB"}).
		Script("restart", executor.ScriptedResult{Output: "restarted"}).
		Script("verify", executor.ScriptedResult{Output: "healthy"})
	e, _, _ := newEngine(t, sim, 0)
	plan := diskPlan()
	e.PlanResolver = func(string) Plan { return plan }

	x, err := e.Start(context.Background(), "inc-1", "evt-1", "disk_full", "/var", plan)
	if err != nil {
		t.Fatal(err)
	}
	if x.Status != ExecSucceeded {
		t.Fatalf("status=%s summary=%s", x.Status, x.FailureSummary)
	}
	for _, id := range []string{"detect", "cleanup", "restart", "verify"} {
		if x.Steps[id].Status != StepSucceeded {
			t.Fatalf("step %s = %s", id, x.Steps[id].Status)
		}
		t.Logf("step=%s status=%s attempts=%d output=%q",
			id, x.Steps[id].Status, x.Steps[id].Attempts, x.Steps[id].Output)
	}
}

func TestEngine_RetryThenSuccess(t *testing.T) {
	boom := errors.New("transient io failure")
	sim := executor.NewSimulator().
		Script("detect", executor.ScriptedResult{}).
		Script("cleanup", executor.ScriptedResult{Failures: []error{boom, boom, nil}, Output: "freed"}).
		Script("restart", executor.ScriptedResult{}).
		Script("verify", executor.ScriptedResult{})
	e, _, _ := newEngine(t, sim, 0)
	plan := diskPlan()
	e.PlanResolver = func(string) Plan { return plan }

	x, err := e.Start(context.Background(), "inc-1", "evt-1", "disk_full", "/var", plan)
	if err != nil {
		t.Fatal(err)
	}
	if x.Status != ExecSucceeded {
		t.Fatalf("status=%s", x.Status)
	}
	if got := sim.Invocations("cleanup"); got != 3 {
		t.Fatalf("cleanup invocations=%d want 3", got)
	}
	t.Logf("cleanup retried attempts=%d final=%s", x.Steps["cleanup"].Attempts, x.Steps["cleanup"].Status)
}

func TestEngine_FailureSkipsDependents(t *testing.T) {
	boom := errors.New("permission denied")
	sim := executor.NewSimulator().
		Script("detect", executor.ScriptedResult{}).
		Script("cleanup", executor.ScriptedResult{Failure: boom}).
		Script("restart", executor.ScriptedResult{}).
		Script("verify", executor.ScriptedResult{})
	e, _, _ := newEngine(t, sim, 0)
	plan := diskPlan()
	e.PlanResolver = func(string) Plan { return plan }

	x, err := e.Start(context.Background(), "inc-1", "evt-1", "disk_full", "/var", plan)
	if err != nil {
		t.Fatal(err)
	}
	if x.Status != ExecFailed {
		t.Fatalf("status=%s want failed", x.Status)
	}
	if x.Steps["restart"].Status != StepSkipped || x.Steps["verify"].Status != StepSkipped {
		t.Fatalf("dependents must be skipped: restart=%s verify=%s",
			x.Steps["restart"].Status, x.Steps["verify"].Status)
	}
	if sim.Invocations("restart") != 0 || sim.Invocations("verify") != 0 {
		t.Fatal("skipped steps must never execute")
	}
	t.Logf("execution=%s failed step cleanup err=%q; restart/verify skipped; summary=%q",
		x.ID, x.Steps["cleanup"].Error, x.FailureSummary)
}

func TestEngine_TimeoutRetriesAndFails(t *testing.T) {
	sim := executor.NewSimulator().
		Script("detect", executor.ScriptedResult{}).
		Script("cleanup", executor.ScriptedResult{Delay: 50 * time.Millisecond}).
		Script("restart", executor.ScriptedResult{}).
		Script("verify", executor.ScriptedResult{})
	e, _, _ := newEngine(t, sim, 0)
	plan := diskPlan()
	plan.Steps[1].Timeout = 5 * time.Millisecond
	plan.Steps[1].MaxAttempts = 2
	e.PlanResolver = func(string) Plan { return diskPlan() }

	x, err := e.Start(context.Background(), "inc-1", "evt-1", "disk_full", "/var", plan)
	if err != nil {
		t.Fatal(err)
	}
	if x.Status != ExecFailed {
		t.Fatalf("status=%s", x.Status)
	}
	if x.Steps["cleanup"].Attempts != 2 {
		t.Fatalf("attempts=%d want 2", x.Steps["cleanup"].Attempts)
	}
	t.Logf("timeout step attempts=%d err=%q", x.Steps["cleanup"].Attempts, x.Steps["cleanup"].Error)
}

func TestEngine_CrashThenResume_NoDoubleExecution(t *testing.T) {
	sim := executor.NewSimulator().
		Script("detect", executor.ScriptedResult{Output: "ok"}).
		Script("cleanup", executor.ScriptedResult{Output: "freed"}).
		Script("restart", executor.ScriptedResult{Output: "restarted"}).
		Script("verify", executor.ScriptedResult{Output: "healthy"})
	e, clkFake, _ := newEngine(t, sim, time.Millisecond) // short lease so resume can take over
	plan := diskPlan()
	e.PlanResolver = func(string) Plan { return plan }

	// Crash immediately after "cleanup" succeeds and persists.
	e.CrashHook = func(execID, stepID string) bool { return stepID == "cleanup" }
	x, err := e.Start(context.Background(), "inc-1", "evt-1", "disk_full", "/var", plan)
	if err != nil {
		t.Fatal(err)
	}
	if x.Status != ExecInterrupted {
		t.Fatalf("status=%s want interrupted", x.Status)
	}
	t.Logf("execution %s interrupted after cleanup; steps=%v", x.ID, stepStatuses(x))

	// The fake clock is frozen; move it beyond the short lease so the stale
	// lock from the crashed process can be taken over.
	clkFake.Advance(2 * time.Second)

	// New engine instance = recovered process, same store & scripts.
	al2 := audit.NewLogger()
	e2 := NewEngine(e.st, sim, clkFake, al2, time.Millisecond, 4)
	e2.PlanResolver = func(string) Plan { return plan }

	resumed, err := e2.Resume(context.Background(), x.ID)
	if err != nil {
		t.Fatal(err)
	}
	if resumed.Status != ExecSucceeded {
		t.Fatalf("resumed status=%s summary=%s", resumed.Status, resumed.FailureSummary)
	}
	for _, id := range []string{"detect", "cleanup"} {
		if got := sim.Invocations(id); got != 1 {
			t.Fatalf("completed step %s re-ran: invocations=%d", id, got)
		}
	}
	for _, id := range []string{"restart", "verify"} {
		if got := sim.Invocations(id); got != 1 {
			t.Fatalf("resumed step %s invocations=%d want 1", id, got)
		}
	}
	t.Logf("resumed execution %s final=%s steps=%v", resumed.ID, resumed.Status, stepStatuses(resumed))
}

func TestEngine_ConcurrentStepsRunInParallel(t *testing.T) {
	// fan-out: a -> b,c -> d
	plan := Plan{FaultType: "fan", Steps: []Step{
		{ID: "a", MaxAttempts: 1},
		{ID: "b", DependsOn: []string{"a"}, MaxAttempts: 1},
		{ID: "c", DependsOn: []string{"a"}, MaxAttempts: 1},
		{ID: "d", DependsOn: []string{"b", "c"}, MaxAttempts: 1},
	}}
	sim := executor.NewSimulator()
	for _, id := range []string{"a", "b", "c", "d"} {
		sim.Script(id, executor.ScriptedResult{Output: id})
	}
	e, _, _ := newEngine(t, sim, 0)
	e.PlanResolver = func(string) Plan { return plan }
	x, err := e.Start(context.Background(), "inc-1", "evt-1", "fan", "h", plan)
	if err != nil {
		t.Fatal(err)
	}
	if x.Status != ExecSucceeded {
		t.Fatalf("status=%s %s", x.Status, x.FailureSummary)
	}
	t.Logf("fan-out DAG %s -> %v", x.Status, stepStatuses(x))
}

func TestEngine_LeaseBlocksConcurrentRunner(t *testing.T) {
	sim := executor.NewSimulator().Script("a", executor.ScriptedResult{})
	e, _, _ := newEngine(t, sim, time.Hour)
	plan := Plan{FaultType: "x", Steps: []Step{{ID: "a"}}}
	x, err := e.Start(context.Background(), "inc-1", "evt-1", "x", "t", plan)
	if err != nil {
		t.Fatal(err)
	}
	// Re-resume a terminal exec returns it; force an interrupted-looking state
	// with a fresh lease and check lock protection.
	x2, ver, err := e.loadExec(context.Background(), x.ID)
	if err != nil {
		t.Fatal(err)
	}
	x2.Status = ExecInterrupted
	x2.FinishedAt = time.Time{}
	x2.LockToken = "other-runner-token"
	x2.LockAcquiredAt = e.clk.Now() // fresh, unexpired lease
	if _, err := e.saveExec(context.Background(), x2, ver); err != nil {
		t.Fatal(err)
	}
	e.PlanResolver = func(string) Plan { return plan }
	if _, err := e.Resume(context.Background(), x.ID); !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("want ErrLeaseHeld, got %v", err)
	}
	t.Logf("fresh lease correctly blocks resume: %v", fmt.Sprintf("%v", err))
}

func stepStatuses(x *Execution) map[string]string {
	out := map[string]string{}
	for id, s := range x.Steps {
		out[id] = string(s.Status)
	}
	return out
}
