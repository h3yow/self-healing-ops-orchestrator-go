package reconcile

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/h3yow/self-healing-ops-orchestrator-go/internal/audit"
	"github.com/h3yow/self-healing-ops-orchestrator-go/internal/clock"
	"github.com/h3yow/self-healing-ops-orchestrator-go/internal/store"
)

func newRec(t *testing.T, probe Probe) *Reconciler {
	st, err := store.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	clk := clock.NewFakeClock(time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC))
	return NewReconciler(st, probe, audit.NewLogger(), clk)
}

func TestJudgeRules(t *testing.T) {
	cases := []struct {
		name string
		ok   bool
		p    ProbeResult
		want Verdict
	}{
		{"green both", true, ProbeResult{Healthy: true, Detail: "ok"}, VerdictSuccess},
		{"steps ok target bad", true, ProbeResult{Healthy: false, Detail: "still full"}, VerdictManual},
		{"steps bad target ok", false, ProbeResult{Healthy: true, Detail: "ok"}, VerdictManual},
		{"both bad", false, ProbeResult{Healthy: false, Detail: "full"}, VerdictFailed},
		{"probe error", true, ProbeResult{Error: errors.New("conn refused")}, VerdictIndeterminate},
	}
	for _, c := range cases {
		v, reason := Judge(c.ok, c.p)
		if v != c.want {
			t.Errorf("%s: verdict=%s want %s", c.name, v, c.want)
		}
		t.Logf("judge[%s] execOK=%v healthy=%v probeErr=%v -> %s (%s)",
			c.name, c.ok, c.p.Healthy, c.p.Error, v, reason)
	}
}

func TestConclude_SuccessAndTraceability(t *testing.T) {
	r := newRec(t, ProbeFn(func(_ context.Context, _, _, _ string) ProbeResult {
		return ProbeResult{Healthy: true, Detail: "disk 40%"}
	}))
	c, err := r.ConcludeFor(context.Background(), "exec-1", "inc-1", "evt-9",
		"/var", "disk_full", map[string]string{"cleanup": "succeeded"}, true)
	if err != nil {
		t.Fatal(err)
	}
	if c.Verdict != VerdictSuccess {
		t.Fatalf("verdict=%s", c.Verdict)
	}
	if c.EventID != "evt-9" || c.IncidentID != "inc-1" {
		t.Fatal("conclusion must link event and incident")
	}
	t.Logf("conclusion exec=%s incident=%s event=%s verdict=%s reason=%q steps=%v",
		c.ExecID, c.IncidentID, c.EventID, c.Verdict, c.Reason, c.StepResults)

	// Idempotent re-conclude with same verdict.
	c2, err := r.ConcludeFor(context.Background(), "exec-1", "inc-1", "evt-9",
		"/var", "disk_full", map[string]string{"cleanup": "succeeded"}, true)
	if err != nil || c2.Version != c.Version {
		t.Fatalf("same verdict must be idempotent: %v", err)
	}
}

func TestConclude_ConflictNotOverwritten(t *testing.T) {
	r := newRec(t, ProbeFn(func(_ context.Context, _, _, _ string) ProbeResult {
		return ProbeResult{Healthy: true}
	}))
	ctx := context.Background()
	if _, err := r.ConcludeFor(ctx, "exec-2", "inc-2", "evt-2", "/", "x", nil, true); err != nil {
		t.Fatal(err)
	}
	// A second, contradictory conclusion must fail and preserve the original.
	_, err := r.ConcludeFor(ctx, "exec-2", "inc-2", "evt-2", "/", "x", nil, false)
	if !errors.Is(err, ErrConclusionConflict) {
		t.Fatalf("want conflict, got %v", err)
	}
	stored, _ := r.Get(ctx, "exec-2")
	if stored.Verdict != VerdictSuccess {
		t.Fatalf("original conclusion silently overwritten: %s", stored.Verdict)
	}
	t.Logf("conflicting verdict rejected: %v; stored=%s", err, stored.Verdict)
}
