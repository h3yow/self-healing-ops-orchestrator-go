package executor

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSimulator_ScriptedFailuresAndRecording(t *testing.T) {
	sim := NewSimulator().
		Script("s1", ScriptedResult{Failures: []error{errors.New("a"), errors.New("b"), nil}, Output: "done"})
	ctx := context.Background()
	a := Action{Name: "s1"}

	r1 := sim.Run(ctx, "exec-1", "s1", a)
	r2 := sim.Run(ctx, "exec-1", "s1", a)
	r3 := sim.Run(ctx, "exec-2", "s1", a)
	if r1.Err == nil || r2.Err == nil {
		t.Fatal("first two attempts must fail")
	}
	if r3.Err != nil || r3.Output != "done" {
		t.Fatalf("third attempt must succeed, got %+v", r3)
	}
	if got := sim.Invocations("s1"); got != 3 {
		t.Fatalf("invocations=%d want 3", got)
	}
	ids := sim.InvocationExecIDs("s1")
	t.Logf("step=s1 invocations=%d execIDs=%v final-output=%q", got3(sim), ids, r3.Output)
}

func got3(s *Simulator) int { return s.Invocations("s1") }

func TestSimulator_ContextTimeout(t *testing.T) {
	sim := NewSimulator().Script("slow", ScriptedResult{Delay: 50 * time.Millisecond})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	r := sim.Run(ctx, "e", "slow", Action{Name: "slow"})
	if !errors.Is(r.Err, context.DeadlineExceeded) {
		t.Fatalf("want deadline exceeded, got %v", r.Err)
	}
	t.Logf("slow step interrupted by timeout as expected: %v", r.Err)
}
