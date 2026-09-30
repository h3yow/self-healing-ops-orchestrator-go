package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
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

const fault = "disk_full"

func plan() workflow.Plan {
	return workflow.Plan{FaultType: fault, Steps: []workflow.Step{
		{ID: "detect", Name: "detect", MaxAttempts: 1},
		{ID: "cleanup", Name: "cleanup", DependsOn: []string{"detect"}, MaxAttempts: 3},
		{ID: "restart", Name: "restart", DependsOn: []string{"cleanup"}, MaxAttempts: 1},
		{ID: "verify", Name: "verify", DependsOn: []string{"restart"}, MaxAttempts: 1},
	}}
}

func main() {
	ctx := context.Background()
	dir, _ := os.MkdirTemp("", "selfheal-demo-")
	defer os.RemoveAll(dir)
	fmt.Println("state dir:", dir)

	st, err := store.NewFileStore(filepath.Join(dir, "state"))
	must(err)
	clk := clock.NewRealClock()
	sink, err := audit.NewFileSink(filepath.Join(dir, "audit.jsonl"))
	must(err)
	defer sink.Close()
	al := audit.NewLogger(sink)

	sim := executor.NewSimulator().
		Script("detect", executor.ScriptedResult{Output: "disk 95%"}).
		Script("cleanup", executor.ScriptedResult{
			Failures: []error{errors.New("device busy"), nil},
			Output:   "freed 40GB",
		}).
		Script("restart", executor.ScriptedResult{Output: "service restarted"}).
		Script("verify", executor.ScriptedResult{Output: "healthy"})

	probe := reconcile.ProbeFn(func(_ context.Context, _, _, _ string) reconcile.ProbeResult {
		return reconcile.ProbeResult{Healthy: true, Detail: "disk 40%"}
	})

	cfg := orchestrator.Config{
		Store:       st,
		Clock:       clk,
		Maintenance: event.AllowsAll{},
		Policy: event.PolicyChain{Rules: []event.Policy{
			&event.MinSeverityPolicy{Min: event.SevHigh},
			&event.AllowedFaultsPolicy{Allowed: map[string]bool{fault: true}},
		}},
		Audit:       al,
		Suppression: 2 * time.Second,
		Lease:       500 * time.Millisecond,
		MaxParallel: 4,
	}
	plans := map[string]workflow.Plan{fault: plan()}
	o := orchestrator.New(cfg, sim, probe, plans)

	evt := event.Event{
		ID: "evt-1", Source: "node-7", FaultType: fault, Target: "/var",
		Severity: event.SevHigh, Message: "disk usage 95%",
	}

	fmt.Println("\n== 1. first event (cleanup fails once then retries) ==")
	out, err := o.Handle(ctx, evt)
	must(err)
	fmt.Printf("incident=%s exec=%s verdict=%s\n", out.IncidentID, out.ExecID, out.Verdict)

	fmt.Println("\n== 2. duplicate redelivery inside suppression window ==")
	dup := evt
	dup.ID = "evt-2"
	out2, err := o.Handle(ctx, dup)
	must(err)
	if out2.Rejected != nil {
		fmt.Printf("duplicate rejected: reason=%s msg=%s\n", out2.Rejected.Reason, out2.Rejected.Message)
	}

	fmt.Println("\n== 3. crash mid-run on a new fault cycle, then recover ==")
	// Wait for suppression to elapse so the same source opens a new cycle.
	time.Sleep(2100 * time.Millisecond)
	o.Engine().CrashHook = func(execID, stepID string) bool { return stepID == "cleanup" }
	evt3 := evt
	evt3.ID = "evt-3"
	out3, err := o.Handle(ctx, evt3)
	must(err)
	fmt.Printf("interrupted exec=%s\n", out3.ExecID)

	time.Sleep(600 * time.Millisecond)             // stale lease
	o2 := orchestrator.New(cfg, sim, probe, plans) // fresh process, same store
	recovered, err := o2.Recover(ctx)
	must(err)
	for _, r := range recovered {
		fmt.Printf("recovered exec=%s status=%s verdict=%s\n", r.ExecID, r.Status, r.Verdict)
	}
	fmt.Printf("completed-step invocations after resume: detect=%d cleanup=%d\n",
		sim.Invocations("detect"), sim.Invocations("cleanup"))

	fmt.Println("\n== audit trail ==")
	entries, err := audit.ReadEntries(filepath.Join(dir, "audit.jsonl"))
	must(err)
	for _, e := range entries {
		fmt.Printf("%-10s %-24s exec=%-26s %s -> %s %s\n",
			e.Entity, e.Action, e.ExecID, e.From, e.To, e.Reason)
	}
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "fatal:", err)
		os.Exit(1)
	}
}
