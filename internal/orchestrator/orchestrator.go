package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/h3yow/self-healing-ops-orchestrator-go/internal/audit"
	"github.com/h3yow/self-healing-ops-orchestrator-go/internal/clock"
	"github.com/h3yow/self-healing-ops-orchestrator-go/internal/event"
	"github.com/h3yow/self-healing-ops-orchestrator-go/internal/reconcile"
	"github.com/h3yow/self-healing-ops-orchestrator-go/internal/store"
	"github.com/h3yow/self-healing-ops-orchestrator-go/internal/workflow"
)

// Outcome describes how an ingested event was handled.
type Outcome struct {
	IncidentID string
	ExecID     string
	Verdict    reconcile.Verdict
	Rejected   *event.RejectedError
	// Already is true when the event was a duplicate merged into an open or
	// suppressed incident (still returned with a Rejected suppression error).
	Already bool
}

// Config wires the orchestrator together.
type Config struct {
	Store            store.Store
	Clock            clock.Clock
	Maintenance      event.Maintenance
	Policy           event.Policy
	Audit            *audit.Logger
	Suppression      time.Duration
	Lease            time.Duration
	MaxParallel      int
	ReconcileBackoff time.Duration
}

// Probe is the target-state checker used after remediation.
type Probe = reconcile.Probe

// Orchestrator is the facade for event intake + remediation + confirmation.
type Orchestrator struct {
	cfg        Config
	receiver   *event.Receiver
	engine     *workflow.Engine
	reconciler *reconcile.Reconciler
	plans      map[string]workflow.Plan
}

// New builds the full object graph from a simulator/executor port, a probe and
// per-fault-type plans.
func New(cfg Config, exec workflow.ExecutorPort, probe reconcile.Probe,
	plans map[string]workflow.Plan) *Orchestrator {
	if cfg.MaxParallel <= 0 {
		cfg.MaxParallel = 4
	}
	al := cfg.Audit
	receiver := event.NewReceiver(cfg.Store, cfg.Clock, cfg.Maintenance,
		cfg.Policy, al, cfg.Suppression)
	engine := workflow.NewEngine(cfg.Store, exec, cfg.Clock, al,
		cfg.Lease, cfg.MaxParallel)
	engine.PlanResolver = func(faultType string) workflow.Plan {
		return plans[faultType]
	}
	rec := reconcile.NewReconciler(cfg.Store, probe, al, cfg.Clock)
	return &Orchestrator{
		cfg:        cfg,
		receiver:   receiver,
		engine:     engine,
		reconciler: rec,
		plans:      plans,
	}
}

// Handle ingests an event; if it triggers remediation the workflow runs and a
// conclusion is produced. Duplicate delivery is safe.
func (o *Orchestrator) Handle(ctx context.Context, e event.Event) (*Outcome, error) {
	inc, trigger, err := o.receiver.Ingest(ctx, e)
	if err != nil {
		rej := event.AsRejected(err)
		out := &Outcome{Rejected: rej}
		if inc != nil {
			out.IncidentID = inc.ID
			out.Already = rej != nil && rej.Reason == event.RejectSuppressed
		}
		// Rejections are a normal, explicit outcome, not a transport error.
		return out, nil
	}
	out := &Outcome{IncidentID: inc.ID}
	if !trigger {
		out.Already = true
		return out, nil
	}

	plan, ok := o.plans[e.FaultType]
	if !ok {
		// No registered plan: policy should normally have filtered this, but
		// fail closed with a distinct rejection.
		out.Rejected = &event.RejectedError{
			Reason:  event.RejectPolicyNotMet,
			Message: fmt.Sprintf("no remediation plan registered for %q", e.FaultType),
		}
		return out, nil
	}

	x, err := o.engine.Start(ctx, inc.ID, e.ID, e.FaultType, e.Target, plan)
	if err != nil {
		return out, err
	}
	out.ExecID = x.ID
	if x.Status == workflow.ExecInterrupted {
		// Process interrupt: no conclusion yet; Recover resumes the run and
		// only then confirms the result. Incident stays remediating.
		out.Verdict = reconcile.Verdict("interrupted")
		return out, nil
	}
	if err := o.receiver.MarkRemediating(ctx, inc.ID, x.ID); err != nil {
		return out, err
	}

	conclusion, err := o.finalize(ctx, x)
	if err != nil {
		return out, err
	}
	out.Verdict = conclusion.Verdict
	return out, nil
}

// finalize maps an execution to a conclusion and closes the incident.
func (o *Orchestrator) finalize(ctx context.Context, x *workflow.Execution) (*reconcile.Conclusion, error) {
	steps := map[string]string{}
	for id, s := range x.Steps {
		steps[id] = string(s.Status)
	}
	execOK := x.Status == workflow.ExecSucceeded

	conclusion, err := o.reconciler.ConcludeFor(ctx, x.ID, x.IncidentID, x.EventID,
		x.Target, x.FaultType, steps, execOK)
	if err != nil {
		// A conflicting conclusion is surfaced, never swallowed.
		return nil, err
	}

	terminal := event.IncidentFailed
	if conclusion.Verdict == reconcile.VerdictSuccess {
		terminal = event.IncidentResolved
	}
	if err := o.receiver.MarkTerminal(ctx, x.IncidentID, terminal, string(conclusion.Verdict)); err != nil {
		return nil, err
	}
	return conclusion, nil
}

// Recover resumes interrupted executions after a process restart and produces
// their conclusions. Completed steps are never re-executed.
func (o *Orchestrator) Recover(ctx context.Context) ([]Recovered, error) {
	execs, err := o.engine.List(ctx)
	if err != nil {
		return nil, err
	}
	var out []Recovered
	var firstErr error
	for _, x := range execs {
		if x.Status != workflow.ExecInterrupted {
			continue
		}
		resumed, err := o.engine.Resume(ctx, x.ID)
		if err != nil {
			if errors.Is(err, workflow.ErrLeaseHeld) {
				// Another live runner owns it; skip but report the reason.
				out = append(out, Recovered{
					ExecID: x.ID, IncidentID: x.IncidentID,
					Status: x.Status, Verdict: reconcile.Verdict("lease_held"),
				})
				continue
			}
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		rec := Recovered{ExecID: resumed.ID, IncidentID: resumed.IncidentID, Status: resumed.Status}
		if resumed.Status == workflow.ExecSucceeded ||
			resumed.Status == workflow.ExecFailed ||
			resumed.Status == workflow.ExecCancelled {
			conclusion, err := o.finalize(ctx, resumed)
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
			} else {
				rec.Verdict = conclusion.Verdict
			}
		}
		out = append(out, rec)
	}
	return out, firstErr
}

// Recovered summarizes one resumed execution.
type Recovered struct {
	ExecID     string
	IncidentID string
	Verdict    reconcile.Verdict
	Status     workflow.ExecStatus
}

// Accessors for tests/demo.
func (o *Orchestrator) Receiver() *event.Receiver         { return o.receiver }
func (o *Orchestrator) Engine() *workflow.Engine          { return o.engine }
func (o *Orchestrator) Reconciler() *reconcile.Reconciler { return o.reconciler }
