package reconcile

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/h3yow/self-healing-ops-orchestrator-go/internal/audit"
	"github.com/h3yow/self-healing-ops-orchestrator-go/internal/clock"
	"github.com/h3yow/self-healing-ops-orchestrator-go/internal/store"
)

// Verdict is the final judgment after remediation.
type Verdict string

const (
	VerdictSuccess       Verdict = "success"
	VerdictFailed        Verdict = "failed"
	VerdictManual        Verdict = "manual"
	VerdictIndeterminate Verdict = "indeterminate"
)

// ErrConclusionConflict means a different verdict already exists for the
// execution and must not be silently overwritten.
var ErrConclusionConflict = errors.New("reconcile: conflicting conclusion already recorded")

// ProbeResult describes the observed target state after remediation.
type ProbeResult struct {
	Healthy bool
	Detail  string
	Error   error
}

// Probe checks the target state.
type Probe interface {
	Check(ctx context.Context, execID string, target string, faultType string) ProbeResult
}

// ProbeFn adapts a function to Probe.
type ProbeFn func(ctx context.Context, execID, target, faultType string) ProbeResult

func (f ProbeFn) Check(ctx context.Context, execID, target, faultType string) ProbeResult {
	return f(ctx, execID, target, faultType)
}

const conclusionPrefix = "conclusions/"

// Conclusion is the persisted final conclusion record.
// EventID provides correlation back to the originating event.
type Conclusion struct {
	ExecID      string            `json:"exec_id"`
	IncidentID  string            `json:"incident_id"`
	EventID     string            `json:"event_id"`
	Verdict     Verdict           `json:"verdict"`
	Reason      string            `json:"reason"`
	StepResults map[string]string `json:"step_results,omitempty"`
	Probe       string            `json:"probe_detail,omitempty"`
	Version     int64             `json:"version"`
	CreatedAt   time.Time         `json:"created_at"`
}

// Reconciler verifies target state and records the final conclusion.
type Reconciler struct {
	st    store.Store
	probe Probe
	audit *audit.Logger
	clk   clock.Clock
}

func NewReconciler(st store.Store, probe Probe, al *audit.Logger, clk clock.Clock) *Reconciler {
	return &Reconciler{st: st, probe: probe, audit: al, clk: clk}
}

// Judge decides the verdict from execution outcome and observed state.
// Exported so tests can assert the decision rules directly.
func Judge(execSucceeded bool, probe ProbeResult) (Verdict, string) {
	switch {
	case probe.Error != nil:
		// Remediation may have worked, but we cannot confirm the target state:
		// a human must verify rather than claim success.
		return VerdictIndeterminate, "target state probe failed: " + probe.Error.Error()
	case execSucceeded && probe.Healthy:
		return VerdictSuccess, "all steps succeeded and target reported healthy"
	case execSucceeded && !probe.Healthy:
		// Actions ran green but the fault is still observable: manual review.
		return VerdictManual, "steps succeeded but target still unhealthy: " + probe.Detail
	case !execSucceeded && probe.Healthy:
		// Conflicting signals: steps failed but state looks healthy. Do not
		// claim a clean success; flag for manual confirmation.
		return VerdictManual, "steps failed but target reports healthy; confirm manually: " + probe.Detail
	default:
		return VerdictFailed, "steps failed and target remains unhealthy: " + probe.Detail
	}
}

// Conclude verifies target state, judges the outcome, and persists the
// conclusion. A conflicting existing verdict returns ErrConclusionConflict
// instead of silently overwriting.
func (r *Reconciler) Conclude(ctx context.Context, execID, incidentID, eventID string,
	stepResults map[string]string, execSucceeded bool) (*Conclusion, error) {
	probe := ProbeResult{}
	if r.probe != nil {
		probe = r.probe.Check(ctx, execID, "", "")
	}
	// Probe is given target/fault via closure; callers that want them use
	// ConcludeFor.
	return r.conclude(ctx, execID, incidentID, eventID, stepResults, execSucceeded, probe)
}

// ConcludeFor is the explicit-target form used by the orchestrator.
func (r *Reconciler) ConcludeFor(ctx context.Context, execID, incidentID, eventID, target, faultType string,
	stepResults map[string]string, execSucceeded bool) (*Conclusion, error) {
	probe := ProbeResult{}
	if r.probe != nil {
		probe = r.probe.Check(ctx, execID, target, faultType)
	}
	return r.conclude(ctx, execID, incidentID, eventID, stepResults, execSucceeded, probe)
}

func (r *Reconciler) conclude(ctx context.Context, execID, incidentID, eventID string,
	stepResults map[string]string, execSucceeded bool, probe ProbeResult) (*Conclusion, error) {
	verdict, reason := Judge(execSucceeded, probe)
	now := time.Time{}
	if r.clk != nil {
		now = r.clk.Now()
	}

	// Guard against duplicate/concurrent conclusions with a different verdict.
	key := conclusionPrefix + execID
	if existing, ev, err := store.LoadAs[Conclusion](ctx, r.st, key); err == nil {
		existing.Version = ev
		if existing.Verdict != verdict {
			return nil, fmt.Errorf("%w: exec=%s existing=%s new=%s",
				ErrConclusionConflict, execID, existing.Verdict, verdict)
		}
		// Idempotent: same verdict -> return the stored record.
		return &existing, nil
	} else if !errors.Is(err, store.ErrNotFound) {
		return nil, err
	}

	c := &Conclusion{
		ExecID:      execID,
		IncidentID:  incidentID,
		EventID:     eventID,
		Verdict:     verdict,
		Reason:      reason,
		StepResults: stepResults,
		Probe:       probe.Detail,
		CreatedAt:   now,
	}
	v, err := store.SaveAs(ctx, r.st, key, 0, *c)
	if err != nil {
		// Concurrent create: reload and compare verdicts.
		if existing, ev, lerr := store.LoadAs[Conclusion](ctx, r.st, key); lerr == nil {
			existing.Version = ev
			if existing.Verdict != verdict {
				return nil, fmt.Errorf("%w: exec=%s existing=%s new=%s",
					ErrConclusionConflict, execID, existing.Verdict, verdict)
			}
			return &existing, nil
		}
		return nil, err
	}
	c.Version = v
	r.log(ctx, c, "conclusion_recorded", reason)
	return c, nil
}

// Get loads a conclusion by execution id.
func (r *Reconciler) Get(ctx context.Context, execID string) (*Conclusion, error) {
	c, v, err := store.LoadAs[Conclusion](ctx, r.st, conclusionPrefix+execID)
	if err != nil {
		return nil, err
	}
	c.Version = v
	return &c, nil
}

// List returns all conclusions (recovery/inspection).
func (r *Reconciler) List(ctx context.Context) ([]*Conclusion, error) {
	keys, err := r.st.List(ctx, conclusionPrefix)
	if err != nil {
		return nil, err
	}
	out := make([]*Conclusion, 0, len(keys))
	for _, k := range keys {
		c, v, err := store.LoadAs[Conclusion](ctx, r.st, k)
		if err != nil {
			return nil, err
		}
		c.Version = v
		out = append(out, &c)
	}
	return out, nil
}

func (r *Reconciler) log(ctx context.Context, c *Conclusion, action, reason string) {
	if r.audit == nil {
		return
	}
	_ = r.audit.Log(ctx, audit.Entry{
		EventID:    c.EventID,
		IncidentID: c.IncidentID,
		ExecID:     c.ExecID,
		Entity:     "conclusion",
		Action:     action,
		To:         string(c.Verdict),
		Reason:     reason,
		Detail:     c.StepResults,
	})
}
