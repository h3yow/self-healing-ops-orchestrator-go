package workflow

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/h3yow/self-healing-ops-orchestrator-go/internal/store"
)

// Start persists a fresh execution, acquires its lease and runs the plan.
func (e *Engine) Start(ctx context.Context, incidentID, eventID, faultType, target string, plan Plan) (*Execution, error) {
	if err := ValidatePlan(plan); err != nil {
		return nil, err
	}
	now := e.clk.Now()
	x := &Execution{
		ID:         newExecID(now),
		IncidentID: incidentID,
		EventID:    eventID,
		FaultType:  faultType,
		Target:     target,
		Status:     ExecRunning,
		Steps:      map[string]StepState{},
		StartedAt:  now,
		UpdatedAt:  now,
	}
	for _, s := range plan.Steps {
		x.Steps[s.ID] = StepState{ID: s.ID, Status: StepPending}
	}

	// Persist first with version 0 (create), then take the lease.
	version, err := e.saveExec(ctx, x, 0)
	if err != nil {
		return nil, err
	}
	e.auditLog(ctx, x, "execution_created", plan.FaultType)

	token, version, err := e.acquireLock(ctx, x, version)
	if err != nil {
		return nil, err
	}
	e.runAndRelease(ctx, x, plan, token, version)
	return x, nil
}

// Resume continues an interrupted execution. Steps that already succeeded are
// never re-executed; only pending/running steps (the latter reset to pending
// after a crash) are driven again.
func (e *Engine) Resume(ctx context.Context, execID string) (*Execution, error) {
	x, version, err := e.loadExec(ctx, execID)
	if err != nil {
		return nil, err
	}

	if x.Status == ExecSucceeded || x.Status == ExecFailed || x.Status == ExecCancelled {
		// Already terminal: nothing to resume.
		return x, nil
	}

	if e.leaseValid(x) {
		return nil, fmt.Errorf("%w: exec=%s", ErrLeaseHeld, execID)
	}

	// Stale lock from a dead process: take it over. A step left "running"
	// never completed, so reset it to pending for a safe retry; succeeded
	// steps stay untouched.
	for id, st := range x.Steps {
		if st.Status == StepRunning {
			st.Status = StepPending
			st.StartedAt = time.Time{}
			x.Steps[id] = st
		}
	}
	x.Status = ExecRunning
	x.FinishedAt = time.Time{}
	x.AttemptedRun++

	plan, err := e.planOf(x)
	if err != nil {
		return nil, err
	}
	token, version, err := e.acquireLock(ctx, x, version)
	if err != nil {
		return nil, err
	}
	e.auditLog(ctx, x, "execution_resumed",
		fmt.Sprintf("run #%d after stale lock", x.AttemptedRun+1))
	e.runAndRelease(ctx, x, plan, token, version)
	return x, nil
}

// Cancel requests cancellation; the running loop observes it at its next
// scheduling pass (durable flag, avoids relying on in-process context).
func (e *Engine) Cancel(ctx context.Context, execID string) error {
	x, version, err := e.loadExec(ctx, execID)
	if err != nil {
		return err
	}
	if x.Status != ExecRunning && x.Status != ExecInterrupted {
		return nil
	}
	x.Status = ExecCancelled
	_, err = e.saveExec(ctx, x, version)
	if err != nil {
		return err
	}
	e.auditLog(ctx, x, "cancel_requested", "")
	return nil
}

// Get loads an execution.
func (e *Engine) Get(ctx context.Context, execID string) (*Execution, error) {
	x, _, err := e.loadExec(ctx, execID)
	return x, err
}

// runAndRelease runs the DAG, then marks interrupted on crash or releases the
// lock on a clean terminal finish.
func (e *Engine) runAndRelease(ctx context.Context, x *Execution, plan Plan, token string, version int64) {
	res := e.runPlan(ctx, x, plan, token, &version)
	if res == runCrashed {
		// Lock intentionally remains held until it goes stale: a concurrent
		// runner must not pick it up immediately.
		x.Status = ExecInterrupted
		_, _ = e.saveExec(ctx, x, version)
		return
	}
	_, _ = e.releaseLock(ctx, x, version, token)
}

// planOf needs the step definitions; the engine cannot reconstruct arbitrary
// payloads from state alone, so plans are supplied via PlanResolver.
func (e *Engine) planOf(x *Execution) (Plan, error) {
	if e.PlanResolver == nil {
		return Plan{}, errors.New("workflow: no plan resolver configured for resume")
	}
	return e.PlanResolver(x.FaultType), nil
}

func newExecID(now time.Time) string {
	return "exec-" + strconv.FormatInt(now.UnixNano(), 36) + "-" + newToken()[:6]
}

var _ = store.ErrNotFound
