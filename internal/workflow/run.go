package workflow

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/h3yow/self-healing-ops-orchestrator-go/internal/audit"
	"github.com/h3yow/self-healing-ops-orchestrator-go/internal/executor"
	"github.com/h3yow/self-healing-ops-orchestrator-go/internal/store"
)

// runResult tells the caller how the loop ended.
type runResult int

const (
	runFinished runResult = iota // all steps reached a terminal state
	runCrashed                   // simulated process interrupt
)

// runPlan drives steps until the DAG is exhausted, cancelled, or crashed.
// It mutates and persists x. Every transition is durable before the next
// decision, which is what makes resume safe.
func (e *Engine) runPlan(ctx context.Context, x *Execution, plan Plan, token string, version *int64) runResult {
	stepByID := map[string]Step{}
	deps := map[string][]string{}
	for _, s := range plan.Steps {
		stepByID[s.ID] = s
		deps[s.ID] = s.DependsOn
	}

	sem := make(chan struct{}, e.maxParallel)
	if e.maxParallel <= 0 {
		sem = make(chan struct{}, len(plan.Steps))
	}

	// crashCtxt lets one worker signal a simulated crash to the coordinator.
	crashCh := make(chan string, 1)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	type done struct {
		id    string
		state StepState
	}
	doneCh := make(chan done, len(plan.Steps))
	inflight := map[string]bool{}

	ready := func(id string) bool {
		st := x.Steps[id]
		if st.Status != StepPending {
			return false
		}
		if inflight[id] {
			return false
		}
		for _, d := range deps[id] {
			switch x.Steps[d].Status {
			case StepSucceeded:
				// ok
			case StepFailed, StepCancelled, StepSkipped:
				return false
			default:
				return false
			}
		}
		return true
	}

	// skipUnreachable marks pending steps whose dependency failed/cancelled.
	propagateSkip := func() bool {
		changed := false
		for id := range stepByID {
			st := x.Steps[id]
			if st.Status != StepPending {
				continue
			}
			for _, d := range deps[id] {
				ds := x.Steps[d]
				if ds.Status == StepFailed || ds.Status == StepCancelled || ds.Status == StepSkipped {
					st.Status = StepSkipped
					st.Error = "skipped: dependency " + d + " did not succeed"
					st.EndedAt = e.clk.Now()
					x.Steps[id] = st
					e.stepAudit(runCtx, x, id, "step_skipped", st.Error)
					changed = true
					break
				}
			}
		}
		return changed
	}

	launch := func(id string) {
		inflight[id] = true
		sem <- struct{}{}
		go func() {
			defer func() { <-sem }()
			state := e.executeStep(runCtx, x.IncidentID, x.EventID, x.ID, stepByID[id])
			doneCh <- done{id: id, state: state}
		}()
	}

	for {
		// Persist a heartbeat/status snapshot each scheduling pass.
		var err error
		*version, err = e.saveExec(ctx, x, *version)
		if err != nil {
			// CAS failure means another writer moved first; reload and continue.
			reloaded, v, lerr := e.loadExec(ctx, x.ID)
			if lerr != nil {
				e.markFailed(ctx, x, version, "persistence error: "+lerr.Error())
				return runFinished
			}
			*x = *reloaded
			*version = v
		}

		// Handle external cancellation (Cancel flips status to cancelled).
		if cur, v, err := e.loadExec(ctx, x.ID); err == nil {
			if cur.Status == ExecCancelled && x.Status != ExecCancelled {
				cancel()
				*x = *cur
				*version = v
				e.finalizeInFlight(x, StepCancelled, "execution cancelled")
				x.FinishedAt = e.clk.Now()
				*version, _ = e.saveExec(ctx, x, *version)
				e.auditLog(ctx, x, "execution_cancelled", "")
				return runFinished
			}
		}

		// A simulated crash leaves status=running, lock held, steps durable.
		select {
		case stepID := <-crashCh:
			e.auditLog(ctx, x, "execution_interrupted", "simulated crash after "+stepID)
			return runCrashed
		default:
		}

		// Skip steps whose dependencies did not succeed; persist immediately.
		if propagateSkip() {
			continue
		}

		// Launch every ready step (bounded by sem).
		launched := false
		for id := range stepByID {
			if ready(id) {
				// mark running + persist BEFORE the executor runs so a crash
				// never loses an in-flight step.
				st := x.Steps[id]
				st.Status = StepRunning
				st.StartedAt = e.clk.Now()
				x.Steps[id] = st
				v, err := e.saveExec(ctx, x, *version)
				if err == nil {
					*version = v
					e.stepAudit(ctx, x, id, "step_started", "")
					launch(id)
					launched = true
				}
			}
		}
		if launched {
			continue
		}

		// Terminal when nothing is in flight and nothing can launch.
		if len(inflight) == 0 {
			e.finalizeStatus(x)
			x.FinishedAt = e.clk.Now()
			*version, _ = e.saveExec(ctx, x, *version)
			e.auditLog(ctx, x, "execution_"+string(x.Status), x.FailureSummary)
			return runFinished
		}

		// Wait for a step to finish (or context cancellation).
		select {
		case d := <-doneCh:
			delete(inflight, d.id)
			x.Steps[d.id] = d.state
			*version, _ = e.saveExec(ctx, x, *version)
			// Crash injection fires only AFTER the completed step is durable,
			// mimicking a process dying between steps: succeeded work is on
			// disk and must never be re-executed on resume.
			if e.CrashHook != nil && e.CrashHook(x.ID, d.id) {
				e.auditLog(ctx, x, "execution_interrupted", "simulated crash after "+d.id)
				return runCrashed
			}
		case <-runCtx.Done():
			e.finalizeInFlight(x, StepCancelled, "context cancelled")
			x.Status = ExecCancelled
			x.FinishedAt = e.clk.Now()
			*version, _ = e.saveExec(ctx, x, *version)
			e.auditLog(ctx, x, "execution_cancelled", runCtx.Err().Error())
			return runFinished
		}
	}
}

// executeStep runs one step with timeout and bounded retries. It never
// persists; the coordinator owns persistence. On simulated crash it reports
// the durable terminal step state via crashCh and returns it as well.
func (e *Engine) executeStep(ctx context.Context, incidentID, eventID, execID string, step Step) StepState {
	st := StepState{ID: step.ID, Status: StepRunning, StartedAt: e.clk.Now()}
	maxAttempts := step.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 1
	}
	action := executor.Action{Name: step.Name, Payload: step.Payload}

	for st.Attempts < maxAttempts {
		st.Attempts++
		runCtx := ctx
		var cancel context.CancelFunc
		if step.Timeout > 0 {
			runCtx, cancel = context.WithTimeout(ctx, step.Timeout)
		}
		e.stepAuditRaw(ctx, eventID, incidentID, execID, step.ID, "step_attempt",
			fmt.Sprintf("attempt=%d/%d", st.Attempts, maxAttempts))

		res := e.exec.Run(runCtx, execID, step.ID, action)
		if cancel != nil {
			cancel()
		}
		if res.Err == nil {
			st.Status = StepSucceeded
			st.Output = res.Output
			st.Error = ""
			st.EndedAt = e.clk.Now()
			e.stepAuditRaw(ctx, eventID, incidentID, execID, step.ID, "step_succeeded", res.Output)
			return st
		}
		st.Error = res.Err.Error()
		e.stepAuditRaw(ctx, eventID, incidentID, execID, step.ID, "step_attempt_failed", st.Error)
		if errors.Is(res.Err, context.Canceled) {
			st.Status = StepCancelled
			st.EndedAt = e.clk.Now()
			return st
		}
	}
	st.Status = StepFailed
	if st.Error == "" {
		st.Error = "max attempts exhausted"
	}
	st.EndedAt = e.clk.Now()
	e.stepAuditRaw(ctx, eventID, incidentID, execID, step.ID, "step_failed", st.Error)
	return st
}

func (e *Engine) stepAudit(ctx context.Context, x *Execution, stepID, action, reason string) {
	if e.audit == nil {
		return
	}
	_ = e.audit.Log(ctx, audit.Entry{
		EventID:    x.EventID,
		IncidentID: x.IncidentID,
		ExecID:     x.ID,
		Entity:     "step",
		Action:     action,
		To:         stepID,
		Reason:     reason,
	})
}

func (e *Engine) stepAuditRaw(ctx context.Context, eventID, incidentID, execID, stepID, action, reason string) {
	if e.audit == nil {
		return
	}
	_ = e.audit.Log(ctx, audit.Entry{
		EventID:    eventID,
		IncidentID: incidentID,
		ExecID:     execID,
		Entity:     "step",
		Action:     action,
		To:         stepID,
		Reason:     reason,
	})
}

// finalizeInFlight forces running steps into a terminal state (crash/cancel).
func (e *Engine) finalizeInFlight(x *Execution, status StepStatus, msg string) {
	for id, st := range x.Steps {
		if st.Status == StepPending || st.Status == StepRunning {
			st.Status = status
			if msg != "" && st.Error == "" {
				st.Error = msg
			}
			st.EndedAt = e.clk.Now()
			x.Steps[id] = st
		}
	}
}

// finalizeStatus derives the overall exec status from step states.
func (e *Engine) finalizeStatus(x *Execution) {
	failed, skipped := 0, 0
	for _, st := range x.Steps {
		switch st.Status {
		case StepFailed:
			failed++
		case StepCancelled:
			x.Status = ExecCancelled
			return
		case StepSkipped:
			skipped++
		}
	}
	if failed > 0 {
		x.Status = ExecFailed
		x.FailureSummary = summarizeFailures(x)
		return
	}
	x.Status = ExecSucceeded
}

func summarizeFailures(x *Execution) string {
	msg := ""
	for id, st := range x.Steps {
		if st.Status == StepFailed {
			if msg != "" {
				msg += "; "
			}
			msg += id + ": " + st.Error
		}
	}
	return msg
}

func (e *Engine) markFailed(ctx context.Context, x *Execution, version *int64, msg string) {
	x.Status = ExecFailed
	x.FailureSummary = msg
	x.FinishedAt = e.clk.Now()
	*version, _ = e.saveExec(ctx, x, *version)
}

var _ = store.ErrNotFound
var _ = time.Second
