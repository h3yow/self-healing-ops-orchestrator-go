package executor

import (
	"context"
	"sync"
	"time"
)

// Action is a single simulated remediation command.
type Action struct {
	Name    string
	Payload map[string]string
}

// Result is what the simulator reports.
type Result struct {
	ActionName string
	Output     string
	Err        error
	Duration   time.Duration
	Attempt    int
}

// Port is what the workflow engine drives.
type Port interface {
	Run(ctx context.Context, execID, stepID string, a Action) Result
}

// ScriptedResult scripts the simulator's behavior per step.
//
// Failures: if Failures is set, the first Failures[min(...)] invocations fail;
// use Failure for a constant failure. Delay is honored and ctx cancellation /
// deadline interrupts it, yielding context.DeadlineExceeded/Canceled.
type ScriptedResult struct {
	ActionName string
	Output     string
	Failure    error
	Failures   []error // per-attempt failures (index 0 = first invocation)
	Delay      time.Duration
}

// invocation is recorded for every real (non-deduped) execution of a step.
type invocation struct {
	ExecID string
	At     time.Time
}

// Simulator is a controllable, invocation-recording local executor.
type Simulator struct {
	mu      sync.Mutex
	scripts map[string]ScriptedResult
	calls   map[string][]invocation
}

func NewSimulator() *Simulator {
	return &Simulator{
		scripts: map[string]ScriptedResult{},
		calls:   map[string][]invocation{},
	}
}

// Script registers a step's behavior and returns the simulator for chaining.
func (s *Simulator) Script(stepID string, r ScriptedResult) *Simulator {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.ActionName == "" {
		r.ActionName = stepID
	}
	s.scripts[stepID] = r
	return s
}

// Run executes the scripted behavior for stepID.
func (s *Simulator) Run(ctx context.Context, execID, stepID string, a Action) Result {
	s.mu.Lock()
	script := s.scripts[stepID]
	attempt := len(s.calls[stepID])
	s.calls[stepID] = append(s.calls[stepID], invocation{ExecID: execID, At: time.Now()})
	s.mu.Unlock()

	start := time.Now()
	res := Result{ActionName: a.Name, Attempt: attempt}

	if script.Delay > 0 {
		select {
		case <-time.After(script.Delay):
		case <-ctx.Done():
			res.Err = ctx.Err()
			res.Duration = time.Since(start)
			return res
		}
	}

	// Determine failure for this attempt.
	var fail error
	if len(script.Failures) > 0 {
		if attempt < len(script.Failures) {
			fail = script.Failures[attempt]
		}
	} else {
		fail = script.Failure
	}
	if fail != nil {
		res.Err = fail
	} else {
		res.Output = script.Output
	}
	res.Duration = time.Since(start)
	return res
}

// Invocations returns how many times a step actually ran (resume dedupe check).
func (s *Simulator) Invocations(stepID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.calls[stepID])
}

// InvocationExecIDs returns the exec IDs that ran a step.
func (s *Simulator) InvocationExecIDs(stepID string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.calls[stepID]))
	for i, c := range s.calls[stepID] {
		out[i] = c.ExecID
	}
	return out
}

// Reset clears invocation counters (scripts are retained).
func (s *Simulator) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = map[string][]invocation{}
}
