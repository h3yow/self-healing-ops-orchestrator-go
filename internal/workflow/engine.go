package workflow

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/h3yow/self-healing-ops-orchestrator-go/internal/audit"
	"github.com/h3yow/self-healing-ops-orchestrator-go/internal/store"
)

const execPrefix = "executions/"

var (
	// ErrLeaseHeld means an execution is locked by a live runner.
	ErrLeaseHeld = errors.New("workflow: execution lease held by another runner")
	// ErrNotFound is re-exported for caller convenience.
	ErrNotFound = store.ErrNotFound
)

func (e *Engine) loadExec(ctx context.Context, execID string) (*Execution, int64, error) {
	x, ver, err := store.LoadAs[Execution](ctx, e.st, execPrefix+execID)
	if err != nil {
		return nil, 0, err
	}
	return &x, ver, nil
}

func (e *Engine) saveExec(ctx context.Context, x *Execution, version int64) (int64, error) {
	x.UpdatedAt = e.clk.Now()
	v, err := store.SaveAs(ctx, e.st, execPrefix+x.ID, version, *x)
	if err != nil {
		return 0, err
	}
	x.Version = v
	return v, nil
}

// acquireLock claims the lease for this runner.
func (e *Engine) acquireLock(ctx context.Context, x *Execution, version int64) (string, int64, error) {
	token := newToken()
	x.LockToken = token
	x.LockAcquiredAt = e.clk.Now()
	v, err := e.saveExec(ctx, x, version)
	if err != nil {
		return "", 0, err
	}
	e.auditLog(ctx, x, "lease_acquired", "")
	return token, v, nil
}

// releaseLock clears the lease when a run finalizes.
func (e *Engine) releaseLock(ctx context.Context, x *Execution, version int64, token string) (int64, error) {
	if x.LockToken != token {
		return version, nil
	}
	x.LockToken = ""
	var zero time.Time
	x.LockAcquiredAt = zero
	return e.saveExec(ctx, x, version)
}

func (e *Engine) leaseValid(x *Execution) bool {
	if x.LockToken == "" {
		return false
	}
	if e.lease <= 0 {
		return true // leases disabled: lock is authoritative
	}
	return e.clk.Now().Before(x.LockAcquiredAt.Add(e.lease))
}

func (e *Engine) auditLog(ctx context.Context, x *Execution, action, reason string) {
	if e.audit == nil {
		return
	}
	_ = e.audit.Log(ctx, audit.Entry{
		EventID:    x.EventID,
		IncidentID: x.IncidentID,
		ExecID:     x.ID,
		Entity:     "execution",
		Action:     action,
		Reason:     reason,
		Detail:     map[string]string{"status": string(x.Status)},
	})
}

func newToken() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// List returns all stored executions (used by recovery).
func (e *Engine) List(ctx context.Context) ([]*Execution, error) {
	keys, err := e.st.List(ctx, execPrefix)
	if err != nil {
		return nil, err
	}
	out := make([]*Execution, 0, len(keys))
	for _, k := range keys {
		x, _, err := store.LoadAs[Execution](ctx, e.st, k)
		if err != nil {
			return nil, err
		}
		out = append(out, &x)
	}
	return out, nil
}

// ValidatePlan fails fast on missing deps, unknown refs and cycles.
func ValidatePlan(plan Plan) error {
	index := map[string]bool{}
	for _, s := range plan.Steps {
		if s.ID == "" {
			return errors.New("workflow: step with empty id")
		}
		if index[s.ID] {
			return fmt.Errorf("workflow: duplicate step id %q", s.ID)
		}
		index[s.ID] = true
	}
	for _, s := range plan.Steps {
		for _, d := range s.DependsOn {
			if !index[d] {
				return fmt.Errorf("workflow: step %q depends on unknown step %q", s.ID, d)
			}
		}
	}
	// DFS cycle detection.
	state := map[string]int{} // 0 unseen, 1 on-stack, 2 done
	var visit func(id string) error
	visit = func(id string) error {
		switch state[id] {
		case 1:
			return fmt.Errorf("workflow: dependency cycle at %q", id)
		case 2:
			return nil
		}
		state[id] = 1
		for _, s := range plan.Steps {
			if s.ID == id {
				for _, d := range s.DependsOn {
					if err := visit(d); err != nil {
						return err
					}
				}
			}
		}
		state[id] = 2
		return nil
	}
	for _, s := range plan.Steps {
		if err := visit(s.ID); err != nil {
			return err
		}
	}
	return nil
}
