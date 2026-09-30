package workflow

import (
	"context"
	"time"

	"github.com/h3yow/self-healing-ops-orchestrator-go/internal/audit"
	"github.com/h3yow/self-healing-ops-orchestrator-go/internal/clock"
	"github.com/h3yow/self-healing-ops-orchestrator-go/internal/executor"
	"github.com/h3yow/self-healing-ops-orchestrator-go/internal/store"
)

// Step is one node of the remediation DAG. Name maps to the executor action.
type Step struct {
	ID          string
	Name        string
	DependsOn   []string
	Timeout     time.Duration
	MaxAttempts int
	Payload     map[string]string
}

// StepStatus is the state of an individual step.
type StepStatus string

const (
	StepPending   StepStatus = "pending"
	StepRunning   StepStatus = "running"
	StepSucceeded StepStatus = "succeeded"
	StepFailed    StepStatus = "failed"
	StepSkipped   StepStatus = "skipped"
	StepCancelled StepStatus = "cancelled"
)

// StepState is the persisted state of one step.
type StepState struct {
	ID        string     `json:"id"`
	Status    StepStatus `json:"status"`
	Attempts  int        `json:"attempts"`
	Output    string     `json:"output,omitempty"`
	Error     string     `json:"error,omitempty"`
	StartedAt time.Time  `json:"started_at,omitempty"`
	EndedAt   time.Time  `json:"ended_at,omitempty"`
}

// Plan is a static DAG of remediation steps.
type Plan struct {
	FaultType string
	Steps     []Step
}

// ExecStatus is the overall execution status.
type ExecStatus string

const (
	ExecRunning     ExecStatus = "running"
	ExecSucceeded   ExecStatus = "succeeded"
	ExecFailed      ExecStatus = "failed"
	ExecCancelled   ExecStatus = "cancelled"
	ExecInterrupted ExecStatus = "interrupted"
)

// Execution is the persisted state of one remediation run.
type Execution struct {
	ID             string               `json:"id"`
	IncidentID     string               `json:"incident_id"`
	EventID        string               `json:"event_id"`
	FaultType      string               `json:"fault_type"`
	Target         string               `json:"target"`
	Status         ExecStatus           `json:"status"`
	Steps          map[string]StepState `json:"steps"`
	StartedAt      time.Time            `json:"started_at"`
	UpdatedAt      time.Time            `json:"updated_at"`
	FinishedAt     time.Time            `json:"finished_at,omitempty"`
	FailureSummary string               `json:"failure_summary,omitempty"`
	LockToken      string               `json:"lock_token,omitempty"`
	LockAcquiredAt time.Time            `json:"lock_acquired_at,omitempty"`
	AttemptedRun   int                  `json:"attempted_run,omitempty"`
	Version        int64                `json:"-"`
}

// ExecutorPort is what the engine drives (satisfied by executor.Simulator).
type ExecutorPort interface {
	Run(ctx context.Context, execID, stepID string, a executor.Action) executor.Result
}

// Engine runs DAG plans with retries, timeouts, cancellation and lease locks.
type Engine struct {
	st          store.Store
	exec        ExecutorPort
	clk         clock.Clock
	audit       *audit.Logger
	lease       time.Duration
	maxParallel int

	// PlanResolver reconstructs a Plan by fault type during Resume.
	PlanResolver func(faultType string) Plan

	// CrashHook is test-only infrastructure for interrupt simulation.
	CrashHook CrashHook
}

// CrashHook (test only) simulates a process crash mid-run.
type CrashHook func(execID, stepID string) bool

func NewEngine(st store.Store, exec ExecutorPort, clk clock.Clock,
	al *audit.Logger, lease time.Duration, maxParallel int) *Engine {
	if maxParallel <= 0 {
		maxParallel = 4
	}
	return &Engine{st: st, exec: exec, clk: clk, audit: al, lease: lease, maxParallel: maxParallel}
}
